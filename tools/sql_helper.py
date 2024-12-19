import re
import sqlalchemy as sa
from langchain_openai import OpenAI
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv
import os
from pathlib import Path
from langchain.schema import Document

current_dir = Path(__file__).resolve().parent
dotenv_path = current_dir.parent / '.env'
load_dotenv(dotenv_path)
openai_api_key = os.getenv('OPENAI_API_KEY')

redshift_dsn = os.getenv("REDSHIFT_DSN")
engine = sa.create_engine(redshift_dsn)

def parse_foreign_key_constraint(sql_statement):
    # Regular expression pattern to match the components
    pattern = r'ALTER\s+TABLE\s+(\w+)\.(\w+)\s+ADD\s+FOREIGN\s+KEY\s+\((\w+)\)\s+REFERENCES\s+(\w+)\.(\w+)\((\w+)\)'
    
    # Match the pattern
    match = re.match(pattern, sql_statement, re.IGNORECASE)
    
    if match:
        # Extract matched groups
        result = {
            'foreign_key': match.group(3),
            'foreign_name': "",
            'reference_schema_name': match.group(4),
            'reference_table_name': match.group(5),
            'reference_key': match.group(6)
        }
        return result
    else:
        raise ValueError("Invalid SQL statement: Could not parse foreign key constraint")
    
def get_table_metadata(schema_name: str, table_name: str) -> dict:
    with engine.connect() as connection:
        columns = []
        foreigns = []
        primary_key = {}
        columns_query = f"""
        SELECT att.attname, des.description, ty.typname
        FROM pg_catalog.pg_attribute att
        LEFT JOIN pg_catalog.pg_description des ON att.attrelid = des.objoid AND att.attnum = des.objsubid
        LEFT JOIN pg_type ty ON ty.oid = att.atttypid
        LEFT JOIN pg_class cl ON cl.oid = att.attrelid
        LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = cl.relnamespace
        WHERE att.attnum > 0
        AND cl.relname = '{table_name}'
        AND ns.nspname = '{schema_name}'
        """
        columns_result = connection.execute(sa.text(columns_query))
        rows = columns_result.fetchall()
        
        for row in rows:
            columns.append({
                "column_name": row[0],
                "column_description": row[1] if row[1] else "",
                "column_type": row[2]
            })
        table_desc_query = f"select obj_description('{schema_name}.{table_name}'::regclass)"
        table_desc_result = connection.execute(sa.text(table_desc_query)).fetchone()[0]

        foreign_key_query = f"""
        select ddl from admin.v_generate_tbl_ddl 
        where schemaname = '{schema_name}' and tablename = '{table_name}'
        and ddl like '%FOREIGN KEY%';
        """
        foreign_key_result = connection.execute(sa.text(foreign_key_query))
        foreign_rows = foreign_key_result.fetchall()
        for foreign_row in foreign_rows:
            foreign_key_constraint = parse_foreign_key_constraint(foreign_row[0])
            foreigns.append(foreign_key_constraint)
        
        constraint_query = f"""
        select a.table_schema, a.table_name, a.constraint_name, b.column_name, a.constraint_type from information_schema.table_constraints a
        left join information_schema.key_column_usage b 
        on b.table_schema = a.table_schema and b.table_name = a.table_name and b.constraint_name = a.constraint_name
        where a.table_schema = '{schema_name}' and a.table_name = '{table_name}'
        """
        constraint_result = connection.execute(sa.text(constraint_query))
        constraint_rows = constraint_result.fetchall()
        for constraint_row in constraint_rows:
            if constraint_row[4] == "PRIMARY KEY":
                primary_key = {
                    "primary_key_column": constraint_row[3],
                    "primary_key_name": constraint_row[2]
                }
            if constraint_row[4] == "FOREIGN KEY":
                index = next((i for i, item in enumerate(foreigns) if item['foreign_key'] == constraint_row[3]), -1)
                if index != -1:
                    foreigns[index]['foreign_name'] = constraint_row[2]
        
        output_result = {
            "schema_name": schema_name,
            "table_name": table_name,
            "table_description": table_desc_result.strip() if table_desc_result else "",
            "primary_key": primary_key,
            "foreign_keys": foreigns,
            "columns": columns 
        }
        return output_result
    
def meaningful_text_from_metadata(metadata: dict) -> Document:
    meaningful_text = f"""
Table `{metadata["table_name"]}` in schema `{metadata["schema_name"]}` is a table that {metadata["table_description"]}.

The table has a primary key with column name `{metadata["primary_key"]["primary_key_column"]}` and the name of the constraint is `{metadata["primary_key"]["primary_key_name"]}` which uniquely identifies each row.
    """
    first = True
    for foreign_key in metadata["foreign_keys"]:
        if first:
            _temp = "\nIt "
            first = False
        else:
            _temp = "It also "
        _temp += f"maintains a foreign key relationship through the `{foreign_key["foreign_key"]}` column, which references the `{foreign_key["reference_key"]}` column in the table `{foreign_key["reference_table_name"]}` table from schema `{foreign_key["reference_schema_name"]}`, the constraint name is `{foreign_key["foreign_name"]}`.\n"
        meaningful_text += _temp
        _temp = ""
    
    meaningful_text += "\nThe columns in the table are:\n"
    for column in metadata["columns"]:
        _temp = f"- Column name: {column['column_name']}, Description: {column['column_description']}, Type: {column['column_type']}\n"
        meaningful_text += _temp
        _temp = ""
    meaningful_text += "\n"
    assumption_summary = get_assumption_summary_and_relationship(meaningful_text)
    meaningful_text += "\n" + assumption_summary + "\n"
    doc = Document(
        page_content=meaningful_text,
        metadata = {
            "table_name": metadata["table_name"], 
            "schema_name": metadata["schema_name"],
            "primary_key": metadata["primary_key"]["primary_key_column"],
        }
    )
    return doc

def get_assumption_summary_and_relationship(query: str) -> str:
    llm = OpenAI(api_key=openai_api_key)
    template = """
Given the information about a table in redshift, write an assumption about the table purpose. Also add assumption about the relationship. 
Example output: 
This table seems or assumed to be ... 
For the relationship, it seems or assumed to have the following relationships: 
- Each transaction (salesid) is associated with a specific list (listid) 
- Transactions involve a seller (sellerid) and ... 
- Each sale is tied to an event (eventid) and ... 

Input: 
```
{query}
```
    """
    prompt = PromptTemplate.from_template(template)
    chain = prompt | llm
    result = chain.invoke({"query": query})
    return str(result)
