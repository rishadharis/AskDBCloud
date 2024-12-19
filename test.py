import re

def parse_foreign_key_constraint(sql_statement):
    # Regular expression pattern to match the components
    pattern = r'ALTER\s+TABLE\s+(\w+)\.(\w+)\s+ADD\s+FOREIGN\s+KEY\s+\((\w+)\)\s+REFERENCES\s+(\w+)\.(\w+)\((\w+)\)'
    
    # Match the pattern
    match = re.match(pattern, sql_statement, re.IGNORECASE)
    
    if match:
        # Extract matched groups
        result = {
            'schema_name': match.group(1),
            'table_name': match.group(2),
            'foreign_key': match.group(3),
            'reference_schema_name': match.group(4),
            'reference_table_name': match.group(5),
            'reference_foreign_key': match.group(6)
        }
        return result
    else:
        return None

# Example usage
sql_statement = "ALTER TABLE lrt_demo.test_constraint_trxz ADD FOREIGN KEY (listid) REFERENCES lrt_demo.test_constraint_master(listid);"

result = parse_foreign_key_constraint(sql_statement)
if result:
    for key, value in result.items():
        print(f"{key}: {value}")