from agents.redshift_agent import redshift_agent
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_pinecone import PineconeVectorStore
from dotenv import load_dotenv
from pathlib import Path
from langchain.prompts import PromptTemplate
import os
from langchain_core.exceptions import OutputParserException
from langchain.schema.output_parser import StrOutputParser

current_dir = Path(__file__).resolve().parent
dotenv_path = current_dir.parent / '.env'
load_dotenv(dotenv_path)

openai_api_key = os.getenv('OPENAI_API_KEY')
pinecone_api_key = os.getenv('PINECONE_API_KEY')
pinecone_index_name = os.getenv('PINECONE_INDEX_NAME')

llm = ChatOpenAI(api_key=openai_api_key, model="gpt-4o-mini", temperature=0)

def ask_redshift(query: str):
    embeddings = OpenAIEmbeddings(api_key=openai_api_key, model="text-embedding-ada-002")
    docsearch = PineconeVectorStore(index_name=pinecone_index_name, embedding=embeddings, pinecone_api_key=pinecone_api_key)
    try:
        result = redshift_agent(query, docsearch)
    except OutputParserException as e:
        _temp = str(e)
        lines = _temp.split("\n")  # Split into lines
        lines = lines[:-2]  # Remove the last two rows
        result = "\n".join(lines)
    
    template = """
    Given the following output:
    {output}

    refine the output (change like Action: Action:Input to a sentence). Refine also the Final Answer so its not using number but still keep the context
    """
    prompt = PromptTemplate.from_template(template)
    chain = prompt | llm | StrOutputParser()

    final_result = chain.invoke({"output": result})
    try:
        fi_result = result.return_values["output"]
    except Exception as e:
        fi_result = result.replace("Parsing LLM output produced both a final answer and a parse-able action:","")
    return fi_result


if __name__ == "__main__":
    question = "Who are the top 3 sellers based on total sales value?"
    result = ask_redshift(question)
    print(f"{result}")