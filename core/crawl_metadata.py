"""Author: Rishad Harisdias Bustomi"""

from tools.sql_helper import meaningful_text_from_metadata, get_table_metadata
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from dotenv import load_dotenv
import os
from pathlib import Path


current_dir = Path(__file__).resolve().parent
dotenv_path = current_dir.parent / '.env'
load_dotenv(dotenv_path)

openai_api_key = os.getenv("OPENAI_API_KEY")
pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_index_name = os.getenv("PINECONE_INDEX_NAME")

if __name__ == "__main__":
    """
    This function crawl the metadata from databases, 
    implement the embedding model, and store to vector database
    """
    list_of_tables = [
        {
            "schema_name": "lrt_demo",
            "table_name": "dm_route"
        },
        {
            "schema_name": "lrt_demo",
            "table_name": "dm_sales"
        },
        {
            "schema_name": "lrt_demo",
            "table_name": "dm_incident_maintenance"
        },
        {
            "schema_name": "lrt_demo",
            "table_name": "dm_route_performance_metrics"
        },
        {
            "schema_name": "lrt_demo",
            "table_name": "dm_financial_performance_metrics"
        }
    ]

    docs = []
    print("Crawling metadata and generating docs...")
    for table in list_of_tables:
        metadata = get_table_metadata(table["schema_name"], table["table_name"])
        doc = meaningful_text_from_metadata(metadata)
        docs.append(doc)

    print(f"{len(docs)} documents generated.")
    print("Implementing embedding model and storing to PineVector...")
    embeddings = OpenAIEmbeddings(api_key=openai_api_key, model="text-embedding-ada-002")
    PineconeVectorStore.from_documents(
        documents=docs,
        embedding=embeddings,
        index_name=pinecone_index_name,
        pinecone_api_key=pinecone_api_key
    )
    print("Done!")
    

