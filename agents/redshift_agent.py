from dotenv import load_dotenv
from langchain.agents import tool
from langchain.agents.output_parsers import ReActSingleInputOutputParser
from langchain.prompts import PromptTemplate
from langchain.tools.render import render_text_description
from langchain_openai import ChatOpenAI
from langchain.tools import Tool
import os
from typing import Union, List
from langchain.schema import AgentAction, AgentFinish
from pathlib import Path
import sqlalchemy as sa
from langchain.agents.format_scratchpad import format_log_to_str
from langchain_pinecone import PineconeVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_core.exceptions import OutputParserException


current_dir = Path(__file__).resolve().parent
dotenv_path = current_dir.parent / '.env'
load_dotenv(dotenv_path)

redshift_dsn = os.getenv("REDSHIFT_DSN")

engine = sa.create_engine(redshift_dsn)

openai_api_key = os.getenv("OPENAI_API_KEY")
pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_index_name = os.getenv("PINECONE_INDEX_NAME")

@tool
def validate_redshift_query(query: str) -> str:
    """Validate the SQL query in Redshift to ensure it can be executed without errors."""
    with engine.connect() as connection:
        test_query = query
        try:
            columns_result = connection.execute(sa.text(test_query))
            row = columns_result.fetchone()
            return "Query is valid"
        except Exception as e:
            return f"Error occurred while validating query: {str(e)}"
        
@tool
def run_redshift_query(query: str) -> Union[str, List[str]]:
    """Run the SQL Query which already validated then return the result."""
    with engine.connect() as connection:
        try:
            result = connection.execute(sa.text(query))
            rows = result.fetchall()
            return f"{str(rows)}"
        except Exception as e:
            return "Redshift SQL query is not valid"
        
def find_tool_by_name(tools: List[Tool], tool_name:str)->Tool:
    for tool in tools:
        if tool.name == tool_name:
            return tool
    raise ValueError(f"Tool with '{tool_name}' not found.")

def redshift_agent(query: str, docsearch: PineconeVectorStore) -> str:
    docs = docsearch.similarity_search(query, k=5)
    context = "\n".join([doc.page_content for doc in docs])
    tools = [validate_redshift_query, run_redshift_query]

    template = """
    You are an agent designed to interact with a Redshift database accurately. You have access to the following tools:

    {tools}

    You also need to create the query and answer based on the given context:
    ```
    {context}
    ```

    Given an input question, create a syntactically correct Redshift SQL query to run. DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
    Use the following format:

    Question: the input question you must response
    Thought: You should start to plan your query based on the question from the given context. You should validate first if the query can be run without error.
    Action: the action you take, should be one of [{tool_names}]
    Action Input: the input to the chosen action. Make it as one line, dont use markdown.
    Observation: the result of the action.
    ... (this Though/Action/Action Input/Observation can repeat N times)
    Thought: I now know the final answer.
    Final Answer: Provide a concise and specific answer to the question based on the SQL query result. Include the following:
    1. The exact answer to the question (e.g., "The seller_id that has the highest total sales value is 22")
    2. The SQL query used to obtain this result
    3. A brief explanation of how you arrived at this answer

    Begin!

    Question: {input}
    Thought: {agent_scratchpad}
    """
    prompt = PromptTemplate.from_template(template=template).partial(
        tools=render_text_description(tools),
        context=context,
        tool_names=", ".join([t.name for t in tools])
    )

    llm = ChatOpenAI(
        api_key=openai_api_key,
        temperature=0,
        stop=["\n    Observation","\nObservation"]
    )

    intermediate_steps = []

    agent = {
        "input": lambda x:x["input"],
        "agent_scratchpad": lambda x: format_log_to_str(x["agent_scratchpad"]),
        } | prompt | llm | ReActSingleInputOutputParser()
    
    agent_step = ""
    last_step = None
    while not isinstance(agent_step, AgentFinish):
        agent_step: Union[AgentAction, AgentFinish] = agent.invoke({"input":query, "agent_scratchpad": intermediate_steps})
        print(agent_step)
        if isinstance(agent_step, AgentAction):
            tool_name = agent_step.tool
            tool_to_use = find_tool_by_name(tools, tool_name)
            tool_input = agent_step.tool_input

            observation = tool_to_use.func(str(tool_input))
            intermediate_steps.append((agent_step, str(observation)))
    
    return agent_step

if __name__ == "__main__":
    embeddings = OpenAIEmbeddings(api_key=openai_api_key, model="text-embedding-ada-002")
    docsearch = PineconeVectorStore(index_name=pinecone_index_name, embedding=embeddings, pinecone_api_key=pinecone_api_key)
    
    query = "Which seller_id has the highest total sales value?"
    result = redshift_agent(query, docsearch)
    print(result.return_values)