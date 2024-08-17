import os
import dotenv
from composio import action
from composio_openai import ComposioToolSet
from custom_tools import run_postgresql_query
from crewai import Agent, Crew, Process, Task
from langchain_openai import ChatOpenAI

# Load environment variables from .env file
dotenv.load_dotenv()

composio_toolset = ComposioToolSet()

# Get required tools
tools = [
    *composio_toolset.get_actions(
        actions=[
            run_postgresql_query,
        ]
    ),
]

llm = ChatOpenAI(model="gpt-3.5")

# Define the Query Executor Agent
query_executor_agent = Agent(
    role="Query Executor Agent",
    goal="""Execute the SQL query and return the results.""",
    backstory=(
        "You are an expert in SQL and database management, "
        "skilled at executing SQL queries and providing results efficiently."
    ),
    verbose=True,
    tools=tools,
    llm=llm,
)

user_description = "The database name is composio"
user_input = "fetch the rows in the users table"
connection_string = os.getenv("PG_CONN_STRING") or ""

# Define the task for executing the SQL query
execute_query_task = Task(
    description=(
        "This is the database description="
        + user_description
        + "form a sql query based on this input="
        + user_input
        + "Execute the SQL query formed by the Query Writer Agent, "
        "and return the results. Pass the query and connection string parameter."
        "The connection string parameter=" + connection_string
    ),
    expected_output="Results of the SQL query were returned. Stop once the goal is achieved",
    tools=tools,
    agent=query_executor_agent,
)


# Define the crew with the agents and tasks
crew = Crew(
    agents=[query_executor_agent],
    tasks=[execute_query_task],
    process=Process.sequential,
)

# Kickoff the process and print the result
result = crew.kickoff()
print(result)
