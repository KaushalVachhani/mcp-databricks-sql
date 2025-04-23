import asyncio
import os
from dotenv import load_dotenv
from databricks_langchain import ChatDatabricks
from mcp_use import MCPAgent, MCPClient

async def main():
    # Load environment variables
    load_dotenv()

    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    # Create configuration dictionary
    config_file = "mcp_configs.json"

    # Create MCPClient from configuration dictionary
    client = MCPClient.from_config_file(config_file)

    # Create LLM
    llm = ChatDatabricks(model="databricks-claude-3-7-sonnet", temperature=0.0)

    # Create agent with the client
    agent = MCPAgent(llm=llm, client=client, max_steps=30)

    # Run the query
    result = await agent.run(
        """
        Give me last 2 transaction data for customer id C10001. 

        Use catalog "kaushal" and schema "b2b" to find relevent tables.
        """,
    )
    print(f"\nResult: {result}")

if __name__ == "__main__":
    asyncio.run(main())