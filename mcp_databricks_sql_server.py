from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from typing import Optional, Dict, Any
import requests
import json
import os

load_dotenv()

# Set your Databricks credentials and warehouse info
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

warehouse_id="ff5512f3badbbd78"

mcp = FastMCP("docs")

@mcp.tool()
async def execute_statement(
    statement: str,
    warehouse_id: str = warehouse_id,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    row_limit: int = 10,
) -> Dict[str, Any]:
    """
    Execute a SQL statement.
    
    Args:
        statement: The SQL statement to execute
        warehouse_id: ID of the SQL warehouse to use
        catalog: Optional catalog to use
        schema: Optional schema to use
        parameters: Optional statement parameters
        
    Returns:
        Response containing query results
        
    Raises:
        DatabricksAPIError: If the API request fails
    """    
    request_data = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "10s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "row_limit": row_limit,
    }
    
    if catalog:
        request_data["catalog"] = catalog
        
    if schema:
        request_data["schema"] = schema
        
    if parameters:
        request_data["parameters"] = parameters
        

    # Prepare the API endpoint and headers
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Send the POST request to execute the statement
    response = requests.post(url, headers=headers, data=json.dumps(request_data))
    
    return response.json()


if __name__ == "__main__":
    mcp.run(transport="stdio")