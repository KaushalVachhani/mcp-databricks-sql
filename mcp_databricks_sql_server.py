"""
Databricks SQL Server implementation for MCP.

This module provides functionality to execute SQL statements against a Databricks SQL warehouse
using the Databricks SQL API.
"""

# Standard library imports
import json
import os
from typing import Any, Dict, Optional

# Third-party imports
import requests
from dotenv import load_dotenv

# Local imports
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()

# Configuration
DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN: str = os.getenv("DATABRICKS_TOKEN")
WAREHOUSE_ID: str = "<YOUR_WAREHOUSE_ID>" #todo: add your warehouse id here

# Initialize MCP
mcp = FastMCP("docs")

@mcp.tool()
async def execute_statement(
    statement: str,
    warehouse_id: str = WAREHOUSE_ID,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    row_limit: int = 10,
) -> Dict[str, Any]:
    """
    Execute a SQL statement against a Databricks SQL warehouse.
    
    Args:
        statement: SQL query to execute on the warehouse
        warehouse_id: Unique identifier of the Databricks SQL warehouse
        catalog: Optional Unity Catalog name to query against
        schema: Optional database schema name within the catalog
        parameters: Optional dictionary of query parameters for parameterized SQL
        row_limit: Maximum number of rows to return (default: 10)
        
    Returns:
        Dict containing query results, including:
        - manifest: Query metadata and schema information
        - result_set: Array of result rows
        - status: Query execution status
    """    
    request_data = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "10s",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "row_limit": row_limit,
    }
    
    # Add optional parameters
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

    # Execute request
    response = requests.post(url, headers=headers, data=json.dumps(request_data))
    
    return response.json()

if __name__ == "__main__":
    mcp.run(transport="stdio")