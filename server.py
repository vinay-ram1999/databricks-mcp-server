from mcp.server.fastmcp import FastMCP

from databricks_mcp_server.tools import (
    fetch_tables_in_schema,
    fetch_table_info,
)

mcp = FastMCP("databricks-mcp-server")

mcp.add_tool(fetch_tables_in_schema)
mcp.add_tool(fetch_table_info)

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
