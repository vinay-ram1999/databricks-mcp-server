from typing import Optional

import asyncio
from mcp.server.fastmcp import FastMCP

from databricks_mcp_server.tools import fetch_table_info

mcp = FastMCP("databricks-mcp-server")

mcp.add_tool(fetch_table_info)

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
