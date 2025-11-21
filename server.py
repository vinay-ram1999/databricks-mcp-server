from mcp.server.fastmcp import FastMCP

from databricks_mcp_server.tools import (
    fetch_schemas_in_catalog,
    fetch_tables_in_schema,
    fetch_table_info,
    execute_spark_sql_query,
)

mcp = FastMCP(
    name="databricks-mcp-server",
    log_level="INFO",
    json_response=True,
)

mcp.add_tool(fetch_schemas_in_catalog)
mcp.add_tool(fetch_tables_in_schema)
mcp.add_tool(fetch_table_info)
mcp.add_tool(execute_spark_sql_query)

# To deploy using uvicorn `uvicorn server:app --host 0.0.0.0 --port 8000`
# app = mcp.streamable_http_app()

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
