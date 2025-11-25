# Databricks MCP Server

## Motivation

Databricks Unity Catalog (UC) provides a single place for catalog, schema, table, and column metadata. Well-populated UC metadata can be directly consumed by LLMs and agents to improve automated data discovery, SQL generation, and pipeline analysis. This project demonstrates practical patterns for exposing that metadata to agents in a deterministic, LLM-friendly format.

## Overview

The MCP server implemented here focuses on:

- Converting complex UC and lineage metadata into compact Markdown summaries optimized for LLMs.
- Providing environment-driven authentication (PAT or OAuth client-credentials) and a small REST adapter for cases where the Databricks SDK is not used.
- Exposing a concise set of async tools (via FastMCP) that let agents discover catalogs/schemas, inspect table metadata and lineage, and execute read-only SQL queries.

The codebase favors small, composable tools so an agent can orchestrate discovery, explanation, and data retrieval in a safe, auditable manner.

## Practical Benefits of UC Metadata for AI Agents

* **Clearer Data Context** — Schema and column descriptions reduce ambiguity and make generated SQL more accurate.
* **Improved Query Precision** — Knowledge of data types, constraints, and comments helps agents form correct WHERE clauses and joins.
* **Faster Exploration** — Agents can programmatically find candidate tables and columns instead of relying on human guidance.
* **Lineage-aware Reasoning** — When lineage includes notebooks and jobs, agents can inspect transformation logic to validate assumptions.

These capabilities reduce the time-to-insight and increase trust in agent-suggested queries.

## Available Tools and Features

This prototype exposes a small number of tools designed for agent composition. Tools return either Markdown (for human/LLM readability) or structured JSON for programmatic workflows.

### Implemented Tools (summary)

- `fetch_schemas_in_catalog(catalog: str) -> str`
   - Retrieves all schemas within a given Unity Catalog catalog and returns a Markdown-formatted summary (name, description, count). Use this when an agent needs to discover which schemas exist in a catalog.

- `fetch_tables_in_schema(catalog: str, schema: str) -> str`
   - Lists tables inside a specific `catalog.schema` and returns the result as Markdown (table count and names). Use this to discover data assets within a schema before inspecting them in detail.

- `fetch_table_info(table_names: List[str]) -> str`
   - Fetches detailed table metadata for one or more fully-qualified tables and returns a compact, LLM-friendly Markdown description for each table. The output includes table identifiers, column definitions (name, type, nullable, comments), table constraints (if present), and lineage information when available.

- `execute_spark_sql_query(query: str) -> Dict[str, Any]`
   - Executes a read-only Spark SQL query against the configured Databricks SQL Warehouse and returns a structured JSON containing the original query, execution `state` (e.g., `SUCCEEDED` / `FAILED`), `data` (rows as JSON objects when available), and an `error` message if the query failed. This tool is intentionally read-only in the prototype (SELECT / DQL style queries).

Each implemented tool is intentionally small and deterministic so an agent can compose discovery → describe → query workflows safely and reliably.

## Setup

### System Requirements

- Python 3.10+
- Recommended: virtual environment for development

### Installation

Create a virtual environment and install dependencies from `requirements.txt`:

```bash
# using uv
uv sync

# using pip
pip install -r requirements.txt
```

Configure authentication (choose one flow):

**PAT flow (easy dev):**

```bash
export DATABRICKS_HOST="https://<your-databricks-host>"
export DATABRICKS_TOKEN="<your-pat>"
export DATABRICKS_SQL_WAREHOUSE_ID="<warehouse-id>"  # used for query execution
```

**OAuth client-credentials (server-to-server):**

```bash
export DATABRICKS_HOST="https://<your-databricks-host>"
export DATABRICKS_CLIENT_ID="<client-id>"
export DATABRICKS_CLIENT_SECRET="<client-secret>"
export DATABRICKS_OAUTH_TOKEN_URL="https://<your-token-endpoint>"
export DATABRICKS_OAUTH_SCOPE="<optional-scope>"
```

The system will prefer a PAT if present, otherwise it will attempt OAuth client-credentials and cache tokens until expiration.

## Permissions Requirements

Ensure the token or service principal used by the server has the following minimum permissions:

1. Unity Catalog
   - `USE CATALOG` on each catalog to inspect
   - `USE SCHEMA` on each schema to inspect
   - `SELECT` on tables to read schema and query data

2. SQL Warehouse
   - `CAN_USE` on the SQL Warehouse used for query execution

3. Principle of least privilege
   - For production, prefer a service principal with the minimum required scopes.

Audit and rotate credentials regularly.

## Running the Server

### Standalone Mode

Run the server locally for demos:

```bash
uv run server.py
# OR
python server.py
```

The server registers the async tools via `FastMCP` (see `server.py`) and exposes them to MCP-aware runners or LLM agents.

### Using with Cursor

This MCP server is designed to be integrated with an agent framework or an MCP client. Typical usage when driving with an agent (using the implemented functions):

1. Call discovery tools (`fetch_schemas_in_catalog`, `fetch_tables_in_schema`) to gather context about available schemas and tables within a catalog.
2. Call `fetch_table_info([full_table_name])` to fetch detailed table metadata (columns, constraints) and lineage information for one or more tables.
3. Construct a read-only SQL query and call `execute_spark_sql_query` to retrieve rows and validate results.

Keep interactions read-only unless you intentionally modify the code to support DDL/DML.

## Example Usage Workflow (for an LLM Agent)

1. Agent calls `fetch_schemas_in_catalog(catalog)` to list schemas in a given catalog.
2. Agent calls `fetch_tables_in_schema(catalog, schema)` to list tables in the selected schema.
3. Agent calls `fetch_table_info([full_table_name])` to collect detailed column metadata, constraints, and lineage for the target table(s).
4. Agent composes a `SELECT` query and calls `execute_spark_sql_query(query)` to validate results.

This staged approach reduces guesswork and leads to more accurate queries.

## Handling Long-Running Queries

Best practices:

- Use `LIMIT` or sampling for exploration.
- Use the statement execution API's `wait_timeout` and pollable statement ids for long-running jobs.
- Return statement ids for long-running queries so agents can poll for completion.
- Be mindful of warehouse cost and concurrency limits.
