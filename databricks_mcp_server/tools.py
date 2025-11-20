from typing import List
import logging

import asyncio

from .unitycatalog import (
    get_schemas_in_catalog,
    get_tables_in_schema,
    get_table_info,
)

logger = logging.getLogger(__name__)


async def fetch_schemas_in_catalog(catalog: str) -> str:
    """
    Retrieves all schemas within a given Unity Catalog catalog.

    Use this tool when you need to:
    - Discover what schemas exist in a particular catalog

    The output is formatted in Markdown and typically includes:
    - Number of schemas present in that catalog
    - A list of schemas with name and description (if available)

    Args:
        catalog: The Unity Catalog catalog name (e.g., "main").
    
    Returns:
        A Markdown-formatted string containing the list of schemas in the specified catalog.
        If an error occurs, a Markdown-formatted error message is returned instead
    """
    logger.info(f"fetching list of schemas in catalog: {catalog}")

    try:
        result = await asyncio.to_thread(
            get_schemas_in_catalog,
            catalog_name=catalog,
        )
        return result
    except Exception as e:
        error_details = str(e)
        logger.error(error_details)
        return f"""**Error**: Could not retrieve list of schemas
**Details:**
```
{error_details}
```
"""


async def fetch_tables_in_schema(catalog: str, schema: str) -> str:
    """
    Retrieves all tables within a given Unity Catalog catalog and schema.

    Use this tool when you need to:
    - Discover what tables exist in a particular catalog.schema
    - Explore available data assets before deciding which tables to inspect in detail
    - Validate that a schema has been correctly populated with tables

    The output is formatted in Markdown and typically includes:
    - Number of tables present in that catalog.schema
    - A list of comma seperated table names

    Args:
        catalog: The Unity Catalog catalog name (e.g., "main").
        schema: The schema name within the catalog (e.g., "analytics").
    
    Returns:
        A Markdown-formatted string containing the list of tables in the specified schema.
        If an error occurs, a Markdown-formatted error message is returned instead
    """
    logger.info(f"fetching list of tables in schema: {catalog}.{schema}")

    try:
        result = await asyncio.to_thread(
            get_tables_in_schema,
            catalog_name=catalog,
            schema_name=schema,
        )
        return result
    except Exception as e:
        error_details = str(e)
        logger.error(error_details)
        return f"""**Error**: Could not retrieve list of tables
**Details:**
```
{error_details}
```
"""

async def fetch_table_info(table_names: List) -> str:
    """
    Provides a detailed description of a Unity Catalog table along with lineage information.
    
    Use this tool to understand the structure (columns, data types) for a single table or multiple tables at once.
    This is essential before constructing SQL queries against the table. 
    It also includes comprehensive lineage information (if there is any) that goes beyond traditional table-to-table dependencies:

    **Table Lineage:**
    - Upstream tables (tables this table reads from)
    - Downstream tables (tables that read from this table)

    The output is formatted in Markdown.

    Args:
        table_names: A list of fully qualified three-level name of the table (e.g., ['catalog.schema.table1', ...]).
    
    Returns:
        A Markdown-formatted string describing the requested tables. This typically includes:
        - Table identifiers (table full name, type)
        - Column definitions (name, data type, nullability, comments)
        - Table constraints information (if any)
        - Upstream and downstream lineage information (if available)
    """
    logger.info(f"fetching metadata for tables: {table_names}")
    try:
        assert isinstance(table_names, list), ValueError("`table_names` argument should be a list of table names.")
        result = await asyncio.to_thread(
            get_table_info,
            table_names=table_names,
        )
        return result
    except Exception as e:
        error_details = str(e)
        logger.error(error_details)
        return f"""**Error**: Could not retrieve table information
**Details:**
```
{error_details}
```
"""
