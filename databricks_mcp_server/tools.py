from typing import Optional

import asyncio

from .unitycatalog import get_table_info


async def fetch_table_info(table_name: str) -> str:
    """
    Provides a detailed description of a specific Unity Catalog table.
    
    Use this tool to understand the structure (columns, data types, partitioning) of a single table.
    This is essential before constructing SQL queries against the table. 
    It also includes comprehensive lineage information that goes beyond traditional table-to-table dependencies:

    **Table Lineage:**
    - Upstream tables (tables this table reads from)
    - Downstream tables (tables that read from this table)

    The output is formatted in Markdown.

    Args:
        table_name: The fully qualified three-level name of the table (e.g., `catalog.schema.table`).
    """
    try:
        result = await asyncio.to_thread(
            get_table_info,
            table_name=table_name,
        )
        return result
    except Exception as e:
        return f"Error getting detailed table description for '{table_name}': {str(e)}"