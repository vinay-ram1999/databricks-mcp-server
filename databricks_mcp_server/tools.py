from typing import Optional, List
import logging

import asyncio

from .unitycatalog import get_table_info


logger = logging.getLogger(__name__)

async def fetch_table_info(table_names: List) -> str:
    """
    Provides a detailed description of a Unity Catalog table along with lineage information.
    
    Use this tool to understand the structure (columns, data types, partitioning) for a single table or multiple tables at once.
    This is essential before constructing SQL queries against the table. 
    It also includes comprehensive lineage information (if there is any) that goes beyond traditional table-to-table dependencies:

    **Table Lineage:**
    - Upstream tables (tables this table reads from)
    - Downstream tables (tables that read from this table)

    The output is formatted in Markdown.

    Args:
        table_names: A list of fully qualified three-level name of the table (e.g., ['catalog.schema.table', ...]).
    """
    logger.info(f"fetching metadata for: {table_names}")
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
        return f"""**Error**: Could Not Retrieve table information
**Details:**
```
{error_details}
```
"""