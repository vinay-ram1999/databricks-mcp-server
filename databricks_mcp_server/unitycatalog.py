import logging

from databricks.sdk.service.catalog import TableInfo, SchemaInfo, ColumnInfo, CatalogInfo
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from .config import DatabricksSDKConfig
from .lineage import get_table_lineage

logger = logging.getLogger(__name__)

config: Config = DatabricksSDKConfig.authorize()
sdk_client = WorkspaceClient(config=config)

def get_table_info(table_name: str) -> str:
    """
    Fetches table metadata and lineage, then formats it into a Markdown string.
    """
    logger.info(f"fetching metadata for: {table_name}")
    
    try:
        table_info: TableInfo = sdk_client.tables.get(full_name=table_name)
        table_lineage = get_table_lineage(table_name)
    except Exception as e:
        error_details = str(e)
        logger.error(error_details)
        return f"""
# Error: Could Not Retrieve Table Details for: `{table_name}`
**Details:**
```
{error_details}
```
"""
    
    return f"""{table_info.as_dict()}\n{table_lineage}"""


