from typing import List
import logging

from databricks.sdk.service.catalog import TableInfo, SchemaInfo, ColumnInfo, CatalogInfo, RegisteredModelInfo
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from .config import DatabricksSDKConfig
from .lineage import get_table_lineage
from .utils import format_table_info

logger = logging.getLogger(__name__)

config: Config = DatabricksSDKConfig.authorize()
sdk_client = WorkspaceClient(config=config)


def get_tables_in_schema(catalog_name: str, schema_name: str) -> list[TableInfo]:
    """
    Fetches all tables in a given schema.
    """
    logger.info(f"fetching tables in schema: {catalog_name}.{schema_name}")
    
    tables = sdk_client.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    return tables


def get_table_info(table_names: List) -> str:
    """
    Fetches table metadata and lineage, then formats it into a Markdown string.
    """
    content = {"tableInfo": [], "lineageInfo": []}
    
    for table_name in table_names:
        table_info: TableInfo = sdk_client.tables.get(full_name=table_name)
        lineage_info = get_table_lineage(table_name)
        content["tableInfo"].append(table_info)
        content["lineageInfo"].append(lineage_info)
    
    # Format the inforamation into markdown
    output = format_table_info(content)
    return output


