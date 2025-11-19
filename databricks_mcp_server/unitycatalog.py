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


def get_schemas_in_catalog(catalog_name: str) -> list[SchemaInfo]:
    """
    Fetches all tables in a given catalog and schema.
    """    
    schemas: List[SchemaInfo] = sdk_client.schemas.list(catalog_name=catalog_name)
    schema_info = [schema for schema in schemas]

    # Format the inforamation into markdown
    output = None
    return output


def get_tables_in_schema(catalog_name: str, schema_name: str) -> list[TableInfo]:
    """
    Fetches all tables in a given catalog and schema.
    """    
    tables: List[TableInfo] = sdk_client.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    table_info = [table for table in tables]
    
    # Format the inforamation into markdown
    output = format_table_info(table_info=table_info, lineage_info=[], extended=False)
    return output


def get_table_info(table_names: List) -> str:
    """
    Fetches table metadata and lineage, then formats it into a Markdown string.
    """
    table_info = []
    lineage_info = []
    
    for table_name in table_names:
        tableInfo: TableInfo = sdk_client.tables.get(full_name=table_name)
        lineageInfo = get_table_lineage(table_name)
        table_info.append(tableInfo)
        lineage_info.append(lineageInfo)
    
    # Format the inforamation into markdown
    output = format_table_info(table_info=table_info, lineage_info=lineage_info, extended=True)
    return output


