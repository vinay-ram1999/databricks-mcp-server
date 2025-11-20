from typing import Dict, Any
import logging
import os

from databricks.sdk.service.sql import StatementResponse, StatementState, ResultManifest, ResultData, Format
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from .config import DatabricksSDKConfig

logger = logging.getLogger(__name__)

DATABRICKS_SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")

config: Config = DatabricksSDKConfig.authorize()
sdk_client = WorkspaceClient(config=config)


def execute_query(query: str) -> Dict[str, Any]:
    """
    Execute sql query using Databricks SQL Warehouse. 
    """
    resp: StatementResponse = sdk_client.statement_execution.execute_statement(
        statement=query,
        warehouse_id=DATABRICKS_SQL_WAREHOUSE_ID,
        wait_timeout="50s",
        format=Format.JSON_ARRAY,
        # row_limit=100,
    )
    logger.info(f"{resp.statement_id} - initiated statement execution")

    while True:
        if resp.status:
            if resp.status.state == StatementState.SUCCEEDED:
                logger.info(f"{resp.statement_id} - statement successfully executed")
                if resp.result.data_array:
                    data: ResultData = resp.result.data_array
                    manifest: ResultManifest = resp.manifest
                    column_names = [col.name for col in manifest.schema.columns]
                    results = [dict(zip(column_names, row)) for row in data]
                    return {"state": f"{resp.status.state.value}", "data": results}
                else:
                    return {"state": f"{resp.status.state.value}", "data": []}
            elif resp.status.error:
                msg = f"{resp.status.error.error_code} - {resp.status.error.message}"
                logger.error(f"{resp.statement_id} - statement failed: \n{msg}")
                return {"state": f"{resp.status.state.value}", "error": msg}

