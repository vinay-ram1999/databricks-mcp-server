import logging

from ._databricks_client import DatabricksClient

logger = logging.getLogger(__name__)

client: DatabricksClient = DatabricksClient.authorize()

def get_table_lineage(table_name: str) -> dict:
    """
    Fetches the table upstream and downstream lineage information.
    """
    logger.info(f"fetching lineage for: {table_name}")

    endpoint = "/api/2.0/lineage-tracking/table-lineage/"
    params = {"table_name": table_name, "include_entity_lineage": True}

    resp = client.do(
        method="GET",
        endpoint=endpoint,
        params=params
    )

    return resp
