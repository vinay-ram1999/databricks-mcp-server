from typing import List, Dict, Optional
import logging

from databricks.sdk.service.catalog import TableInfo, TableConstraint, SchemaInfo, ColumnInfo, CatalogInfo, RegisteredModelInfo

logger = logging.getLogger(__name__)

def _format_column_info(column: ColumnInfo) -> str:
    """Format a single column into a compact row.
    
    Args:
        column: ColumnInfo object to format
        
    Returns:
        Formatted column row as string
    """
    name = column.name
    type_text = column.type_text
    nullable = "TRUE" if column.nullable else "FALSE"
    comment = column.comment or ""
    return f"| {name} | {type_text} | {nullable} | {comment} |"


def _format_columns(columns: Optional[List[ColumnInfo]]) -> str:
    """Format table columns into a markdown table.
    
    Args:
        columns: List of ColumnInfo objects
        
    Returns:
        Formatted markdown table with columns
    """
    if not columns:
        return "No columns defined."
    
    # Header
    rows = [
        "| Column | Type | Nullable | Comment |",
        "|--------|------|----------|---------|",
    ]
    
    # Add each column
    for col in columns:
        rows.append(_format_column_info(col))
    
    return "\n".join(rows)


def _format_lineage_info(lineage: Dict) -> str:
    """Format lineage information into a compact section.
    
    Parses the tableInfo from upstreams and downstreams and formats them
    as full_table_name (catalog.schema.table).
    
    Args:
        lineage: Lineage dictionary with 'upstreams' and/or 'downstreams' keys,
                 each containing tableInfo objects
        
    Returns:
        Formatted lineage section or empty string if no lineage data
    """
    if not lineage:
        return "No lineage information"
    
    sections = []
    
    def _extract_table_name(table_info: Optional[Dict]) -> Optional[str]:
        """Extract and format table name as catalog.schema.table."""
        if not table_info:
            return None
        catalog = table_info.get("catalog_name", "")
        schema = table_info.get("schema_name", "")
        table = table_info.get("name", "")
        
        if all([catalog, schema, table]):
            return f"{catalog}.{schema}.{table}"
        return None
    
    # Format upstreams - extract tableInfo from each upstream entry
    upstreams = lineage.get("upstreams", [])
    if upstreams:
        upstream_tables = []
        for upstream in upstreams:
            table_info = upstream.get("tableInfo")
            table_name = _extract_table_name(table_info)
            if table_name:
                upstream_tables.append(table_name)
        
        if upstream_tables:
            sections.append(f"**Upstream Tables:** {', '.join(f'`{t}`' for t in upstream_tables)}")
    
    # Format downstreams - extract tableInfo from each downstream entry
    downstreams = lineage.get("downstreams", [])
    if downstreams:
        downstream_tables = []
        for downstream in downstreams:
            table_info = downstream.get("tableInfo")
            table_name = _extract_table_name(table_info)
            if table_name:
                downstream_tables.append(table_name)
        
        if downstream_tables:
            sections.append(f"**Downstream Tables:** {', '.join(f'`{t}`' for t in downstream_tables)}")
    
    return "\n".join(sections) if sections else ""


def _format_table_constraints(constraints: List[TableConstraint]) -> str:
    """Format table constraints into a markdown section.
    
    Args:
        constraints: List of TableConstraint objects
        
    Returns:
        Formatted markdown table with constraints or empty string if none exist
    """
    if not constraints:
        return "No constraints"

    rows = [
        "| Constraint Name | Type | Columns | Details | Rely |",
        "|---|---|---|---|---|",
    ]

    for constraint in constraints:
        # Determine which concrete constraint is present
        if constraint.primary_key_constraint:
            pk = constraint.primary_key_constraint
            name = pk.name
            child_cols = pk.child_columns
            cols_str = ", ".join(child_cols) if len(child_cols) > 1 else child_cols[0]
            timeseries = pk.timeseries_columns
            details = f"timeseries_columns={', '.join(timeseries)}" if timeseries else "N/A"
            rely = pk.rely
            rely_str = "TRUE" if rely is True else ("FALSE" if rely is False else "UNKNOWN")
            rows.append(f"| {name} | PRIMARY_KEY | {cols_str} | {details} | {rely_str} |")
            continue

        if constraint.foreign_key_constraint:
            fk = constraint.foreign_key_constraint
            name = fk.name
            child_cols = fk.child_columns
            cols_str = ", ".join(child_cols) if len(child_cols) > 1 else child_cols[0]
            parent_table = fk.parent_table
            parent_cols = fk.parent_columns
            parent_str = f"references {parent_table}({', '.join(parent_cols)})"
            rely = fk.rely
            rely_str = "TRUE" if rely is True else ("FALSE" if rely is False else "UNKNOWN")
            rows.append(f"| {name} | FOREIGN_KEY | {cols_str} | {parent_str} | {rely_str} |")
            continue

        if constraint.named_table_constraint:
            nt = constraint.named_table_constraint
            name = nt.name
            rows.append(f"| {name} | NAMED_CONSTRAINT | N/A | N/A | N/A |")
            continue

    return "\n".join(rows)


def _format_single_table(table: TableInfo, lineage: Dict) -> str:
    """Format a single table's information into markdown.
    
    Args:
        table: TableInfo object containing table metadata
        lineage: Optional lineage information dictionary
        extended: Whether to include extended information
        
    Returns:
        Formatted markdown string for the table
    """
    # Basic table info
    full_name = table.full_name
    table_type = table.table_type or "UNKNOWN"
    data_source = table.data_source_format or "UNKNOWN"
    comment = table.comment or "No description"
    
    # Build the table section
    lines = [
        f"## Table Info",
        f"**Name:** `{full_name}`",
        f"**Type:** `{table_type.value}`",
        f"**Data Format:** `{data_source.value}`",
        f"**Description:** {comment}",
        "",
    ]

    # Add schema/columns section
    if table.columns:
        lines.append("### Schema")
        lines.append(_format_columns(table.columns))
        lines.append("")
        
    # Add table constraints section if available
    constraints_section = _format_table_constraints(table.table_constraints)
    if constraints_section:
        lines.append("### Constraints")
        lines.append(constraints_section)
        lines.append("")
    
    # Add lineage section if available
    lineage_section = _format_lineage_info(lineage)
    if lineage_section:
        lines.append("### Lineage")
        lines.append(lineage_section)
        lines.append("")
        
    return "\n".join(lines)


def format_table_info(table_info: List[TableInfo], lineage_info: Optional[List[Dict]] = [], extended: Optional[bool] = False) -> str:
    """Parse and format multiple table information into LLM-friendly Markdown.
    
    This function takes table metadata and lineage information and formats it
    into a structured markdown document optimized for consumption by LLMs.
    
    Args:
        table_info: table metadata objects
        lineage_info: lineage data for each table
        extended: Whether to include extended information
            
    Returns:
        Formatted markdown string with all table information
    """
    if not table_info:
        return "**No tables found**"

    if lineage_info:
        print(lineage_info)
        assert len(table_info) == len(lineage_info), ValueError("table info and lineage info length mismatch")
    else:
        lineage_info.extend({} for _ in table_info)
    
    # Build the document
    if extended:
        doc_lines = [
            "# Table Information",
            "",
        ]
        
        # Format each table
        for idx, (table, lineage) in enumerate(zip(table_info, lineage_info), 1):
            table: TableInfo
            logger.info(f"parsing table `{table.full_name}` info")
            
            try:
                formatted_table = _format_single_table(table, lineage, extended)
                doc_lines.append(formatted_table)
            except Exception as e:
                msg = f"Error parsing table info `{table.full_name}`: {e}"
                logger.error(msg)
                logger.warning("Falling back to entire table info and lineage information")
                doc_lines.append(msg)
                doc_lines.append("")
                doc_lines.append("Table Info:")
                doc_lines.append(f"{table.as_dict()}")
                doc_lines.append("")
                doc_lines.append("Lineage Info:")
                doc_lines.append(f"{lineage_info}")
                doc_lines.append("")

            # Add separator between tables (except after the last one)
            if idx < len(table_info):
                doc_lines.append("---")
                doc_lines.append("")
    else:
        doc_lines = [
            f"# List of tables in `{table_info[0].catalog_name}.{table_info[0].schema_name}`",
            "",
            f"*Number of Tables*: {len(table_info)}",
            "",
            "## Table names:",
            ""
        ]

        table_names = [table.name for table in table_info]
        doc_lines.append(", ".join(table_names))
        doc_lines.append("")

    return "\n".join(doc_lines)


