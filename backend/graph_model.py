"""
Graph model for SAP Order-to-Cash data.

Defines node types, edge relationships, and provides a function
to build a graph from the SQLite database.
"""

import sqlite3
from pathlib import Path


GRAPH_SCHEMA = {
    "nodes": {
        "SalesOrder": {
            "table": "sales_order_headers",
            "id_field": "salesOrder",
        },
        "SalesOrderItem": {
            "table": "sales_order_items",
            "id_fields": ["salesOrder", "salesOrderItem"],
        },
        "DeliveryHeader": {
            "table": "outbound_delivery_headers",
            "id_field": "deliveryDocument",
        },
        "DeliveryItem": {
            "table": "outbound_delivery_items",
            "id_fields": ["deliveryDocument", "deliveryDocumentItem"],
        },
        "BillingHeader": {
            "table": "billing_document_headers",
            "id_field": "billingDocument",
        },
        "BillingItem": {
            "table": "billing_document_items",
            "id_fields": ["billingDocument", "billingDocumentItem"],
        },
        "JournalEntry": {
            "table": "journal_entry_items_accounts_receivable",
            "id_field": "accountingDocument",
        },
        "Payment": {
            "table": "payments_accounts_receivable",
            "id_fields": ["accountingDocument", "accountingDocumentItem"],
        },
        "BusinessPartner": {
            "table": "business_partners",
            "id_field": "businessPartner",
        },
        "Product": {
            "table": "products",
            "id_field": "product",
        },
        "Plant": {
            "table": "plants",
            "id_field": "plant",
        },
    },
    "edges": [
        {
            "name": "HAS_ITEM",
            "source": "SalesOrder",
            "target": "SalesOrderItem",
            "join": {"salesOrder": "salesOrder"},
        },
        {
            "name": "CONTAINS_PRODUCT",
            "source": "SalesOrderItem",
            "target": "Product",
            "join": {"material": "product"},
        },
        {
            "name": "DELIVERS",
            "source": "DeliveryItem",
            "target": "SalesOrderItem",
            "join": {
                "referenceSdDocument": "salesOrder",
                "referenceSdDocumentItem": "salesOrderItem",
            },
        },
        {
            "name": "FROM_PLANT",
            "source": "DeliveryItem",
            "target": "Plant",
            "join": {"plant": "plant"},
        },
        {
            "name": "BILLS",
            "source": "BillingItem",
            "target": "SalesOrderItem",
            "join": {
                "referenceSdDocument": "salesOrder",
                "referenceSdDocumentItem": "salesOrderItem",
            },
        },
        {
            "name": "CREATES_JOURNAL",
            "source": "BillingHeader",
            "target": "JournalEntry",
            "join": {"accountingDocument": "accountingDocument"},
        },
        {
            "name": "PAYS",
            "source": "Payment",
            "target": "BillingHeader",
            "join": {"invoiceReference": "billingDocument"},
        },
        {
            "name": "PLACES_ORDER",
            "source": "BusinessPartner",
            "target": "SalesOrder",
            "join": {"businessPartner": "soldToParty"},
        },
    ],
}


def get_node_id(node_type: str, row: dict) -> str:
    """Generate a unique node ID from row data."""
    schema = GRAPH_SCHEMA["nodes"][node_type]
    if "id_field" in schema:
        return f"{node_type}:{row[schema['id_field']]}"
    else:
        parts = [row[f] for f in schema["id_fields"]]
        return f"{node_type}:{'-'.join(parts)}"


def get_node_label(node_type: str, row: dict) -> str:
    """Generate a human-readable label for a node."""
    schema = GRAPH_SCHEMA["nodes"][node_type]
    if "id_field" in schema:
        return row[schema["id_field"]]
    else:
        parts = [row[f] for f in schema["id_fields"]]
        return "-".join(parts)


def query_table(conn: sqlite3.Connection, table: str) -> list[dict]:
    """Query all rows from a table as dicts."""
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    return [dict(row) for row in cursor.fetchall()]


def build_graph(db_path: str | Path) -> dict:
    """
    Build a graph from the SQLite database.

    Returns:
        {
            "nodes": [{"id": str, "type": str, "label": str, "data": dict}, ...],
            "edges": [{"source": str, "target": str, "relationship": str}, ...]
        }
    """
    conn = sqlite3.connect(db_path)
    nodes = []
    edges = []

    # Store all rows by type for edge building
    rows_by_type = {}
    node_id_map = {}  # Maps (type, row_index) -> node_id

    # Build all nodes
    for node_type, schema in GRAPH_SCHEMA["nodes"].items():
        table = schema["table"]
        rows = query_table(conn, table)
        rows_by_type[node_type] = rows

        for i, row in enumerate(rows):
            node_id = get_node_id(node_type, row)
            label = get_node_label(node_type, row)

            nodes.append({
                "id": node_id,
                "type": node_type,
                "label": label,
                "data": row,
            })
            node_id_map[(node_type, i)] = node_id

    # Build all edges by matching join conditions
    for edge_def in GRAPH_SCHEMA["edges"]:
        source_type = edge_def["source"]
        target_type = edge_def["target"]
        relationship = edge_def["name"]
        join = edge_def["join"]  # {source_field: target_field, ...}

        source_rows = rows_by_type.get(source_type, [])
        target_rows = rows_by_type.get(target_type, [])

        for si, source_row in enumerate(source_rows):
            source_id = node_id_map[(source_type, si)]

            for ti, target_row in enumerate(target_rows):
                # Check if all join conditions match
                match = True
                for source_field, target_field in join.items():
                    if source_row.get(source_field) != target_row.get(target_field):
                        match = False
                        break
                    # Skip if either value is empty/None
                    if not source_row.get(source_field):
                        match = False
                        break

                if match:
                    target_id = node_id_map[(target_type, ti)]
                    edges.append({
                        "source": source_id,
                        "target": target_id,
                        "relationship": relationship,
                    })

    conn.close()

    return {"nodes": nodes, "edges": edges}


if __name__ == "__main__":
    # Test the graph builder
    db_path = Path(__file__).parent / "data" / "o2c.db"
    graph = build_graph(db_path)

    print(f"Nodes: {len(graph['nodes'])}")
    print(f"Edges: {len(graph['edges'])}")

    # Count by type
    from collections import Counter
    node_types = Counter(n["type"] for n in graph["nodes"])
    edge_types = Counter(e["relationship"] for e in graph["edges"])

    print("\nNode counts:")
    for t, c in sorted(node_types.items()):
        print(f"  {t}: {c}")

    print("\nEdge counts:")
    for t, c in sorted(edge_types.items()):
        print(f"  {t}: {c}")
