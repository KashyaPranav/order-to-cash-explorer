"""
Ingest SAP O2C data from JSONL files into SQLite database.

Walks through backend/data/sap-o2c-data/, reads all JSONL files per subfolder,
creates one table per entity type, and inserts all records.

Idempotent: drops and recreates tables on each run.
"""

import json
import os
import sqlite3
from pathlib import Path


DATA_DIR = Path(__file__).parent / "data" / "sap-o2c-data"
DB_PATH = Path(__file__).parent / "data" / "o2c.db"


def read_jsonl_files(folder: Path) -> list[dict]:
    """Read all .jsonl files in a folder and combine into one list."""
    records = []
    for jsonl_file in folder.glob("*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def infer_columns(records: list[dict]) -> list[str]:
    """Infer column names from the first record's keys."""
    if not records:
        return []
    return list(records[0].keys())


def sanitize_table_name(name: str) -> str:
    """Sanitize folder name for use as SQL table name."""
    return name.replace("-", "_").replace(" ", "_")


def create_table(conn: sqlite3.Connection, table_name: str, columns: list[str]):
    """Drop and recreate table with TEXT columns."""
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    cursor.execute(f"CREATE TABLE {table_name} ({col_defs})")
    conn.commit()


def serialize_value(value):
    """Serialize nested dicts/lists to JSON strings."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def insert_records(conn: sqlite3.Connection, table_name: str, columns: list[str], records: list[dict]):
    """Insert records into table, handling missing keys with None."""
    cursor = conn.cursor()
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(f'"{col}"' for col in columns)

    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    for record in records:
        values = [serialize_value(record.get(col)) for col in columns]
        cursor.execute(sql, values)

    conn.commit()


def main():
    print(f"Data directory: {DATA_DIR}")
    print(f"Database path: {DB_PATH}")
    print("-" * 50)

    if not DATA_DIR.exists():
        print(f"Error: Data directory not found: {DATA_DIR}")
        return

    conn = sqlite3.connect(DB_PATH)
    summary = []

    for subfolder in sorted(DATA_DIR.iterdir()):
        if not subfolder.is_dir():
            continue
        if subfolder.name.startswith("."):
            continue

        table_name = sanitize_table_name(subfolder.name)
        records = read_jsonl_files(subfolder)

        if not records:
            print(f"Skipping {table_name}: no records found")
            continue

        columns = infer_columns(records)
        create_table(conn, table_name, columns)
        insert_records(conn, table_name, columns, records)

        summary.append((table_name, len(records)))

    conn.close()

    print("\nIngestion Summary:")
    print("-" * 50)
    total = 0
    for table_name, count in summary:
        print(f"{table_name}: {count} rows")
        total += count
    print("-" * 50)
    print(f"Total: {len(summary)} tables, {total} rows")
    print(f"\nDatabase created at: {DB_PATH}")


if __name__ == "__main__":
    main()
