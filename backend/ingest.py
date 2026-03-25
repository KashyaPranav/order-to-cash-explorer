"""
Ingest SAP O2C data from JSONL files into SQLite database.

Walks through backend/data/sap-o2c-data/, reads all JSONL files per subfolder,
creates one table per entity type, and inserts all records.

Idempotent: drops and recreates tables on each run.
Production-ready: uses relative paths, logs progress, handles errors gracefully.
"""

import json
import sqlite3
import sys
from pathlib import Path

# Use paths relative to this script's location
SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR = SCRIPT_DIR / "data" / "sap-o2c-data"
DB_PATH = SCRIPT_DIR / "data" / "o2c.db"


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
    """Main ingestion function with error handling and progress logging."""
    print("=" * 60)
    print("SAP O2C Data Ingestion")
    print("=" * 60)
    print(f"Script directory: {SCRIPT_DIR}")
    print(f"Data directory:   {DATA_DIR}")
    print(f"Database path:    {DB_PATH}")
    print("-" * 60)

    # Check data directory exists
    if not DATA_DIR.exists():
        print(f"ERROR: Data directory not found: {DATA_DIR}")
        print("Make sure sap-o2c-data folder exists in backend/data/")
        sys.exit(1)

    # Ensure data directory parent exists for database
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    summary = []
    errors = []

    # Process each subfolder
    subfolders = sorted([f for f in DATA_DIR.iterdir() if f.is_dir() and not f.name.startswith(".")])
    print(f"Found {len(subfolders)} entity folders to process\n")

    for i, subfolder in enumerate(subfolders, 1):
        table_name = sanitize_table_name(subfolder.name)
        print(f"[{i}/{len(subfolders)}] Processing: {table_name}")

        try:
            # Read records
            records = read_jsonl_files(subfolder)

            if not records:
                print(f"    -> Skipped: no records found")
                continue

            # Create table and insert
            columns = infer_columns(records)
            create_table(conn, table_name, columns)
            insert_records(conn, table_name, columns, records)

            print(f"    -> Success: {len(records)} rows, {len(columns)} columns")
            summary.append((table_name, len(records)))

        except Exception as e:
            error_msg = f"Failed to process {table_name}: {str(e)}"
            print(f"    -> ERROR: {error_msg}")
            errors.append(error_msg)
            # Continue with next table instead of failing completely
            continue

    conn.close()

    # Print summary
    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)

    if summary:
        total_rows = 0
        for table_name, count in summary:
            print(f"  {table_name}: {count} rows")
            total_rows += count
        print("-" * 60)
        print(f"  Total: {len(summary)} tables, {total_rows} rows")

    if errors:
        print(f"\n  Errors: {len(errors)}")
        for err in errors:
            print(f"    - {err}")

    print(f"\nDatabase created at: {DB_PATH}")
    print("=" * 60)

    # Exit with error code if there were failures
    if errors and not summary:
        sys.exit(1)

    return len(summary), len(errors)


if __name__ == "__main__":
    main()
