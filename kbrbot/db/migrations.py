import sqlite3


def ensure_database_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name in columns:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
    conn.commit()

