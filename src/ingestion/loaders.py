import csv
from pathlib import Path

from psycopg import Connection

def load_csv(
    conn: Connection,
    file_path: Path,
    table_name: str,
    columns: list[str],
) -> int:
    with file_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        next(reader) #skip header

        with conn.cursor() as cur:
            with cur.copy(
                f"COPY {table_name}({', ' .join(columns)}) FROM STDIN"
            ) as copy:
                row_count = 0

                for row in reader:
                    copy.write_row(row)
                    row_count += 1

    return row_count