from pathlib import Path

from app.core.sql_runner import run_sql_file
from app.core.db import get_connection

def main():
    sql_path = Path("04_sql/test_sqlite.sql")
    run_sql_file(sql_path)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM test_table;")
    rows = cursor.fetchall()

    print("SQL runner test completed.")
    print("Rows: ")
    for row in rows:
        print(dict(row))

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()