from app.config.settings import SQLITE_DB_PATH
from app.core.db import get_connection

def main():
    print("DB Path: ", SQLITE_DB_PATH)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT 1;")
    result = cursor.fetchone()

    print("SQLite connected successfully.")
    print("Result: ", result[0])

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()