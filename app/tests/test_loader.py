from app.core.db import get_connection
from app.pipeline.loader import load_csv_to_table
from app.config.settings import RAW_DATA_DIR

def main():
    csv_path = RAW_DATA_DIR / "olist_orders_dataset.csv"
    table_name = "raw_orders"

    load_csv_to_table(csv_path, table_name)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) AS cnt FROM raw_orders;")
    row = cursor.fetchone()

    print("Loader test completed.")
    print("Row count: ", row["cnt"])

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()