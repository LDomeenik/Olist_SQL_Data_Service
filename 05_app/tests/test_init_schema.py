from pathlib import Path
from sql_runner import run_sql_file
from config import SQL_DIR

def main():
    sql_path = SQL_DIR / "01_load" / "01_init_environment_schema_only.sql"
    run_sql_file(sql_path)
    print("Schema initialization completed succesfully")

if __name__ == "__main__":
    main()