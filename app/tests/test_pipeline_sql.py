from config import SQL_DIR
from sql_runner import run_sql_folder

def main():
    # staging
    run_sql_folder(SQL_DIR / "02_staging")

    # datamart
    run_sql_folder(SQL_DIR / "03_datamart")

    # analysis module
    run_sql_folder(SQL_DIR / "04_analysismodule")

    # analysis
    run_sql_folder(SQL_DIR / "05_analysis")

    # bi
    run_sql_folder(SQL_DIR / "06_bi")

    print("\nAll SQL pipeline executed successfully.")

if __name__ == "__main__":
    main()