"""
전체 파이프라인 실행 관리 파일

주요 역할:
- 스키마 초기화
- 원본 데이터 적재
- SQL 파이프라인 실행
- 결과 CSV export
- 전체 실행 순서와 실행 로그 관리
"""

import time

from pipeline.init_schema import init_schema
from pipeline.loader import load_all_raw_data
from pipeline.sql_pipeline import run_sql_pipeline
from pipeline.exporter import export_all_results

def run_pipeline() -> None:
    start_time = time.time()

    try:
        print("=" * 80)
        print("OLIST AUTOMATION PIPELINE")
        print("=" * 80)

        print("\n[1/4] Initializing schema ...")
        init_schema()

        print("\n[2/4] Loading raw CSV data ...")
        load_all_raw_data()

        print("\n[3/4] Running SQL pipeline ...")
        run_sql_pipeline()

        print("\n[4/4] Exporting result CSV files ...")
        export_all_results()

        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print(f"Total execution time: {minutes} min {seconds:.2f} sec")
        print("=" * 80)
    
    except Exception:
        print("\n" + "=" * 80)
        print("PIPELINE FAILED")
        print("=" * 80)
        raise

def main() -> None:
    run_pipeline()

if __name__ == "__main__":
    main()