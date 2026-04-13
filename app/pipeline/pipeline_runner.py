"""
전체 파이프라인 실행 관리 파일

주요 역할:
- SQLite 실행 환경 초기화
- 원본 데이터 적재
- SQL 파이프라인 실행
- 결과 CSV export
- 전체 실행 순서와 실행 로그 관리
"""


import time

from app.pipeline.init_environment import init_environment
from app.pipeline.loader import load_all_raw_data
from app.pipeline.sql_pipeline import run_sql_pipeline
from app.pipeline.exporter import export_all_results


# 전체 파이프라인 실행
def run_pipeline() -> None:
    """
    전체 자동화 파이프라인을 순차적으로 실행합니다.

    실행 순서:
    1. SQLite 환경 초기화
    2. 원본 CSV 적재
    3. SQL 파이프라인 실행
    4. BI 결과 CSV export
    """
    start_time = time.time()

    try:
        print("=" * 80)
        print("OLIST AUTOMATION PIPELINE")
        print("=" * 80)

        print("\n[1/4] Initializing SQLite environment ...")
        init_environment()

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
        print("파이프라인이 정상적으로 완료되었습니다.")
        print(f"Total execution time: {minutes} min {seconds:.2f} sec")
        print("=" * 80)
    
    except Exception as e:
        print("\n" + "=" * 80)
        print("파이파라인이 완료되지 않았습니다.")
        print(f"Error: {e}")
        print("=" * 80)
        raise

# 실행 진입점
def main() -> None:
    """
    전체 파이프라인 실행 진입 함수입니다.
    """
    run_pipeline()

if __name__ == "__main__":
    main()
