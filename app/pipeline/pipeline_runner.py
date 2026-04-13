"""
전체 파이프라인 실행 관리 파일

주요 역할:
- SQLite 기반 데이터 파이프라인 실행 관리
- 데이터 적재 → SQL 실행 → 결과 export
- 필요 시 DB 초기화 (선택적)

설계 원칙:
- DB 초기화는 기본적으로 수행하지 않음
- Streamlit 환경에서 안정적으로 동작하도록 구성
- 단계별 실행 흐름 명확화
"""

import time

from app.config.settings import SQLITE_DB_PATH
from app.pipeline.init_environment import init_environment
from app.pipeline.loader import load_all_raw_data
from app.pipeline.sql_pipeline import run_sql_pipeline
from app.pipeline.exporter import export_all_results

# 전체 파이프라인 
def run_pipeline(force_reset: bool = False) -> None:
    """
    전체 데이터 파이프라인을 실행합니다.

    Parameters:
    - force_reset: True일 경우 SQLite DB를 강제 초기화

    실행 순서:
    1. (선택) SQLite 환경 초기화
    2. 원본 CSV 적재
    3. SQL 파이프라인 실행
    4. BI 결과 CSV export
    """
    start_time = time.time()

    try:
        print("=" * 80)
        print("OLIST AUTOMATION PIPELINE")
        print("=" * 80)

        if force_reset or not SQLITE_DB_PATH.exists():
            print("\n[1/4] Initializing SQLite environment ...")
            init_environment(force_reset=force_reset)
        else:
            print("\n[1/4] SQLite environment already exists. Skip initialization.")

        print("\n[2/4] Loading raw CSV data ...")
        load_all_raw_data()

        print("\n[3/4] Running SQL pipeline ...")
        run_sql_pipeline()

        print("\n[4/4] Exporting result CSV files ...")
        export_all_results()

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60

        print("\n" + "=" * 80)
        print("파이프라인이 정상적으로 완료되었습니다.")
        print(f"Total execution time: {minutes} min {seconds:.2f} sec")
        print("=" * 80)

    except Exception as e:
        print("\n" + "=" * 80)
        print("파이프라인 실행 중 오류 발생")
        print(f"Error: {e}")
        print("=" * 80)
        raise

# 전체 초기화 + 파이프라인 실행 (Full Rebuild 전용)
def run_full_rebuild() -> None:
    """
    SQLite DB를 완전히 초기화한 후 전체 파이프라인을 실행합니다.

    주의:
    - 기존 데이터가 모두 삭제됩니다.
    - Streamlit 실행 중 호출하지 않는 것이 권장됩니다.
    """

    print("\n⚠️ FULL REBUILD MODE")

    run_pipeline(force_reset=True)

# 실행 진입점
def main() -> None:
    """
    CLI 실행 진입 함수
    """
    run_pipeline()


if __name__ == "__main__":
    main()