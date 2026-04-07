/****************************************************************************************************************************************/


/*
 * File: 02_stg_customers.sql
 * Description:
 * 	- Source 데이터: olist_raw.customers
 *  - 고객 키(customer_id / customer_unique_id) 기준 조인 안전성 확보
 * 	- 지역 컬럼(city/state/zip_code) 표준화
 * Notes:
 * 	- customer_id는 orders.customer_id의 조인 키이므로 PK로 사용하였습니다.
 * 	- 모든 컬럼에 결측치가 발견되지 않았으나, 추가 데이터 유입 시 데이터 손실을 막기 위해 지역 속성 컬럼(zip/city/state)은 NULL을 허용하였습니다.
 * 	- customer_unique_id는 사람 단위 분석용으로 사용될 것이므로 인덱스만 부여하였습니다. (중복 존재)
 * 	- 정합성 위반이나 결측 값 및 빈 문자열은 없는 것으로 확인되어, 본 테이블에 플래그로 저장하지 않고, 통합 DQ 요약 스크립트에서 계산하여 관리합니다.
 */


/****************************************************************************************************************************************/

USE olist_stg;


/*
 * customers 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포멧 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- customers 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 customers 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */


-- 샘플
SELECT  *
  FROM  olist_raw.customers
 LIMIT  10;

-- 원본 데이터 타입
DESCRIBE olist_raw.customers;

-- row count (99,441 행)
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.customers;

-- customer_id의 유니크 확인 (PK 사용 가능 여부) -> cnt: 99,441 / distinct_cnt: 99,441 / 중복 개수: 0건 / 공백 개수: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT customer_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT customer_id) AS dup_cnt
		,SUM(customer_id IS NULL OR customer_id = '') AS blank_cnt
  FROM  olist_raw.customers;

-- customer_unique_id 중복 패턴 확인 (사람 단위 분석) -> cnt: 99,441 / distinct_unique_cnt: 96,096 / 중복 개수: 3,345건 / 공백 개수: 0건
-- 	- customer_unique_id는 PK로는 사용 불가
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT customer_unique_id) AS distinct_unique_cnt
		,COUNT(*) - COUNT(DISTINCT customer_unique_id) AS dup_unique_cnt
		,SUM(customer_unique_id IS NULL OR customer_unique_id = '') AS blank_unique_cnt
  FROM  olist_raw.customers;

-- 필수 컬럼 결측 확인 (customer_zip_code_prefix: 0건 / customer_city: 0건 / customer_state: 0건)
SELECT  SUM(customer_zip_code_prefix IS NULL OR customer_zip_code_prefix = '') AS zip_code_blank_cnt
		,SUM(customer_city IS NULL OR customer_city = '') AS city_blank_cnt
		,SUM(customer_state IS NULL OR customer_state = '') AS state_blank_cnt
  FROM  olist_raw.customers;

-- state 분포 확인 -> SP: 41.98% / RJ: 12.92% / MG: 11.7% / 이후 다른 state는 10% 미만의 분포를 가짐
WITH CTE AS (
	SELECT  UPPER(TRIM(customer_state)) AS state_norm
			,COUNT(*) AS cnt
	  FROM  olist_raw.customers
	 GROUP
	    BY  1
)
SELECT  state_norm
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- customer_state 길이 이상치 확인 -> 길이가 2 이상인 state 개수: 0건
SELECT  SUM(customer_state IS NOT NULL 
		AND TRIM(REPLACE(customer_state, '\r', '')) <> '' 
		AND LENGTH(TRIM(REPLACE(customer_state, '\r', ''))) <> 2) AS state_len_invalid_cnt
  FROM  olist_raw.customers;

-- customer_zip_code_prefix의 데이터 타입 변경 여부 확인 -> 최소 길이: 5 / 최대 길이: 5 / 문자열: 0
SELECT  MIN(LENGTH(TRIM(REPLACE(customer_zip_code_prefix, '\r', '')))) AS min_len
		,MAX(LENGTH(TRIM(REPLACE(customer_zip_code_prefix, '\r', '')))) AS max_len
		,SUM(TRIM(REPLACE(customer_zip_code_prefix, '\r', '')) REGEXP '^[0-9]+$' = 0) AS non_numeric_cnt
  FROM  olist_raw.customers
 WHERE  customer_zip_code_prefix IS NOT NULL
   AND  TRIM(REPLACE(customer_zip_code_prefix, '\r', '')) <> '';


/*
 * stg_customers 테이블 ETL:
 * 	- olist_raw.customers 데이터의 타입을 변환 -> customer_zip_code_prefix: CHAR(5) (길이가 5로 고정 / 조인 안정성을 위해 문자형) / customer_state: CHAR(2) (길이가 2로 고정)
 * 	- city와 state를 결합한 customer_city_state 컬럼 생성	
 * 
 * Note:
 * 	- 시간 관련 컬럼이 없기 때문에 리드타임 파생 컬럼은 생성되지 않습니다.
 * 	- 추후 분석의 편의를 위해 city와 state를 결합한 파생 컬럼을 생성하였습니다.
 * 	- customer_zip_code_prefix의 경우 모두 숫자 / 길이 5로 고정이나, 안전성을 고려해 문자열로 CHAR(5)로 지정하였습니다.
 * 	- 사전 DQ 결과, 정합성 위반 row는 따로 발견되지 않아 플래그 컬럼은 생성하지 않았습니다. (전체 DQ 스크립트를 통해 다시 한번 점검 예정)
 */

DROP TABLE IF EXISTS olist_stg.stg_customers;

-- 테이블 생성
CREATE TABLE olist_stg.stg_customers (
	customer_id				  VARCHAR(50) NOT NULL,
	customer_unique_id		  VARCHAR(50) NOT NULL,
	
	-- 고객 지역 정보
	customer_zip_code_prefix  CHAR(5) NULL,
	customer_city			  VARCHAR(100) NULL,
	customer_state			  CHAR(2) NULL,
	
	-- 결합 컬럼
	customer_city_state		  VARCHAR(200) NULL,
	
	-- PK 및 Indexs 지정
	PRIMARY KEY (customer_id),
	INDEX idx_stg_customers_unique_id (customer_unique_id),
	INDEX idx_stg_customers_zip_prefix (customer_zip_code_prefix),
	INDEX idx_stg_customers_state (customer_state)
);


-- 데이터 적재
-- 	- customer_city: LOWER(TRIM(REPLACE('\r', ''))): 공백, 줄 바꿈 제거 및 소문자로 표준화
-- 	- customer_state: UPPER(TRIM(REPLACE('\r', ''))): 공백, 줄 바꿈 제거 및 대문자로 표준화
-- 	- 사전 DQ 상 결측 0건이었으나, 운영 확장 가능성을 고려해 속성 컬럼은 NULL 허용
-- 	- customer_city_state는 "-"를 통해 연결
TRUNCATE TABLE olist_stg.stg_customers;


INSERT INTO olist_stg.stg_customers  (
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state,
	customer_city_state
)
SELECT  customer_id
		,customer_unique_id
		,zip_norm AS customer_zip_code_prefix
		,city_norm AS customer_city
		,state_norm AS customer_state
		,CONCAT(city_norm, '_', state_norm) AS customer_city_state
  FROM  (
  		SELECT  customer_id
				,customer_unique_id
				,TRIM(REPLACE(customer_zip_code_prefix, '\r', '')) AS zip_norm
				,LOWER(TRIM(REPLACE(customer_city, '\r', ''))) AS city_norm
				,UPPER(TRIM(REPLACE(customer_state, '\r', ''))) AS state_norm
	  	  FROM  olist_raw.customers
  		) cleaned
 WHERE  customer_id IS NOT NULL
   AND  customer_id <> '';



-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_customers
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_customers;

-- 행 수 확인: raw.orders: 99,441 행 / stg_customers: 99,441 행
SELECT  (SELECT COUNT(*) FROM olist_raw.customers) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_customers) AS stg_cnt;

-- PK 중복 재확인 (전체 행: 99,441 행 / customer_id의 distinct count: 99,441 / 중복 개수: 0 건)
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT customer_id) AS  distinct_cnt
		,COUNT(*) - COUNT(DISTINCT customer_id) AS dup_cnt
  FROM  olist_stg.stg_customers;

-- 정합성 재확인(1) -> NULL 값 혹은 공백 0 건
SELECT  SUM(customer_zip_code_prefix IS NULL OR customer_zip_code_prefix = '') AS zip_code_blank_cnt
		,SUM(customer_city IS NULL OR customer_city = '') AS city_blank_cnt
		,SUM(customer_state IS NULL OR customer_state = '') AS state_blank_cnt
  FROM  olist_stg.stg_customers;

-- 정합성 재확인(2) -> customer_state 길이 2 고정
SELECT  SUM(customer_state IS NOT NULL 
		AND TRIM(REPLACE(customer_state, '\r', '')) <> '' 
		AND LENGTH(TRIM(REPLACE(customer_state, '\r', ''))) <> 2) AS state_len_invalid_cnt
  FROM  olist_stg.stg_customers;

-- 정합성 재확인(3) -> customer_zip_code_prefix의 최소 길이: 5 / 최대 길이: 5 / 문자열: 0
SELECT  MIN(LENGTH(TRIM(REPLACE(customer_zip_code_prefix, '\r', '')))) AS min_len
		,MAX(LENGTH(TRIM(REPLACE(customer_zip_code_prefix, '\r', '')))) AS max_len
		,SUM(TRIM(REPLACE(customer_zip_code_prefix, '\r', '')) REGEXP '^[0-9]+$' = 0) AS non_numeric_cnt
  FROM  olist_stg.stg_customers
 WHERE  customer_zip_code_prefix IS NOT NULL
   AND  TRIM(REPLACE(customer_zip_code_prefix, '\r', '')) <> '';

-- stg_orders와의 조인 정합성 -> 전체 행: 99,441건 / 조인 실패 행: 0건 / match percent: 100%
SELECT  COUNT(*) AS orders_cnt
		,SUM(c.customer_id IS NULL) AS unmatched_cnt
		,ROUND((1 - SUM(c.customer_id IS NULL) / NULLIF(COUNT(*), 0)) * 100, 4) AS match_rate_pct
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_stg.stg_customers AS c
    ON  o.customer_id = c.customer_id;

















