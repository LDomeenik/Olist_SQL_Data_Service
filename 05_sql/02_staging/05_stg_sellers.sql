/****************************************************************************************************************************************/


/*
 * File: 05_stg_sellers.sql
 * Description:
 * 	- Source 데이터: olist_raw.sellers
 *  - 판매자(seller) 단위의 Staging 테이블 생성
 * 	- 판매자 식별자(seller_id) 기준 조인 안전성 확보 및 분석용 타입 표준화 수행
 * 	- 판매자 위치 정보를 통합한 파생 컬럼 생성(seller_city_state)
 * Notes:
 * 	- seller_id를 PK로 사용하였습니다.
 * 	- 식별자 성격을 지닌 ID 컬럼(seller_id)은 NOT NULL을 적용하였습니다.
 * 	- 그 외의 컬럼에 대하여는 NULL을 허용하였습니다. (추후 데이터 확장 고려)
 * 	- seller_city_state의 경우 city와 state 중 하나라도 NULL일 경우 NULL이 되도록 설정하였습니다.
 * 	- 정합성 위반이나 결측 값 및 빈 문자열은 없는 것으로 확인되어, 본 테이블에 플래그로 저장하지 않고, 통합 DQ 요약 스크립트에서 계산하여 관리합니다.
 */


/****************************************************************************************************************************************/


USE olist_stg;


/*
 * sellers 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포맷 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- sellers 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 sellers 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */


-- 샘플
SELECT  *
  FROM  olist_raw.sellers
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_raw.sellers;

-- row count: 3,095행
SELECT  COUNT(*)
  FROM  olist_raw.sellers;

-- seller_id의 유니크 확인(PK 가능 여부) -> cnt: 3,095 / distinct_cnt: 3,095 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT seller_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT seller_id) AS dup_cnt
		,SUM(seller_id IS NULL OR TRIM(seller_id) = '') AS blank_cnt
  FROM  olist_raw.sellers;

-- 필수 컬럼 결측 확인
-- 	- seller_zip_code_prefix 공백 및 결측: 0건
-- 	- seller_city 공백 및 결측: 0건
-- 	- seller_state 공백 및 결측: 0건
SELECT  SUM(seller_zip_code_prefix IS NULL OR TRIM(seller_zip_code_prefix) = '') AS zip_code_blank_cnt
		,SUM(seller_city IS NULL OR TRIM(seller_city) = '') AS city_blank_cnt
		,SUM(seller_state IS NULL OR TRIM(seller_state) = '') AS state_blank_cnt
  FROM  olist_raw.sellers;

-- city 정규화 여부 확인 -> 정규화 전: 610건 / 정규화 후: 610건
SELECT  COUNT(DISTINCT seller_city) AS raw_city_distinct
		,COUNT(DISTINCT LOWER(TRIM(REPLACE(seller_city, '\r', '')))) AS norm_city_distinct
  FROM  olist_raw.sellers;

-- state 분포 확인 -> SP: 59.74%(1,849건) / PR: 11.28%(349건) / MG: 7.88%(244건) ...
WITH CTE AS (
	SELECT  UPPER(TRIM(REPLACE(seller_state, '\r', ''))) AS state_norm
			,COUNT(*) AS cnt
	  FROM  olist_raw.sellers
	 GROUP
	    BY  1
)
SELECT  state_norm
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- seller_state 길이 이상치 확인 -> 길이가 2 이상인 state 개수: 0건
SELECT  SUM(seller_state IS NOT NULL 
			AND TRIM(REPLACE(seller_state, '\r', '')) <> '' 
			AND LENGTH(TRIM(REPLACE(seller_state, '\r', ''))) <> 2) AS state_len_invalid_cnt
  FROM  olist_raw.sellers;

-- seller_zip_code_prefix의 데이터 타입 변경 여부 확인 -> 최소 길이: 5 / 최대 길이: 5 / 문자열: 0
SELECT  MIN(LENGTH(TRIM(REPLACE(seller_zip_code_prefix, '\r', '')))) AS min_len
		,MAX(LENGTH(TRIM(REPLACE(seller_zip_code_prefix, '\r', '')))) AS max_len
		,SUM(TRIM(REPLACE(seller_zip_code_prefix, '\r', '')) REGEXP '^[0-9]+$' = 0) AS non_numeric_cnt
  FROM  olist_raw.sellers
 WHERE  seller_zip_code_prefix IS NOT NULL
   AND  TRIM(REPLACE(seller_zip_code_prefix, '\r', '')) <> '';

-- 조인 정합성(1): order_items에는 있지만 sellers에는 없는 seller_id: 0건
SELECT  COUNT(DISTINCT oi.seller_id) AS missing_seller_cnt
  FROM  olist_raw.order_items AS oi
  LEFT
  JOIN  olist_raw.sellers AS s
    ON  s.seller_id = oi.seller_id
 WHERE  s.seller_id IS NULL;

-- 조인 정합성(2): sellers에는 있지만 order_items에는 없는 seller_id: 0건
SELECT  COUNT(DISTINCT s.seller_id) AS no_order_seller_cnt
  FROM  olist_raw.sellers AS s
  LEFT
  JOIN  olist_raw.order_items AS oi
    ON  oi.seller_id = s.seller_id
 WHERE  oi.seller_id IS NULL;


/*
 * stg_sellers 테이블 ETL:
 * 	- olist_raw.sellers 데이터의 타입을 변환 -> seller_zip_code_prefix: CHAR(5) (길이가 5로 고정 / 조인 안정성을 위해 문자형) / seller_state: CHAR(2) (길이가 2로 고정)
 * 	- city와 state를 결합한 seller_city_state 컬럼 생성	
 * 
 * Note:
 * 	- 시간 관련 컬럼이 없기 때문에 리드타임 파생 컬럼은 생성되지 않습니다.
 * 	- 추후 분석의 편의를 위해 city와 state를 결합한 파생 컬럼을 생성하였습니다.
 * 	- seller_zip_code_prefix의 경우 모두 숫자 / 길이 5로 고정이나, 안전성을 고려해 문자열로 CHAR(5)로 지정하였습니다.
 * 	- 사전 DQ 결과, 정합성 위반 row는 따로 발견되지 않아 플래그 컬럼은 생성하지 않았습니다. (전체 DQ 스크립트를 통해 다시 한번 점검 예정)
 */

DROP TABLE IF EXISTS olist_stg.stg_sellers;

-- 테이블 생성
CREATE TABLE olist_stg.stg_sellers (
	seller_id				VARCHAR(50)   NOT NULL,
	seller_zip_code_prefix  CHAR(5)		  NULL,
	seller_city				VARCHAR(100)  NULL,
	seller_state			CHAR(2)		  NULL,
	
	seller_city_state		VARCHAR(200)  NULL,
	
	-- PK 및 INDEX 지정
	PRIMARY KEY (seller_id),
	INDEX idx_stg_sellers_zip_prefix (seller_zip_code_prefix),
	INDEX idx_stg_sellers_state (seller_state)
);

-- 데이터 적재
TRUNCATE TABLE olist_stg.stg_sellers;

INSERT INTO olist_stg.stg_sellers (
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state,
	seller_city_state
)
SELECT  seller_id
		,zip_code_norm AS seller_zip_code_prefix
		,city_norm AS seller_city
		,state_norm AS seller_state
		,CASE WHEN city_norm IS NOT NULL AND state_norm IS NOT NULL THEN CONCAT(city_norm, '_', state_norm)
			  ELSE NULL
			  END AS seller_city_state
  FROM  (
  		SELECT  seller_id
  				,NULLIF(TRIM(REPLACE(seller_zip_code_prefix, '\r', '')), '') AS zip_code_norm
  				,NULLIF(LOWER(TRIM(REPLACE(seller_city, '\r', ''))), '') AS city_norm
  				,NULLIF(UPPER(TRIM(REPLACE(seller_state, '\r', ''))), '') AS state_norm
  		  FROM  olist_raw.sellers
  		) cleaned
 WHERE  seller_id IS NOT NULL
   AND  TRIM(seller_id) <> '';

 
-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_sellers
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_sellers;

-- row count: 3,095행
SELECT  COUNT(*)
  FROM  olist_stg.stg_sellers;

-- seller_id의 유니크 확인 -> cnt: 3,095 / distinct_cnt: 3,095 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT seller_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT seller_id) AS dup_cnt
		,SUM(seller_id IS NULL OR TRIM(seller_id) = '') AS blank_cnt
  FROM  olist_stg.stg_sellers;

-- 필수 컬럼 결측 확인
-- 	- seller_zip_code_prefix 공백 및 결측: 0건
-- 	- seller_city 공백 및 결측: 0건
-- 	- seller_state 공백 및 결측: 0건
SELECT  SUM(seller_zip_code_prefix IS NULL OR TRIM(seller_zip_code_prefix) = '') AS zip_code_blank_cnt
		,SUM(seller_city IS NULL OR TRIM(seller_city) = '') AS city_blank_cnt
		,SUM(seller_state IS NULL OR TRIM(seller_state) = '') AS state_blank_cnt
  FROM  olist_stg.stg_sellers;

-- city 분포 확인 -> sao paulo: 22.46%(695건) / curitiba: 4.1%(127건) / rio de janeiro: 3.1%(96건) ... 총 610개
WITH CTE AS (
	SELECT  seller_city
			,COUNT(*) AS cnt
	  FROM  olist_stg.stg_sellers
	 GROUP
	    BY  1
)
SELECT  seller_city
		,cnt
		,ROUND(cnt / SUM(cnt) over() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- state 분포 확인 -> SP: 59.74%(1,849건) / PR: 11.28%(349건) / MG: 7.88%(244건) ... 총 23개의 state
WITH CTE AS (
	SELECT  seller_state
			,COUNT(*) AS cnt
	  FROM  olist_stg.stg_sellers
	 GROUP
	    BY  1
)
SELECT  seller_state
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- city_state 분포 확인 -> sao paulo_SP: 22.46%(695건) / curitiba_PR: 4.01%(124건) / rio de janeiro_RJ: 3%(93건) ... 총 635개
-- city는 610개로 집계되며, 동일 city가 서로 다른 state에 존재하여 city_state의 개수가 증가함
WITH CTE AS (
	SELECT  seller_city_state
			,COUNT(*) AS cnt
	  FROM  olist_stg.stg_sellers
	 GROUP
	    BY  1
)
SELECT  seller_city_state
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- 조인 정합성(1): order_items에는 있지만 sellers에는 없는 seller_id: 0건
SELECT  COUNT(DISTINCT oi.seller_id) AS missing_seller_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_stg.stg_sellers AS s
    ON  s.seller_id = oi.seller_id
 WHERE  s.seller_id IS NULL;

-- 조인 정합성(2): sellers에는 있지만 order_items에는 없는 seller_id: 0건
SELECT  COUNT(DISTINCT s.seller_id) AS no_order_seller_cnt
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_stg.stg_order_items AS oi
    ON  oi.seller_id = s.seller_id
 WHERE  oi.seller_id IS NULL;














