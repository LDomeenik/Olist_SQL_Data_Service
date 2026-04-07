/******************************************************************************************************************************************************/


/*
 * File: dm_dim_geolocation.sql
 * Description:
 * 	- Data MArt 공용 지역 차원 생성 (source: olist_stg.stg_geolocation)
 * 	- Grain: 1 row = 1 zip_code_prefix (대표 city/state/lat/lng)
 * 
 * Note:
 * 	- 해당 dim_geolocation 테이블은 Sales Mart와 Operations Mart에서 모두 dimension 테이블로 사용되는 공용 차원 테이블입니다.
 * 	- stg_geolocation의 집계/로직을 그대로 가져왔습니다. (추가적인 집계/로직 없음)
 * 	- 따라서 DQ, QC의 경우. 이미 품질 검사가 완료된 데이터이기 때문에 기본적인 적재 정합성만을 확인합니다.
 * 	- 조인 정합성 확인 결과 조인 누락이 있는 경우가 발견되었습니다 (customers: 278건 / sellers: 7건)
 * 	- geolocation은 대표 좌표와 주소를 설정하였다는 점과 커버리지의 한계로 인해 조인 누락이 발생할 수 있습니다.
 * 	- 따라서 해당 테이블을 다른 테이블과 조인할 때는 참조용으로 LEFT JOIN을 전제로 설계되었습니다.
*/


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.dim_geolocation;

-- DDL(테이블 생성)
CREATE TABLE olist_dm.dim_geolocation (
	geolocation_zip_code_prefix		CHAR(5)				NOT NULL,
	geolocation_lat					DECIMAL(10, 6)		NULL,
	geolocation_lng					DECIMAL(10, 6)		NULL,
	geolocation_city				VARCHAR(100)		NULL,
	geolocation_state				CHAR(2)				NULL,
	geolocation_city_state			VARCHAR(200)		NULL,
	row_cnt							INT					NOT NULL,
	mode_cnt						INT					NOT NULL,
	mode_ratio_pct					DECIMAL(6, 2)		NOT NULL,
	invalid_latlng_cnt				INT					NOT NULL,
	city_cnt						TINYINT				NOT NULL,
	state_cnt						TINYINT				NOT NULL,
	is_invalid_latlng_exists		TINYINT				NOT NULL,
	is_multi_city					TINYINT				NOT NULL,
	is_multi_state					TINYINT				NOT NULL,
	
	-- PK 및 Indexes
	PRIMARY KEY (geolocation_zip_code_prefix),
	INDEX idx_dm_dim_geolocation_city_state (geolocation_city_state),
	INDEX idx_dm_dim_geolocation_state (geolocation_state)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;



-- ===========================================================================================================================================


/*
 * ETL: dim_geolocation
 * 	- stg 레이어의 stg_geolocation을 복사
*/

TRUNCATE TABLE olist_dm.dim_geolocation;

-- 데이터 적재
INSERT INTO olist_dm.dim_geolocation (
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state,
	geolocation_city_state,
	row_cnt,
	mode_cnt,
	mode_ratio_pct,
	invalid_latlng_cnt,
	city_cnt,
	state_cnt,
	is_invalid_latlng_exists,
	is_multi_city,
	is_multi_state
)
SELECT  geolocation_zip_code_prefix
		,geolocation_lat
		,geolocation_lng
		,geolocation_city
		,geolocation_state
		,geolocation_city_state
		,row_cnt
		,mode_cnt
		,mode_ratio_pct
		,invalid_latlng_cnt
		,city_cnt
		,state_cnt
		,is_invalid_latlng_exists
		,is_multi_city
		,is_multi_state
  FROM  olist_stg.stg_geolocation;


-- ===========================================================================================================================================


/*
 * QC: dim_geolocation
 * 	- row count: 19,015건 (stg_geolocation: 19,015)
 * 	- PK 유니크 -> cnt: 19,015 / distinct_cnt: 19,015 / 중복: 0건 / 결측 및 공백: 0건
 * 	- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
 * 	- 플래그 일관성 -> 플래그와 실제 값이 다른 건수: 0건
 * 	- 조인 정합성 
 * 		-> customers에는 있지만 geolocation에는 없는 zip_code_prefix: 278건
 * 		-> sellers에는 있지만 geolocation에는 없는 zip_code_prefix: 7건
*/


-- 샘플
SELECT  *
  FROM  olist_dm.dim_geolocation
 LIMIT  50;

-- 데이터 타입
DESCRIBE olist_dm.dim_geolocation;

-- row count: 19,015행 (stg_geolocation: 19,015행)
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.dim_geolocation;

SELECT  COUNT(*) AS row_cnt
  FROM  olist_stg.stg_geolocation;

-- PK 유니크 확인 -> cnt: 19,015 / distinct_cnt: 19,015 / 중복: 0건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix) AS dup_cnt
		,SUM(geolocation_zip_code_prefix IS NULL OR geolocation_zip_code_prefix = '') AS blank_cnt
  FROM  olist_dm.dim_geolocation;

-- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
SELECT  COUNT(*) AS mismatch_cnt
  FROM  olist_dm.dim_geolocation AS dg
  JOIN  olist_stg.stg_geolocation AS sg
    ON  sg.geolocation_zip_code_prefix = dg.geolocation_zip_code_prefix
 WHERE  NOT (dg.geolocation_zip_code_prefix <=> sg.geolocation_zip_code_prefix)
    OR  NOT (dg.geolocation_lat <=> sg.geolocation_lat)
    OR  NOT (dg.geolocation_lng <=> sg.geolocation_lng)
    OR  NOT (dg.geolocation_city <=> sg.geolocation_city)
    OR  NOT (dg.geolocation_state <=> sg.geolocation_state);

-- 플래그 일관성 -> 플래그와 실제 값이 다른 건수: 0건
SELECT  COUNT(*) AS cnt
  FROM  olist_dm.dim_geolocation
 WHERE  (is_multi_city = 1 AND city_cnt <= 1)
    OR  (is_multi_state = 1 AND state_cnt <= 1)
    OR  (is_invalid_latlng_exists = 1 AND invalid_latlng_cnt = 0);

-- 조인 정합성(1) -> customers에는 있지만 geolcation에는 없는 zip_code_prefix: 278건
SELECT  COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_customers AS c
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
   ON   dg.geolocation_zip_code_prefix = c.customer_zip_code_prefix
 WHERE  c.customer_zip_code_prefix IS NOT NULL
   AND  c.customer_zip_code_prefix <> ''
   AND  dg.geolocation_zip_code_prefix IS NULL;

-- 조인 정합성(2) -> sellers에는 있지만 geolocation에는 없는 zip_code_prefix: 7건
SELECT  COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
    ON  dg.geolocation_zip_code_prefix = s.seller_zip_code_prefix
 WHERE  s.seller_zip_code_prefix IS NOT NULL
   AND  s.seller_zip_code_prefix <> ''
   AND  dg.geolocation_zip_code_prefix IS NULL;




