/******************************************************************************************************************************************************/


/*
 * File: dm_dim_customer.sql
 * Description:
 * 	- Data Mart 공용 고객 정보 차원 생성 (source: olist_stg.stg_customers)
 * 	- Grain: 1 row = 1 customer_id
 * 
 * Note:
 * 	- 해당 dim_customer 테이블은 Sales Mart와 Operations Mart에서 모두 dimension 테이블로 사용되는 공용 차원 테이블입니다.
 * 	- stg_customers의 집계/로직을 그대로 가져왔습니다. (추가적인 집계/로직 없음)
 * 	- 따라서 DQ, QC의 경우. 이미 품질 검사가 완료된 데이터이기 때문에 기본적인 적재 정합성만을 확인합니다.
*/


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.dim_customer;

-- DDL(테이블 생성)
CREATE TABLE olist_dm.dim_customer (
	customer_id					VARCHAR(50)		NOT NULL,
	customer_unique_id			VARCHAR(50)		NOT NULL,
	customer_zip_code_prefix	CHAR(5)			NULL,
	customer_city				VARCHAR(100)	NULL,
	customer_state				CHAR(2)			NULL,
	customer_city_state			VARCHAR(200)	NULL,
	
	-- PK 및 INDEX
	PRIMARY KEY (customer_id),
	INDEX idx_dm_dim_customer_unique_id (customer_unique_id),
	INDEX idx_dm_dim_customer_zip_prefix (customer_zip_code_prefix),
	INDEX idx_dm_dim_customer_state (customer_state),
	INDEX idx_dm_dim_customer_city_state (customer_city_state)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;



-- ===========================================================================================================================================


/*
 * ETL: dim_customer
 * 	- stg 레이어의 stg_customers를 복사
*/


TRUNCATE TABLE olist_dm.dim_customer;

-- 데이터 적재
INSERT INTO olist_dm.dim_customer (
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state,
	customer_city_state
)
SELECT  customer_id
		,customer_unique_id
		,customer_zip_code_prefix
		,customer_city
		,customer_state
		,customer_city_state
  FROM  olist_stg.stg_customers;


-- ===========================================================================================================================================


/*
 * QC: dim_customer
 * 	- row count: 99,441건 (stg_customers: 99,441)
 * 	- PK 유니크 -> cnt: 99,441 / distinct_cnt: 99,441 / 중복: 0건 / 결측 및 공백: 0건
 * 	- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
 * 	- 조인 정합성 -> customers에는 있지만 geolocation에는 없는 zip_code_prefix: 278건
*/


-- 샘플
SELECT  *
  FROM  olist_dm.dim_customer
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_dm.dim_customer;

-- row count: 99,441행 (stg_customers: 99,441행)
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.dim_customer;

SELECT  COUNT(*) AS stg_cnt
  FROM  olist_stg.stg_customers;

-- PK 유니크 확인 -> cnt: 99,441 / distinct_cnt: 99,441 / 중복: 0건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT customer_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT customer_id) AS dup_cnt
		,SUM(customer_id IS NULL OR customer_id = '') AS blank_cnt
  FROM  olist_dm.dim_customer;

-- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
SELECT  COUNT(*) AS mismatch_cnt
  FROM  olist_dm.dim_customer AS dc
  JOIN  olist_stg.stg_customers AS sc
    ON  sc.customer_id = dc.customer_id
 WHERE  NOT (sc.customer_unique_id <=> dc.customer_unique_id)
    OR  NOT (sc.customer_zip_code_prefix <=> dc.customer_zip_code_prefix)
    OR  NOT (sc.customer_city <=> dc.customer_city)
    OR  NOT (sc.customer_state <=> dc.customer_state)
    OR  NOT (sc.customer_city_state <=> dc.customer_city_state);

-- 조인 누락건수(dim_geolocation, 고객 주소 기준): 278건
SELECT  COUNT(*) AS orphan_cnt
  FROM  olist_dm.dim_customer AS dc
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
    ON  dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
 WHERE  dc.customer_zip_code_prefix IS NOT NULL
   AND  dc.customer_zip_code_prefix <> ''
   AND  dg.geolocation_zip_code_prefix IS NULL;

















