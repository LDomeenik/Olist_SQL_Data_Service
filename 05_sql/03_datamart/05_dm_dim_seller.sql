/******************************************************************************************************************************************************/


/*
 * File: dm_dim_seller.sql
 * Description:
 * 	- Sales Data Mart 판매자 정보 차원 생성 (source: olist_stg.stg_sellers)
 * 	- Grain: 1 row = 1 seller_id
 * 
 * Note:
 * 	- 해당 dim_seller 테이블은 Sales Mart에서 dimension 테이블로 사용되는 차원 테이블입니다.
 * 	- stg_sellers의 집계/로직을 그대로 가져왔습니다. (추가적인 집계/로직 없음)
 * 	- 따라서 DQ, QC의 경우. 이미 품질 검사가 완료된 데이터이기 때문에 기본적인 적재 정합성만을 확인합니다.
*/


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.dim_seller;

-- DDL(테이블 생성)
CREATE TABLE olist_dm.dim_seller (
	seller_id				VARCHAR(50) 	NOT NULL,
	seller_zip_code_prefix	CHAR(5)			NULL,
	seller_city				VARCHAR(100) 	NULL,
	seller_state			CHAR(2)			NULL,
	seller_city_state		VARCHAR(200)	NULL,
	
	-- PK 및 INDEX
	PRIMARY KEY (seller_id),
	INDEX idx_dm_dim_seller_zip_prefix (seller_zip_code_prefix),
	INDEX idx_dm_dim_seller_state (seller_state),
	INDEX idx_dm_dim_seller_city_state (seller_city_state)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;



-- ===========================================================================================================================================


/*
 * ETL: dim_seller
 * 	- stg 레이어의 stg_sellers를 복사
*/


TRUNCATE TABLE olist_dm.dim_seller;

-- 데이터 적재
INSERT INTO olist_dm.dim_seller (
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state,
	seller_city_state
)
SELECT  seller_id
		,seller_zip_code_prefix
		,seller_city
		,seller_state
		,seller_city_state
  FROM  olist_stg.stg_sellers;


-- ===========================================================================================================================================


/*
 * QC: dim_seller
 * 	- row count: 3,095건 (stg_sellers: 3,095)
 * 	- PK 유니크 -> cnt: 3,095 / distinct_cnt: 3,095 / 중복: 0건 / 결측 및 공백: 0건
 * 	- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
 * 	- 조인 정합성 -> sellers에는 있지만 geolocation에는 없는 zip_code_prefix: 7건
*/


-- 샘플
SELECT  *
  FROM  olist_dm.dim_seller
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_dm.dim_seller;

-- row count: 3,095행 (stg_sellers: 3,095행)
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.dim_seller;

SELECT  COUNT(*) AS stg_cnt
  FROM  olist_stg.stg_sellers;

-- PK 유니크 확인 -> cnt: 3,095 / distinct_cnt: 3,095 / 중복: 0건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT seller_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT seller_id) AS dup_cnt
		,SUM(seller_id IS NULL OR seller_id = '') AS blank_cnt
  FROM  olist_dm.dim_seller;

-- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
SELECT  COUNT(*) AS mismatch_cnt
  FROM  olist_dm.dim_seller AS ds
  JOIN  olist_stg.stg_sellers AS ss
    ON  ss.seller_id = ds.seller_id
 WHERE  NOT (ss.seller_zip_code_prefix <=> ds.seller_zip_code_prefix)
    OR  NOT (ss.seller_city <=> ds.seller_city)
    OR  NOT (ss.seller_state <=> ds.seller_state);

-- 조인 누락건수(dim_geolocation, 판매자 주소 기준): 7건
SELECT  COUNT(*) AS orphan_cnt
  FROM  olist_dm.dim_seller AS ds
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
    ON  dg.geolocation_zip_code_prefix = ds.seller_zip_code_prefix
 WHERE  ds.seller_zip_code_prefix IS NOT NULL
   AND  ds.seller_zip_code_prefix <> ''
   AND  dg.geolocation_zip_code_prefix IS NULL;

















