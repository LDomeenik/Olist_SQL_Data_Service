/******************************************************************************************************************************************************/


/*
 * File: dm_dim_product.sql
 * Description:
 * 	- Sales Data Mart 제품 정보 차원 생성 (source: olist_stg.stg_products)
 * 	- Grain: 1 row = 1 product_id
 * 
 * Note:
 * 	- 해당 dim_product 테이블은 Sales Mart에서 dimension 테이블로 사용되는 차원 테이블입니다.
 * 	- stg_products의 집계/로직을 그대로 가져왔습니다. (추가적인 집계/로직 없음)
 * 	- 따라서 DQ, QC의 경우. 이미 품질 검사가 완료된 데이터이기 때문에 기본적인 적재 정합성만을 확인합니다.
*/


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.dim_product;

-- DDL(테이블 생성)
CREATE TABLE olist_dm.dim_product (
	product_id					VARCHAR(50)		NOT NULL,
	product_category_name		VARCHAR(100)	NULL,
	product_category_name_en	VARCHAR(100)	NULL,
	product_name_length			INT				NULL,
	product_description_length	INT				NULL,
	product_photos_qty			INT				NULL,
	product_weight_g			INT				NULL,
	product_length_cm			INT				NULL,
	product_height_cm			INT				NULL,
	product_width_cm			INT				NULL,
	product_volume_cm3			BIGINT			NULL,
	is_category_blank			TINYINT			NOT NULL,
	is_category_en_unmapped		TINYINT			NOT NULL,
	is_weight_zero				TINYINT			NOT NULL,
	
	-- PK 및 Indexes
	PRIMARY KEY (product_id),
	INDEX idx_dm_dim_product_category_name (product_category_name),
	INDEX idx_dm_dim_product_category_name_en (product_category_name_en)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;



-- ===========================================================================================================================================


/*
 * ETL: dim_product
 * 	- stg 레이어의 stg_products를 복사
*/


TRUNCATE TABLE olist_dm.dim_product;

-- 데이터 적재
INSERT INTO olist_dm.dim_product (
	product_id,
	product_category_name,
	product_category_name_en,
	product_name_length,
	product_description_length,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm,
	product_volume_cm3,
	is_category_blank,
	is_category_en_unmapped,
	is_weight_zero
)
SELECT  product_id
		,product_category_name
		,product_category_name_en
		,product_name_length
		,product_description_length
		,product_photos_qty
		,product_weight_g
		,product_length_cm
		,product_height_cm
		,product_width_cm
		,product_volume_cm3
		,is_category_blank
		,is_category_en_unmapped
		,is_weight_zero
  FROM  olist_stg.stg_products;


-- ===========================================================================================================================================


/*
 * QC: dim_product
 * 	- row count: 32,951건 (stg_product: 32,951)
 * 	- PK 유니크 -> cnt: 32,951 / distinct_cnt: 32,951 / 중복: 0건 / 결측 및 공백: 0건
 * 	- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
 * 	- 플래그 일관성 -> 플래그와 실제 값이 다른 건수: 0건
 * 	- 조인 정합성
 * 		-> order_items에 존재하는 product_id가 dim_product에 없는 경우: 0건
 * 		-> order_items와 조인했을 때 row count: 두 row count 모두 112,650으로 변화 없음 (이상 없음)
*/


-- 샘플
SELECT  *
  FROM  olist_dm.dim_product
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_dm.dim_product;

-- row count: 32,951행 (stg_products: 32,951행)
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.dim_product;

SELECT  COUNT(*) AS stg_cnt
  FROM  olist_stg.stg_products;

-- PK 유니크 확인 -> cnt: 32,951 / distinct_cnt: 32,951 / 중복: 0건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT product_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT product_id) AS dup_cnt
		,SUM(product_id IS NULL OR product_id = '') AS blank_cnt
  FROM  olist_dm.dim_product;

-- stg와 dm의 주요 컬럼 값 불일치 건수: 0건
SELECT  COUNT(*) AS mismatch_cnt
  FROM  olist_dm.dim_product AS dp
  JOIN  olist_stg.stg_products AS sp
    ON  sp.product_id = dp.product_id
 WHERE  NOT (sp.product_category_name <=> dp.product_category_name)
    OR  NOT (sp.product_category_name_en <=> dp.product_category_name_en)
    OR  NOT (sp.product_name_length <=> dp.product_name_length)
    OR  NOT (sp.product_description_length <=> dp.product_description_length)
    OR  NOT (sp.product_photos_qty <=> dp.product_photos_qty)
    OR  NOT (sp.product_weight_g <=> dp.product_weight_g)
    OR  NOT (sp.product_length_cm <=> dp.product_length_cm)
    OR  NOT (sp.product_height_cm <=> dp.product_height_cm)
    OR  NOT (sp.product_width_cm <=> dp.product_width_cm);

-- 플래그 일관성 -> 플래그와 실제 값이 다른 건수: 0건
SELECT  COUNT(*) AS cnt
  FROM  olist_dm.dim_product
 WHERE  (is_category_blank = 1 AND product_category_name IS NOT NULL)
    OR  (is_category_en_unmapped = 1 AND NOT (product_category_name IS NOT NULL AND product_category_name_en IS NULL))
    OR  (is_weight_zero = 1 AND product_weight_g >= 1);

-- 조인 정합성(1) -> order_items에 존재하는 product_id가 dim_product에 없는 경우: 0건
SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_dm.dim_product AS dp
    ON  dp.product_id = oi.product_id
 WHERE  dp.product_id IS NULL;

-- 조인 정합성(2) -> order_items와 조인했을 때 row count: 두 row count 모두 112,650으로 변화 없음(이상 없음)
SELECT  (
		SELECT  COUNT(*)
		  FROM  olist_stg.stg_order_items
		) AS oi_cnt
		,(
		SELECT  COUNT(*)
		  FROM  olist_stg.stg_order_items AS oi
		  JOIN  olist_dm.dim_product AS dp
		    ON  dp.product_id = oi.product_id
		) AS joined_cnt;

















