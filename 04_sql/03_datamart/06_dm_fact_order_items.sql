/******************************************************************************************************************************************************/


/*
 * File: dm_fact_order_items.sql
 * Description:
 * 	- Sales Mart Fact 테이블(fact_order_items) 생성 및 적재
 * 	- Grain: 1 row = 1 order item in 1 order
 * 
 * Note:
 * 	- 해당 fact_order_items 테이블은 Sales Mart에서 Fact 테이블로 사용되는 테이블입니다.
 * 	- Source로 사용된 테이블과 컬럼은 다음과 같습니다.
 * 		- olist_stg.stg_order_items (base 테이블)
 * 		- olist_stg.stg_orders (customer_id, order_purchase_date)
 * 		- olist_stg.stg_customers (customer_zip_code_prefix)
 * 		- olist_stg.stg_sellers (seller_zip_code_prefix)
 * 	- FK로 연결될 테이블과 컬럼은 다음과 같습니다.
 * 		- olist_dm.dim_date (order_purchase_date_key)
 * 		- olist_dm.dim_customer (customer_id)
 * 		- olist_dm.dim_product (product_id)
 * 		- olist_dm.dim_seller (seller_id)
 * 		- olist_dm.dim_geolocation (customer_zip_code_prefix, seller_zip_code_prefix)
 * 	- order_purchase_date_key의 경우 stg_orders의 order_purchase_date에 DATE_FORMAT을 적용하였습니다(YYYYMMDD 형태).
 * 	- 그 외의 컬럼들은 각 테이블에서 추가적인 집계/로직 없이 그대로 값을 반영하였습니다.
 */


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.fact_order_items;


-- DDL(테이블 생성)
CREATE TABLE olist_dm.fact_order_items (
	order_id					VARCHAR(50)			NOT NULL,
	order_item_id				VARCHAR(50)			NOT NULL,
	order_item_seq				INT					NULL,
	customer_id					VARCHAR(50)			NOT NULL,
	product_id					VARCHAR(50)			NOT NULL,
	seller_id					VARCHAR(50)			NOT NULL,
	order_purchase_date_key		INT					NOT NULL,
	customer_zip_code_prefix	CHAR(5)				NULL,
	seller_zip_code_prefix		CHAR(5)				NULL,
	price						DECIMAL(10, 2)		NULL,
	freight_value				DECIMAL(10, 2)		NULL,
	item_total_value			DECIMAL(10, 2)		NULL,
	
	-- PK 및 INDEX
	PRIMARY KEY (order_id, order_item_id),
	INDEX idx_dm_fact_order_items_date_key (order_purchase_date_key),
	INDEX idx_dm_fact_order_items_product (product_id),
	INDEX idx_dm_fact_order_items_seller (seller_id),
	INDEX idx_dm_fact_order_items_customer (customer_id),
	INDEX idx_dm_fact_order_items_customer_zip_prefix (customer_zip_code_prefix),
	INDEX idx_dm_fact_order_items_seller_zip_prefix (seller_zip_code_prefix)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;



-- ===========================================================================================================================================


/*
 * ETL: fact_order_items
 * 	- stg 레이어의 stg_order_items를 복사
 * 	- stg 레이어의 orders와 조인하여 customer_id, order_purchase_date_key를 복사 및 생성
 * 	- stg 레이어의 stg_customers와 조인하여 customer_zip_code_prefix를 복사
 * 	- stg 레이어의 stg_sellers와 조인하여 seller_zip_code_prefix를 복사
*/

TRUNCATE TABLE olist_dm.fact_order_items;

-- 데이터 적재
INSERT INTO olist_dm.fact_order_items (
	order_id,
	order_item_id,
	order_item_seq,
	customer_id,
	product_id,
	seller_id,
	order_purchase_date_key,
	customer_zip_code_prefix,
	seller_zip_code_prefix,
	price,
	freight_value,
	item_total_value
)
SELECT  soi.order_id
		,soi.order_item_id
		,soi.order_item_seq
		,o.customer_id
		,soi.product_id
		,soi.seller_id
		,DATE_FORMAT(o.order_purchase_date, '%Y%m%d') AS order_purchase_date_key
		,c.customer_zip_code_prefix
		,s.seller_zip_code_prefix
		,soi.price
		,soi.freight_value
		,soi.item_total_value
  FROM  olist_stg.stg_order_items AS soi
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = soi.order_id
  LEFT
  JOIN  olist_stg.stg_customers AS c
    ON  c.customer_id = o.customer_id
  LEFT
  JOIN  olist_stg.stg_sellers AS s
    ON  s.seller_id = soi.seller_id;


-- ===========================================================================================================================================


/*
 * QC: fact_order_items
 * 	- row count: 112,650건 (stg_sellers: 112,650)
 * 	- PK 유니크 -> cnt: 112,650 / distinct_cnt: 112,650 / 중복: 0건 / 결측 및 공백: 0건
 * 	- FK 컬럼의 결측치(order_purchase_date_key/customer_id/product_id/seller_id/customer_zip_code_prefix/seller_zip_code_prefix): 0건
 * 	- 조인 정합성: geolocation과의 조인 정합성은 orphan_cnt가 1 이상이어도 이상치라고 볼 수 없음(원본 데이터 커버리지의 한계) / 그 외의 orphan_cnt는 0이어야 함 (geolocation 관련 상세 조인 정합 비율은 dm_QC_all 스크립트에서 관리)
 * 		-> fact_order_items에는 있지만 stg_orders에는 없는 주문 건수: 0건
 * 		-> fact_order_items에는 있지만 dim_customer에는 없는 고객: 0건
 * 		-> fact_order_items에는 있지만 dim_product에는 없는 상품: 0건
 * 		-> fact_order_items에는 있지만 dim_seller에는 없는 판매자: 0건
 * 		-> fact_order_items에는 있지만 dim_date에는 없는 주문 날짜: 0건
 * 		-> fact_order_items에는 있지만 dim_geolocation에는 없는 customer_zip_code_prefix: 302건
 * 		-> fact_order_items에는 있지만 dim_geolocation에는 없는 seller_zip_code_prefix: 253건
*/


-- 샘플
SELECT  *
  FROM  olist_dm.fact_order_items
 LIMIT  50;

-- row count: 112,650건 (stg_order_items: 112,650)
SELECT  COUNT(*) AS dm_row_cnt 
  FROM olist_dm.fact_order_items;

SELECT  COUNT(*) AS stg_row_cnt
  FROM  olist_stg.stg_order_items;

-- PK 유니크 -> cnt: 112,650 / distinct_cnt: 112,650 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT CONCAT(order_id, '_', order_item_id)) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '_', order_item_id)) AS dup_cnt
		,SUM(CONCAT(order_id, '_', order_item_id) IS NULL OR CONCAT(order_id, '_', order_item_id) = '') AS blank_cnt
  FROM  olist_dm.fact_order_items;

-- FK 컬럼 결측: 결측 없음
SELECT  SUM(order_purchase_date_key IS NULL) AS null_date_key
		,SUM(customer_id IS NULL) AS null_customer_id
		,SUM(product_id IS NULL) AS null_product_id
		,SUM(seller_id IS NULL) AS null_seller_id
		,SUM(customer_zip_code_prefix IS NULL) AS null_customer_zip
		,SUM(seller_zip_code_prefix IS NULL) AS null_seller_zip
  FROM  olist_dm.fact_order_items;

-- 조인 정합성(1) -> fact_order_items에는 있지만 stg_orders에는 없는 주문 건수: 0건
SELECT  COUNT(*) AS orphan_order_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = foi.order_id
 WHERE  o.order_id IS NULL;

-- 조인 정합성(2) -> fact_order_items에는 있지만 dim_customer에는 없는 고객: 0건
SELECT  COUNT(*) AS orphan_customer_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_dm.dim_customer AS dc
    ON  dc.customer_id = foi.customer_id
 WHERE  dc.customer_id IS NULL;

-- 조인 정합성(3) -> fact_order_items에는 있지만 dim_product에는 없는 상품: 0건
SELECT  COUNT(*) AS orphan_product_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_dm.dim_product AS dp
    ON  dp.product_id = foi.product_id
 WHERE  dp.product_id IS NULL;

-- 조인 정합성(4) -> fact_order_items에는 있지만 dim_seller에는 없는 판매자: 0건
SELECT  COUNT(*) AS orphan_seller_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_dm.dim_seller AS ds
    ON  ds.seller_id = foi.seller_id
 WHERE  ds.seller_id IS NULL;

-- 조인 정합성(5) -> fact_order_items에는 있지만 dim_date에는 없는 주문 날짜: 0건
SELECT  COUNT(*) AS orphan_date_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = foi.order_purchase_date_key
 WHERE  dd.date_key IS NULL;

-- 조인 정합성(6) -> fact_order_items에는 있지만 dim_geolocation에는 없는 customer_zip_code_prefix: 302건
SELECT  COUNT(*) AS orphan_customer_zip_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
    ON  dg.geolocation_zip_code_prefix = foi.customer_zip_code_prefix
 WHERE  foi.customer_zip_code_prefix IS NOT NULL
   AND  dg.geolocation_zip_code_prefix IS NULL;

-- 조인 정합성(7) -> fact_order_items에는 있지만 dim_geolocation에는 없는 seller_zip_code_prefix: 253건
SELECT  COUNT(*) AS orphan_seller_zip_cnt
  FROM  olist_dm.fact_order_items AS foi
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
    ON  dg.geolocation_zip_code_prefix = foi.seller_zip_code_prefix
 WHERE  foi.seller_zip_code_prefix IS NOT NULL
   AND  dg.geolocation_zip_code_prefix IS NULL;



