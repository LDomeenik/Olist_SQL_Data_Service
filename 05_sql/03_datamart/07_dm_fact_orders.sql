/******************************************************************************************************************************************************/


/*
 * File: dm_fact_orders.sql
 * Description:
 * 	- Operations Data Mart Fact 테이블(fact_orders) 생성 및 적재
 * 	- Grain: 1 row = 1 order_id
 * 
 * Note:
 * 	- 해당 fact_orders 테이블은 Operations Data Mart에서 Fact 테이블로 사용되는 테이블입니다.
 * 	- Source로 사용된 테이블과 컬럼은 다음과 같습니다.
 * 		- olist_stg.stg_orders (base 테이블)
 * 		- olist_stg.stg_customers (customer_zip_code_prefix)
 * 	- FK로 연결될 테이블과 컬럼은 다음과 같습니다.
 * 		- olist_dm.dim_date (order_purchase_date_key)
 * 		- olist_dm.dim_customer (customer_id)
 * 		- olist_dm.dim_geolocation (customer_zip_code_prefix)
 * 	- order_purchase_date_key의 경우 stg_orders의 order_purchase_date에 DATE_FORMAT을 적용하였습니다(YYYYMMDD 형태).
 * 	- 그 외의 컬럼들은 각 테이블에서 추가적인 집계/로직 없이 그대로 값을 반영하였습니다.
 */


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.fact_orders;


-- DDL(테이블 생성)
CREATE TABLE olist_dm.fact_orders (
	order_id						VARCHAR(50)		NOT NULL,
	customer_id						VARCHAR(50)		NOT NULL,
	order_purchase_date_key			INT				NOT NULL,
	customer_zip_code_prefix		CHAR(5)			NULL,
	order_status					VARCHAR(20)		NOT NULL,
	order_purchase_dt				DATETIME		NOT NULL,
	order_approved_dt				DATETIME		NULL,
	order_delivered_carrier_dt		DATETIME		NULL,
	order_delivered_customer_dt		DATETIME		NULL,
	order_estimated_delivery_dt		DATE			NULL,
	approve_lead_days				INT				NULL,
	delivery_lead_days				INT				NULL,
	delivery_delay_days				INT				NULL,
	is_delivered					TINYINT			NOT NULL,
	is_canceled						TINYINT			NOT NULL,
	
	-- PK 및 Indexes
	PRIMARY KEY (order_id),
	INDEX idx_dm_fact_orders_purchase_date_key (order_purchase_date_key),
	INDEX idx_dm_fact_orders_customer_id (customer_id),
	INDEX idx_dm_fact_orders_status (order_status),
	INDEX idx_dm_fact_orders_zip_prefix (customer_zip_code_prefix)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;



-- ===========================================================================================================================================


/*
 * ETL: fact_orders
 * 	- stg 레이어의 stg_orders를 복사
 * 	- stg 레이어의 customers와 조인하여 customer_zip_code_prefix를 복사 및 생성
*/

TRUNCATE TABLE olist_dm.fact_orders;

-- 데이터 적재
INSERT INTO olist_dm.fact_orders (
	order_id,
	customer_id,
	order_purchase_date_key,
	customer_zip_code_prefix,
	order_status,
	order_purchase_dt,
	order_approved_dt,
	order_delivered_carrier_dt,
	order_delivered_customer_dt,
	order_estimated_delivery_dt,
	approve_lead_days,
	delivery_lead_days,
	delivery_delay_days,
	is_delivered,
	is_canceled
)
SELECT  so.order_id
		,so.customer_id
		,CAST(DATE_FORMAT(so.order_purchase_dt, '%Y%m%d') AS UNSIGNED) AS order_purchase_date_key
		,sc.customer_zip_code_prefix
		,so.order_status
		,so.order_purchase_dt
		,so.order_approved_dt
		,so.order_delivered_carrier_dt
		,so.order_delivered_customer_dt
		,so.order_estimated_delivery_dt
		,so.approve_lead_days
		,so.delivery_lead_days
		,so.delivery_delay_days
		,so.is_delivered
		,so.is_canceled 
  FROM  olist_stg.stg_orders AS so
  LEFT
  JOIN  olist_stg.stg_customers AS sc
    ON  sc.customer_id = so.customer_id;


-- ===========================================================================================================================================


/*
 * QC: fact_orders
 * 	- row count: 99,441건 (stg_orders: 99,441)
 * 	- PK 유니크 -> cnt: 99,441 / distinct_cnt: 99,441 / 중복: 0건 / 결측 및 공백: 0건
 * 	- FK 컬럼의 결측치(customer_id/order_purchase_date_key/order_status/order_purchase_dt): 0건
 * 	- 조인 정합성: geolocation과의 조인 정합성은 orphan_cnt가 1 이상이어도 이상치라고 볼 수 없음(원본 데이터 커버리지의 한계) / 그 외의 orphan_cnt는 0이어야 함 (geolocation 관련 상세 조인 정합 비율은 dm_QC_all 스크립트에서 관리)
 * 		-> fact_orders에는 있지만 dim_customers에는 없는 고객: 0건
 * 		-> fact_orders에는 있지만 dim_date에는 없는 주문 날짜: 0건
 * 		-> fact_orders에는 있지만 dim_geolocation에는 없는 customer_zip_code_prefix: 278건
*/


-- 샘플
SELECT  *
  FROM  olist_dm.fact_orders
 LIMIT  50;

-- row count: 99,441건 (stg_orders: 99,441)
SELECT  COUNT(*) AS cnt
  FROM  olist_dm.fact_orders;

SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_orders;

-- PK 유니크 -> cnt: 99,441 / distinct_cnt: 99,441 / 중복: 0건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS row_cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_cnt
  FROM  olist_dm.fact_orders;

-- 필수 컬럼 결측치(customer_id/order_purchase_date_key/order_status/order_purchase_dt): 0건
SELECT  SUM(customer_id IS NULL) AS null_customer_cnt
		,SUM(order_purchase_date_key IS NULL) AS null_date_cnt
		,SUM(order_status IS NULL) AS null_status_cnt
		,SUM(order_purchase_dt IS NULL) AS null_purchase_dt_cnt
  FROM  olist_dm.fact_orders;

-- 조인 정합성(1) -> fact_orders에는 있지만 dim_customers에는 없는 고객: 0건
SELECT  COUNT(*) AS orphan_customer_cnt
  FROM  olist_dm.fact_orders AS fo
  LEFT
  JOIN  olist_dm.dim_customer AS dc
    ON  dc.customer_id = fo.customer_id
 WHERE  dc.customer_id IS NULL;

-- 조인 정합성(2) -> fact_orders에는 있지만 dim_date에는 없는 주문 날짜: 0건
SELECT  COUNT(*) AS orphan_date_cnt
  FROM  olist_dm.fact_orders AS fo
  LEFT
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
 WHERE  dd.date_key IS NULL;

-- 조인 정합성(3) -> fact_orders에는 있지만 dim_geolocation에는 없는 customer_zip_code_prefix: 278건
SELECT  COUNT(*) AS orphan_customer_zip_cnt
  FROM  olist_dm.fact_orders AS fo
  LEFT
  JOIN  olist_dm.dim_geolocation AS dg
    ON  dg.geolocation_zip_code_prefix = fo.customer_zip_code_prefix
 WHERE  fo.customer_zip_code_prefix IS NOT NULL 
   AND  dg.geolocation_zip_code_prefix IS NULL;

 







