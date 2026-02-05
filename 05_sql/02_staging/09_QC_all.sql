/****************************************************************************************************************************************/


/*
 * File: 09_QC_all.sql
 * Description:
 * 	- [01] STG row count snapshot
 * 	- [02] PK 결측 및 유일성 snapshot
 * 	- [03] 주요 컬럼 공백 및 결측 snapshot
 * 	- [04] 플래그 컬럼 snapshot
 * 	- [05] 도메인 / 값 범위 검증
 * 	- [06] 파생 컬럼 정합성 검증
 * 	- [07] 조인 정합성 검증
 * 
 * Note:
 * 	- 해당 통합 QC 스크립트는 각 주제별로 최소한의 내용만을 담았습니다. (데이터 통과를 위한 최소 조건)
 * 	- 따라서 상세한 QC를 확인하기 위해서는 각 테이블의 ETL 파일 QC(테이블 검증) 섹션을 참조해야 합니다.
 */


/****************************************************************************************************************************************/

USE olist_stg;

-- 스키마 존재 확인
SELECT  schema_name
  FROM  information_schema.SCHEMATA
 WHERE  schema_name IN ('olist_raw', 'olist_stg');


-- ======================================================================================================================================================================


/*
 * [01] STG row count snapshot: ETL 과정에서의 데이터 손실 또는 증폭 여부 조기 감지를 위한 Row Count 기반 품질 검증
 * 	- Stg 레이어와 Raw 레이어의 row count 차이 -> geolocation 테이블에서 diff가 -981148로 row count가 크게 차이 남
 * 	- geolocation 테이블의 특성 상 zip_code_prefix 하나 당 한 행으로 적재되어 있기 때문에 raw의 distinct zip_code_prefix 개수와 stg row count를 비교 -> diff: 0
 * 	- Stg 레이어 테이블의 row count 추정치
 *  - Stg 레이어 테이블의 정확한 row count
 * 
 * Note:
 * 	- 대부분의 테이블은 Raw와 Stg의 row count가 동일해야 정상입니다(diff = 0).
 * 	- geolocation은 zip_code_prefix 기준 1행으로 축약 적재되었으므로 raw 전체 row count와 diff가 크게 나는 것이 정상입니다.
 * 	- 해당 diff에 대해서는 아래 'raw distinct zip_prefix vs stg row count' 비교로 축약 규칙을 검증하였습니다(diff = 0).
*/

-- Raw VS Stg row count
-- entity			raw_cnt	 stg_cnt	diff
-- =================================================
-- orders			 99441	  99441	     0
-- customers		 99441	  99441		 0
-- order_items		 112650	  112650	 0
-- products			 32951	  32951	  	 0
-- sellers			 3095	  3095		 0
-- order_payments	 103886	  103886	 0
-- order_reviews	 99224	  99224		 0
-- geolocation		 1000163  19015		-981148
SELECT  'orders' AS entity
		,(SELECT COUNT(*) FROM olist_raw.orders) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_orders) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_orders) - (SELECT COUNT(*) FROM olist_raw.orders) AS diff

UNION ALL

SELECT  'customers' AS entity
		,(SELECT COUNT(*) FROM olist_raw.customers) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_customers) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_customers) - (SELECT COUNT(*) FROM olist_raw.customers) AS diff

UNION ALL

SELECT  'order_items' AS entity
		,(SELECT COUNT(*) FROM olist_raw.order_items) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_items) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_items) - (SELECT COUNT(*) FROM olist_raw.order_items) AS diff

UNION ALL

SELECT  'products' AS entity
		,(SELECT COUNT(*) FROM olist_raw.products) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_products) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_products) - (SELECT COUNT(*) FROM olist_raw.products) AS diff

UNION ALL

SELECT  'sellers' AS entity
		,(SELECT COUNT(*) FROM olist_raw.sellers) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_sellers) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_sellers) - (SELECT COUNT(*) FROM olist_raw.sellers) AS diff

UNION ALL

SELECT  'order_payments' AS entity
		,(SELECT COUNT(*) FROM olist_raw.order_payments) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_payments) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_payments) - (SELECT COUNT(*) FROM olist_raw.order_payments) AS diff

UNION ALL

SELECT  'order_reviews' AS entity
		,(SELECT COUNT(*) FROM olist_raw.order_reviews) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_reviews) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_reviews) - (SELECT COUNT(*) FROM olist_raw.order_reviews) AS diff

UNION ALL

SELECT  'geolocation' AS entity
		,(SELECT COUNT(*) FROM olist_raw.geolocation) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_geolocation) AS stg_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_geolocation) - (SELECT COUNT(*) FROM olist_raw.geolocation) AS diff;

-- geolocation row count (aggregation expected)
-- 	- raw: 1 row per (zip_prefix, lat, lng, city, state)
-- 	- stg: 1 row per zip_code_prefix (PK)

-- raw_distinct_zip_prefix  stg_cnt  diff
-- =========================================
--			 19015			 19015	   0
SELECT  (SELECT  COUNT(DISTINCT geolocation_zip_code_prefix) FROM olist_raw.geolocation) AS raw_distinct_zip_prefix
		,(SELECT  COUNT(*) FROM olist_stg.stg_geolocation) AS stg_cnt
		,(SELECT  COUNT(*) FROM olist_stg.stg_geolocation) - (SELECT COUNT(DISTINCT geolocation_zip_code_prefix) FROM olist_raw.geolocation) AS diff;

-- Staging 테이블 row count 일괄 조회
-- stg_orders			108072
-- stg_customers	    97041
-- stg_order_items	    111894
-- stg_products			34426
-- stg_sellers			2998
-- stg_order_payments	103851
-- stg_order_reviews	99377
-- stg_geolocation	    18826
SELECT  table_name
		,table_rows
  FROM  information_schema.tables
 WHERE  table_schema = 'olist_stg'
   AND  table_type = 'BASE TABLE'
 ORDER
    BY  table_name;

-- 정확한 row count
-- stg_orders			99441
-- stg_customers		99441
-- stg_order_items		112650
-- stg_products			32951
-- stg_sellers			3095
-- stg_order_payments	103886
-- stg_order_reviews	99224
-- stg_geolocation		19015
SELECT  'stg_orders' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_orders

UNION ALL

SELECT  'stg_customers' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_customers

UNION ALL

SELECT  'stg_order_items' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_order_items

UNION ALL

SELECT  'stg_products' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_products

UNION ALL

SELECT  'stg_sellers' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_sellers

UNION ALL

SELECT  'stg_order_payments' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_reviews' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_order_reviews

UNION ALL

SELECT  'stg_geolocation' AS table_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_geolocation;


-- ======================================================================================================================================================================


/*
 * [02] PK 확인 (Summary)
 * 	- Staging 테이블의 엔티티 식별 안정성 확보를 위해 각 테이블의 PK 컬럼과 PK 컬럼의 유일성을 통합 snapshot으로 출력
 * 	- Staging 테이블의 엔티티 식별 안정성 확보를 위해 각 테이블의 PK NOT NULL을 확인하는 snapshot을 출력
 * 
 * Note:
 * 	- 중복 row 수(dup_cnt)는 0건이어야 합니다.
 * 	- PK의 NULL 허용 건수는 0건이어야 합니다.
 * 	- 예외는 없습니다. 
 * 	- dup_cnt는 "중복 또는 PK NULL 존재" 시 0이 아닐 수 있으므로, PK NULL 검사는 별도 NOT NULL QC에서 확인하였습니다.
*/

-- PK를 확인하기 위한 DESCRIBE
-- DESCRIBE olist_stg.stg_;

-- 각 테이블의 PK 컬럼과 PK 유일성 확인
-- table_name			pk_cols							dup_cnt
-- ===============================================================
-- stg_orders			order_id							0
-- stg_customers		customer_id							0
-- stg_order_items		(order_id, order_item_id)			0
-- stg_products			product_id							0
-- stg_sellers			seller_id							0
-- stg_order_payments	(order_id, payment_sequential)		0
-- stg_order_reviews	(review_id, order_id)				0
-- stg_geolocation		geolocation_zip_code_prefix			0

SELECT  'stg_orders' AS table_name
		,'order_id' AS pk_cols
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
  FROM  olist_stg.stg_orders

UNION ALL

SELECT  'stg_customers'
		,'customer_id'
		,COUNT(*) - COUNT(DISTINCT customer_id) AS dup_cnt
  FROM  olist_stg.stg_customers
  
UNION ALL
 
SELECT  'stg_order_items'
		,'(order_id, order_item_id)'
		,COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '|', order_item_id)) AS dup_cnt
  FROM  olist_stg.stg_order_items

UNION ALL

SELECT  'stg_products'
		,'product_id'
		,COUNT(*) - COUNT(DISTINCT product_id) AS dup_cnt
  FROM  olist_stg.stg_products

UNION ALL

SELECT  'stg_sellers'
		,'seller_id'
		,COUNT(*) - COUNT(DISTINCT seller_id) AS dup_cnt
  FROM  olist_stg.stg_sellers

UNION ALL

SELECT  'stg_order_payments'
		,'(order_id, payment_sequential)'
		,COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '|', payment_sequential)) AS dup_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_reviews'
		,'(review_id, order_id)'
		,COUNT(*) - COUNT(DISTINCT CONCAT(review_id, '|', order_id)) AS dup_cnt
  FROM  olist_stg.stg_order_reviews

UNION ALL

SELECT  'stg_geolocation'
		,'geolocation_zip_code_prefix'
		,COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix) AS dup_cnt
  FROM  olist_stg.stg_geolocation;


-- 각 테이블의 PK 컬럼과 PK 컬럼에 존재하는 NULL 건수 확인
-- table_name			pk_cols							pk_null_cnt
-- =====================================================================
-- stg_orders			order_id							0
-- stg_customers		customer_id							0
-- stg_order_items		(order_id, order_item_id)			0
-- stg_products			product_id							0
-- stg_sellers			seller_id							0
-- stg_order_payments	(order_id, payment_sequential)		0
-- stg_order_reviews	(review_id, order_id)				0
-- stg_geolocation		geolocation_zip_code_prefix			0
SELECT  'stg_orders' AS table_name
		,'order_id' AS pk_cols
		,SUM(order_id IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_orders

UNION ALL

SELECT  'stg_customers'
		,'customer_id'
		,SUM(customer_id IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_customers

UNION ALL

SELECT  'stg_order_items'
		,'(order_id, order_item_id)'
		,SUM(order_id IS NULL OR order_item_id IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_order_items

UNION ALL

SELECT  'stg_products'
		,'product_id'
		,SUM(product_id IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_products

UNION ALL

SELECT  'stg_sellers'
		,'seller_id'
		,SUM(seller_id IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_sellers

UNION ALL

SELECT  'stg_order_payments'
		,'(order_id, payment_sequential)'
		,SUM(order_id IS NULL OR payment_sequential IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_reviews'
		,'(review_id, order_id)'
		,SUM(review_id IS NULL OR order_id IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_order_reviews

UNION ALL

SELECT  'stg_geolocation'
		,'geolocation_zip_code_prefix'
		,SUM(geolocation_zip_code_prefix IS NULL) AS pk_null_cnt
  FROM  olist_stg.stg_geolocation;


-- ======================================================================================================================================================================


/*
 * [03] Critical columns NOT NULL / BLANK check (Summary)
 * 	- PK 외의 조인/분석/KPI 산출에 필수인 핵심 컬럼들에 대해 NULL 또는 BLANK 여부를 확인하는 snapshot 출력
 * 
 * Note:
 * 	- critical_null_cnt는 0이어야 합니다.
 * 	- 예외적으로 비즈니스/데이터 특성상 NULL이 허용되는 컬럼은 제외합니다. (ex. delivered_*, date, flag 등)
*/


-- 각 테이블의 Critical column NOT NULL/BLANK 건수
-- table_name			critical_cols														critical_null_cnt
-- ================================================================================================================
-- stg_orders			customer_id, order_purchase_dt, order_status								0
-- stg_customers		customer_unique_id, customer_zip_code_prefix, customer_city_state			0
-- stg_order_items		product_id, seller_id, price, freight_value									0
-- stg_sellers			seller_zip_code_prefix, seller_city_state									0
-- stg_order_payments	payment_type, payment_value													0
-- stg_order_reviews	review_score																0

SELECT  'stg_orders' AS table_name
		,'customer_id, order_purchase_dt, order_status' AS critical_cols
		,SUM(customer_id IS NULL) + SUM(order_purchase_dt IS NULL) + SUM(order_status IS NULL OR TRIM(order_status) = '') AS critical_null_cnt
  FROM  olist_stg.stg_orders

UNION ALL

SELECT  'stg_customers'
		,'customer_unique_id, customer_zip_code_prefix, customer_city_state' AS critical_cols
		,SUM(customer_unique_id IS NULL) + SUM(customer_zip_code_prefix IS NULL) + SUM(customer_city_state IS NULL OR TRIM(customer_city_state) = '') AS critical_null_cnt
  FROM  olist_stg.stg_customers

UNION ALL

SELECT  'stg_order_items'
		,'product_id, seller_id, price, freight_value' AS critical_cols
		,SUM(product_id IS NULL) + SUM(seller_id IS NULL) + SUM(price IS NULL) + SUM(freight_value IS NULL) AS critical_null_cnt
  FROM  olist_stg.stg_order_items

UNION ALL

SELECT  'stg_sellers'
		,'seller_zip_code_prefix, seller_city_state' AS critical_cols
		,SUM(seller_zip_code_prefix IS NULL) + SUM(seller_city_state IS NULL OR TRIM(seller_city_state) = '') AS critical_null_cnt
  FROM  olist_stg.stg_sellers

UNION ALL

SELECT  'stg_order_payments'
		,'payment_type, payment_value' AS critical_cols
		,SUM(payment_type IS NULL OR TRIM(payment_type) = '') + SUM(payment_value IS NULL) AS critical_null_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_reviews'
		,'review_score' AS critical_cols
		,SUM(review_score IS NULL) AS critical_null_cnt
  FROM  olist_stg.stg_order_reviews;


-- ======================================================================================================================================================================


/*
 * [04] Flag columns snapshot (Summary)
 * 	- Staging 레이어에서 생성된 모든 플래그 컬럼(is_*)에 대해 통합 QC snapshot을 출력
 * 
 * Note:
 * 	- 플래그 컬럼은 "문제 존재 여부"를 표시하므로 값이 1인 항목이 반드시 오류는 아닙니다.
 * 	- 해당 summary는 NULL 여부 / 0/1 도메인 위반 여부 / 발생 비율을 모니터링하기 위함입니다.
 * 	- 플래그 컬럼은 모두 TINYINT로 이루어져 있다고 전제합니다.
*/

-- table_name				flag_col				total_cnt		null_cnt	invalid_value_cnt	true_rate_pct
-- ====================================================================================================================
-- stg_geolocation		is_invalid_latlng_exists	 19015				0				0				0.11
-- stg_geolocation		is_multi_state				 19015				0				0				0.04
-- stg_order_payments	is_installments_zero		 103886				0				0				0.00
-- stg_order_payments	is_payment_value_zero		 103886				0				0				0.01
-- stg_order_reviews	is_title_blank				 99224				0				0				88.34
-- stg_order_reviews	is_message_blank			 99224				0				0				58.71
-- stg_orders			is_delivered				 99441				0				0				97.02
-- stg_orders			is_canceled					 99441				0				0				1.24
-- stg_orders			is_time_inconsistent		 99441				0				0				0.17
-- stg_orders			is_status_inconsistent		 99441				0				0				0.01
-- stg_orders			is_carrier_dt_missing		 99441				0				0				0.32
-- stg_products			is_category_blank			 32951				0				0				1.85
-- stg_products			is_weight_zero				 32951				0				0				0.01


-- 세팅
SET SESSION group_concat_max_len = 1000000;
SET @schema_name = 'olist_stg';

-- is_* 플래그 컬럼 목록을 바탕으로 UNION ALL QC 쿼리 생성
SELECT  GROUP_CONCAT(qry SEPARATOR ' UNION ALL ')
  INTO  @flag_qc_sql
  FROM  (
		SELECT  CONCAT("SELECT '", table_name, "' AS table_name, '", column_name, "' AS flag_col, ",
					  "COUNT(*) AS total_cnt, ",
					  "SUM(", column_name, " IS NULL) AS null_cnt, ",
					  "SUM(", column_name, " IS NOT NULL AND ", column_name, " NOT IN (0,1)) AS invalid_value_cnt, ",
           			  "ROUND(AVG(CASE WHEN ", column_name, " = 1 THEN 1 ELSE 0 END) * 100, 2) AS true_rate_pct ",
            		  "FROM ", @schema_name, ".", table_name
					  ) AS qry
		  FROM  information_schema.COLUMNS
		 WHERE  table_schema = @schema_name
		   AND  column_name LIKE 'is\_%' ESCAPE '\\'
		 ORDER
		    BY  table_name
		    	,column_name
  		) AS t;

-- 쿼리 실행
PREPARE stmt FROM @flag_qc_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- ======================================================================================================================================================================


/*
 * [05] Domain / Range QC (Summary)
 * 	- 값이 존재하지만 비즈니스/데이터 규칙(도메인, 범위)을 위반하는 케이스 탐지
 * 
 * Note:
 * 	- invalid_cnt는 0이어야 합니다.
*/

-- 테이블과 적재 규칙/규칙 위반 건수

-- table_name					rule_name						invalid_cnt
-- ================================================================================
-- stg_order_payments	payment_type_domain							0
-- stg_order_payments	payment_sequential >= 1						0
-- stg_order_payments	payment_installments >= 0					0
-- stg_order_payments	payment_value >= 0							0
-- stg_order_reviews	review_score BETWEEN 1 AND 5				0
-- stg_order_items		price >= 0 AND freight_value >= 0			0
-- stg_geolocation		lat in [-35, 6]								0
-- stg_geolocation		lng in [-75, -30]							0

SELECT  'stg_order_payments' AS table_name
		,'payment_type_domain' AS rule_name
		,SUM(payment_type IS NOT NULL AND TRIM(payment_type) <> '' AND payment_type NOT IN ('credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined')) AS invalid_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_payments'
		,'payment_sequential >= 1' AS rule_name
		,SUM(payment_sequential IS NOT NULL AND payment_sequential < 1) AS invalid_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_payments'
		,'payment_installments >= 0' AS rule_name
		,SUM(payment_installments IS NOT NULL AND payment_installments < 0) AS invalid_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_payments'
		,'payment_value >= 0' AS rule_name
		,SUM(payment_value IS NOT NULL AND payment_value < 0) AS invalid_cnt
  FROM  olist_stg.stg_order_payments

UNION ALL

SELECT  'stg_order_reviews' AS table_name
		,'review_score BETWEEN 1 AND 5' AS rule_name
		,SUM(review_score IS NOT NULL AND (review_score < 1 OR review_score > 5)) AS invalid_cnt
  FROM  olist_stg.stg_order_reviews

UNION ALL

SELECT  'stg_order_items' AS table_name
		,'price >= 0 AND freight_value >= 0' AS rule_name
		,SUM((price IS NOT NULL AND price < 0) OR (freight_value IS NOT NULL AND freight_value < 0)) AS invalid_cnt
  FROM  olist_stg.stg_order_items

UNION ALL

SELECT  'stg_geolocation' AS table_name
		,'lat in [-35, 6] and lng in [-75, -30]' AS rule_name
		,SUM(geolocation_lat IS NOT NULL AND (geolocation_lat < -35 OR geolocation_lat > 6)) AS invalid_cnt
  FROM  olist_stg.stg_geolocation

UNION ALL

SELECT  'stg_geolocation' AS table_name
		,'lng in [-75, -30]' AS rule_name
		,SUM(geolocation_lng IS NOT NULL AND (geolocation_lng < -75 OR geolocation_lng > -30)) AS invalid_cnt
  FROM  olist_stg.stg_geolocation;


-- ======================================================================================================================================================================


/*
 * [06] Derived column consistency check (Summary)
 * 	- 파생 컬럼의 계산 정합성 검증 snapshot
 * 
 * Note:
 * 	- null_inconsistency_cnt는 0이어야 합니다.
 * 	- mismatch_cnt는 0이어야 합니다.
 * 	- DECIMAL/DOUBLE 연산 오차를 고려해 허용 오차를 두었습니다.
*/

-- item_total_value (price + freight_value)의 계산 정합성

-- null_inconsistency_cnt		mismatch_cnt
-- ===========================================
-- 				0					  0

SELECT  SUM(price IS NOT NULL AND freight_value IS NOT NULL AND item_total_value IS NULL) AS null_inconsistency_cnt
		,SUM(price IS NOT NULL AND freight_value IS NOT NULL AND item_total_value IS NOT NULL AND ABS(item_total_value - (price + freight_value)) > 0.000001) AS mismatch_cnt
  FROM  olist_stg.stg_order_items;


-- ======================================================================================================================================================================


/*
 * [07] 조인 정합성
 * 	- Staging 레이어에서 주요 엔티티 간 조인 정합성을 검증
 * 
 * Note:
 * 	- geolocation과의 조인이 아닌 조인의 orphan_cnt는 0이어야 합니다.
 * 	- 비즈니스 특성상 FK가 NULL 허용인 경우에는 "fk IS NOT NULL" 조건을 추가하여 검증하여야 합니다.
 * 	- stg.geolocation과의 조인은 orphan_cnt가 0보다 클 수 있습니다(stg.geolocation 테이블의 zip_code_prefix는 소스 커버리지 한계로 인해 일부 zip_prefix 매핑이 누락될 수 있음).
 * 	- 따라서 해당 orphan_cnt는 드릴다운하여 샘플의 플래그 컬럼과의 일관성을 확인해야 합니다.
 * 	- 해당 orphan은 fail 처리하지 않고 coverage 지표로 관리할 예정입니다.
*/

-- 각 테이블의 조인 정합성

-- table_name							rule_name							orphan_cnt
-- ==========================================================================================
-- stg_orders			orders.customer_id -> customers.customer_id				0
-- stg_order_items		order_items.order_id -> orders.order_id					0
-- stg_order_items		order_items.product_id -> products.product_id			0
-- stg_order_items		order_items.seller_id -> sellers.seller_id				0
-- stg_order_payments	order_payments.order_id -> orders.order_id				0
-- stg_order_reviews	order_reviews.order_id -> orders.order_id				0
-- stg_customers		customers.zip_prefix -> geolocation.zip_prefix		   278
-- stg_sellers			sellers.zip_prefix -> geolocation.zip_prefix			7


-- stg_orders.customer_id -> stg_customers.customer_id 조인
SELECT  'stg_orders' AS table_name
		,'orders.customer_id -> customers.customer_id' AS rule_name
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_stg.stg_customers AS c
    ON  c.customer_id = o.customer_id
 WHERE  o.customer_id IS NOT NULL
   AND  c.customer_id IS NULL

UNION ALL

-- stg_order_items.order_id -> stg_orders.order_id 조인
SELECT  'stg_order_items'
		,'order_items.order_id -> orders.order_id'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = oi.order_id
 WHERE  oi.order_id IS NOT NULL
   AND  o.order_id IS NULL

UNION ALL

-- stg_order_items.product_id -> stg_products.product_id 조인
SELECT  'stg_order_items'
		,'order_items.product_id -> products.product_id'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_stg.stg_products AS p
    ON  p.product_id = oi.product_id
 WHERE  oi.product_id IS NOT NULL
   AND  p.product_id IS NULL

UNION ALL

-- stg_order_items.seller_id -> stg_sellers.seller_id 조인
SELECT  'stg_order_items'
		,'order_items.seller_id -> sellers.seller_id'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_stg.stg_sellers AS s
    ON  s.seller_id = oi.seller_id
 WHERE  oi.seller_id IS NOT NULL
   AND  s.seller_id IS NULL

UNION ALL

-- stg_order_payments.order_id -> stg_orders.order_id 조인
SELECT  'stg_order_payments'
		,'order_payments.order_id -> orders.order_id'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_order_payments AS op
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = op.order_id
 WHERE  op.order_id IS NOT NULL
   AND  o.order_id IS NULL

UNION ALL

-- stg_order_reviews.order_id -> stg_orders.order_id 조인
SELECT  'stg_order_reviews'
		,'order_reviews.order_id -> orders.order_id'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_order_reviews AS r
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = r.order_id
 WHERE  r.order_id IS NOT NULL
   AND  o.order_id IS NULL

UNION ALL

-- stg_customers.customer_zip_code_prefix -> stg_geolocation.geolocation_zip_code_prefix 조인
SELECT  'stg_customers'
		,'customers.zip_prefix -> geolocation.zip_prefix'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_customers AS c
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
 WHERE  c.customer_zip_code_prefix IS NOT NULL
   AND  g.geolocation_zip_code_prefix IS NULL

UNION ALL

-- stg_sellers.seller_zip_code_prefix -> stg_geolocation.geolocation_zip_code_prefix 조인
SELECT  'stg_sellers'
		,'sellers.zip_prefix -> geolocation.zip_prefix'
		,COUNT(*) AS orphan_cnt
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
 WHERE  s.seller_zip_code_prefix IS NOT NULL
   AND  g.geolocation_zip_code_prefix IS NULL;


/*
 * stg.geolocation과의 조인시 발생하는 orphan_cnt 드릴다운 QC
 * 	- zip_code_prefix 단위로 몇개씩 orphan_cnt가 발생했는지 확인
 * 	- orphan_cnt가 발생한 zip_code_prefix가 olist_raw.geolocation에는 있는지 확인
 * 
 * 결과:
 * 	- geolocation과의 join 과정에서 발생한 orphan_cnt는 해당 zip_code_prefix가 raw.geolocation에도 존재하지 않은 것으로 확인됨
 * 	- 따라서 원본 소스의 커버리지 한계로 판단됨
 * 	- 이를 해결하기 위해 해당 orphan_cnt를 fail로 처리하지 않고 DM 레이어에서 coverage 지표로 관리하며 분석 시 LEFT JOIN을 사용할 예정
 * 	- 아래 snapshot은 coverage 지표를 확인한 내용
*/

-- customers와 geolocation 조인시 발생하는 zip_code_prefix 단위의 orphan_cnt
SELECT  c.customer_zip_code_prefix AS zip_prefix
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_customers AS c
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
 WHERE  c.customer_zip_code_prefix IS NOT NULL
   AND  g.geolocation_zip_code_prefix IS NULL
 GROUP
    BY  1
 ORDER
    BY  cnt DESC
 LIMIT  30;

-- sellers와 geolocation 조인시 발생하는 zip_code_prefix 단위의 orphan_cnt
SELECT  s.seller_zip_code_prefix AS zip_prefix
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
 WHERE  s.seller_zip_code_prefix IS NOT NULL
   AND  g.geolocation_zip_code_prefix IS NULL
 GROUP
    BY  1
 ORDER
    BY  cnt DESC
 LIMIT  30;

-- customers에서 orphan_cnt가 발생한 zip_code_prefix가 olist_raw.geolocation에는 있는지 확인 -> 없음
SELECT  c.customer_zip_code_prefix AS zip_prefix,
        COUNT(*) AS cust_cnt,
        CASE WHEN r.geolocation_zip_code_prefix IS NULL THEN 0 ELSE 1 END AS exists_in_raw_geo
  FROM  olist_stg.stg_customers AS c
  LEFT  JOIN olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
  LEFT  
  JOIN  (
        SELECT geolocation_zip_code_prefix
          FROM olist_raw.geolocation
         GROUP BY 1
  		) AS r
    ON  r.geolocation_zip_code_prefix = c.customer_zip_code_prefix
 WHERE  c.customer_zip_code_prefix IS NOT NULL
   AND  g.geolocation_zip_code_prefix IS NULL
 GROUP 
    BY  1, 3
 ORDER 
    BY  cust_cnt DESC
 LIMIT  50;

-- sellers에서 orphan_cnt가 발생한 zip_code_prefix가 olist_raw.geolocation에는 있는지 확인 -> 없음
SELECT  s.seller_zip_code_prefix AS zip_prefix,
        COUNT(*) AS seller_cnt,
        CASE WHEN r.geolocation_zip_code_prefix IS NULL THEN 0 ELSE 1 END AS exists_in_raw_geo
  FROM  olist_stg.stg_sellers AS s
  LEFT  
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
  LEFT  
  JOIN  (
        SELECT geolocation_zip_code_prefix
          FROM olist_raw.geolocation
         GROUP BY 1
  		) AS r
    ON  r.geolocation_zip_code_prefix = s.seller_zip_code_prefix
 WHERE  s.seller_zip_code_prefix IS NOT NULL
   AND  g.geolocation_zip_code_prefix IS NULL
 GROUP 
    BY  1, 3
 ORDER 
    BY  seller_cnt DESC
 LIMIT  50;


/*
 * Geo coverage snapshot
 *  - geolocation은 소스 데이터 커버리지 한계로 인해 일부 zip_prefix가 매핑되지 않을 수 있음
 *  - 따라서 orphan_cnt를 오류(fail)로 판단하지 않고, coverage 지표로 모니터링
 */

-- customers geo coverage (%)

-- table_name		  metric_name		metric_value
-- ===================================================
-- stg_customers	geo_coverage_pct	   99.72

SELECT  'stg_customers' AS table_name
        ,'geo_coverage_pct' AS metric_name
        ,ROUND(
            100 * (1 - (SUM(g.geolocation_zip_code_prefix IS NULL) / COUNT(*)))
          ,2
        ) AS metric_value
  FROM  olist_stg.stg_customers AS c
  LEFT  
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
 WHERE  c.customer_zip_code_prefix IS NOT NULL;

-- sellers geo coverage (%)

-- table_name		  metric_name		metric_value
-- ===================================================
-- stg_sellers		geo_coverage_pct		99.77

SELECT  'stg_sellers' AS table_name
        ,'geo_coverage_pct' AS metric_name
        ,ROUND(
            100 * (1 - (SUM(g.geolocation_zip_code_prefix IS NULL) / COUNT(*)))
          ,2
        ) AS metric_value
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
 WHERE  s.seller_zip_code_prefix IS NOT NULL;



