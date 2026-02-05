/****************************************************************************************************************************************/


/*
 * File: 01_init_environment.sql
 * Description:
 * 	- 데이터베이스 레이어 생성 (raw / stg / dm)
 * 	- raw 레이어 테이블을 정의 (olist의 원본 데이터를 바탕으로 9개의 테이블 생성)
 * 	- raw 레이어 테이블에 원본 데이터 적재
 * Notes:
 * 	- 기본적인 레이어(스키마)를 구성하기 위한 스크립트로 모든 스크립트 중 가장 먼저 실행되어야 합니다.
 * 	- raw 스키마에 대한 적재이기 떄문에 데이터 가공이나 전처리는 진행되지 않은 상태입니다.
 */


/****************************************************************************************************************************************/


/*
 * 데이터베이스 생성: raw / staging / datamart 세 개의 레이어로 구성
 */


-- raw schema 생성
CREATE DATABASE IF NOT EXISTS olist_raw
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_0900_ai_ci;

-- stg shcema 생성
CREATE DATABASE IF NOT EXISTS olist_stg
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_0900_ai_ci;


-- dm schema 생성
CREATE DATABASE IF NOT EXISTS olist_dm
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_0900_ai_ci;


/*
 * 테이블 생성: raw 레이어에 테이블 생성
 */

USE olist_raw;

-- olist_raw에 Raw 테이블 생성

DROP TABLE IF EXISTS orders;
CREATE TABLE IF NOT EXISTS orders (
	order_id                       VARCHAR(50),
	customer_id                    VARCHAR(50),
	order_status                   VARCHAR(20),
	order_purchase_timestamp       VARCHAR(30),
	order_approved_at              VARCHAR(30),
	order_delivered_carrier_date   VARCHAR(30),
	order_delivered_customer_date  VARCHAR(30),
	order_estimated_delivery_date  VARCHAR(30)
);

DROP TABLE IF EXISTS order_items;
CREATE TABLE IF NOT EXISTS order_items (
	order_id			 VARCHAR(50),
	order_item_id		 VARCHAR(50),
	product_id			 VARCHAR(50),
	seller_id			 VARCHAR(50),
	shipping_limit_date  VARCHAR(30),
	price                DECIMAL(10,2),
	freight_value		 DECIMAL(10,2)
);

DROP TABLE IF EXISTS order_payments;
CREATE TABLE IF NOT EXISTS order_payments (
	order_id			   VARCHAR(50),
	payment_sequential	   INT,
	payment_type		   VARCHAR(30),
	payment_installments   INT,
	payment_value		   DECIMAL(10,2)
);

DROP TABLE IF EXISTS order_reviews;
CREATE TABLE IF NOT EXISTS order_reviews (
	review_id				 VARCHAR(50),
	order_id				 VARCHAR(50),
	review_score			 INT,
	review_comment_title	 TEXT,
	review_comment_message	 TEXT,
	review_creation_date	 VARCHAR(30),
	review_answer_timestamp  VARCHAR(30)
);

DROP TABLE IF EXISTS customers;
CREATE TABLE IF NOT EXISTS customers (
	customer_id				  VARCHAR(50),
	customer_unique_id		  VARCHAR(50),
	customer_zip_code_prefix  VARCHAR(20),
	customer_city			  VARCHAR(100),
	customer_state			  VARCHAR(10)
);

DROP TABLE IF EXISTS products;
CREATE TABLE IF NOT EXISTS products (
	product_id					VARCHAR(50),
	product_category_name		VARCHAR(100),
	product_name_length			INT,
	product_description_length  INT,
	product_photos_qty			INT,
	product_weight_g			INT,
	product_length_cm			INT,
	product_height_cm			INT,
	product_width_cm			INT
);

DROP TABLE IF EXISTS sellers;
CREATE TABLE IF NOT EXISTS sellers (
	seller_id				VARCHAR(50),
	seller_zip_code_prefix  VARCHAR(20),
	seller_city				VARCHAR(100),
	seller_state			VARCHAR(10)
);

DROP TABLE IF EXISTS geolocation;
CREATE TABLE IF NOT EXISTS geolocation (
	geolocation_zip_code_prefix  VARCHAR(20),
	geolocation_lat				 DECIMAL(10, 6),
	geolocation_lng				 DECIMAL(10,6),
	geolocation_city			 VARCHAR(100),
	geolocation_state			 VARCHAR(10)
);

DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE IF NOT EXISTS product_category_name_translation (
	product_category_name		   VARCHAR(100),
	product_category_name_english  VARCHAR(100)
);


/*
 * 테이블에 데이터 적재: raw 레이어의 각 테이블에 데이터 적재
*/


-- orders (99,441행)
TRUNCATE TABLE olist_raw.orders;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_orders_dataset.csv'
INTO TABLE olist_raw.orders
CHARACTER SET utf8mb4
FIELDS 
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES 
	TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT 'orders' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.orders;

SELECT  *
  FROM  olist_raw.orders
 LIMIT  5;


-- order_items (112,650행)
TRUNCATE TABLE olist_raw.order_items;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_order_items_dataset.csv'
INTO TABLE olist_raw.order_items
CHARACTER SET utf8mb4
FIELDS 
	TERMINATED BY ',' 
	ENCLOSED BY '"'
LINES 
	TERMINATED BY '\n'
IGNORE 1 ROWS
(
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	@price,
	@freight_value
)
SET
	price = NULLIF(@price, ''),
	freight_value = NULLIF(@freight_value, '');


SELECT  'order_items' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.order_items;

SELECT  *
  FROM  olist_raw.order_items
 LIMIT  5;


-- order_payments (103,886행)
TRUNCATE TABLE olist_raw.order_payments;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_order_payments_dataset.csv'
INTO TABLE olist_raw.order_payments
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS
(
	order_id,
	@payment_sequential,
	payment_type,
	@payment_installments,
	@payment_value
)
SET 
	payment_sequential = NULLIF(@payment_sequential, ''),
	payment_installments = NULLIF(@payment_installments, ''),
	payment_value = NULLIF(@payment_value, '');


SELECT  'order_payments' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.order_payments;

SELECT  *
  FROM  olist_raw.order_payments
 LIMIT  5;


-- order_reviews (99,224 행)
TRUNCATE TABLE olist_raw.order_reviews;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_order_reviews_dataset.csv'
INTO TABLE olist_raw.order_reviews
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
	ESCAPED BY '"' -- 리뷰 데이터 특성 상 \n, ",", """, "'" 등이 사용되는 경우가 많아 따로 escape 문자를 설정하여 적재
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS
(
	review_id,
	order_id,
	@review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
)
SET 
	review_score = NULLIF(@review_score, '');


SELECT  'order_reviews' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.order_reviews;

SELECT  *
  FROM  olist_raw.order_reviews
 LIMIT  5;


-- customers (99,441 행)
TRUNCATE TABLE olist_raw.customers;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_customers_dataset.csv'
INTO TABLE olist_raw.customers
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT  'customers' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.customers;

SELECT  *
  FROM  olist_raw.customers
 LIMIT  5;


-- products (32,951행)
TRUNCATE TABLE olist_raw.products;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_products_dataset.csv'
INTO TABLE olist_raw.products
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS
(
	product_id,
	product_category_name,
	@product_name_length,
	@product_description_length,
	@product_photos_qty,
	@product_weight_g,
	@product_length_cm,
	@product_height_cm,
	@product_width_cm
)
SET
	product_name_length = NULLIF(@product_name_length, ''),
	product_description_length = NULLIF(@product_description_length, ''),
	product_photos_qty = NULLIF(@product_photos_qty, ''),
	product_weight_g = NULLIF(@product_weight_g, ''),
	product_length_cm = NULLIF(@product_length_cm, ''),
	product_height_cm = NULLIF(@product_height_cm, ''),
	product_width_cm = NULLIF(@product_width_cm, '');


SELECT  'products' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.products;

SELECT  *
  FROM  olist_raw.products
 LIMIT  5;


-- sellers (3,095 행)
TRUNCATE TABLE olist_raw.sellers;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_sellers_dataset.csv'
INTO TABLE olist_raw.sellers
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT  'sellers' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.sellers;

SELECT  *
  FROM  olist_raw.sellers
 LIMIT  5;


-- geolocation (1,000,163 행)
TRUNCATE TABLE olist_raw.geolocation;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/olist_geolocation_dataset.csv'
INTO TABLE olist_raw.geolocation
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS
(
	geolocation_zip_code_prefix,
	@geolocation_lat,
	@geolocation_lng,
	geolocation_city,
	geolocation_state
)
SET
	geolocation_lat = NULLIF(@geolocation_lat, ''),
	geolocation_lng = NULLIF(@geolocation_lng, '');


SELECT  'geolocation' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.geolocation;

SELECT  *
  FROM  olist_raw.geolocation
 LIMIT  5;


-- product_category_name_translation (71 행)
TRUNCATE TABLE olist_raw.product_category_name_translation;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Olist_SQL_Data_Service/product_category_name_translation.csv'
INTO TABLE olist_raw.product_category_name_translation
CHARACTER SET utf8mb4
FIELDS
	TERMINATED BY ','
	ENCLOSED BY '"'
LINES
	TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT  'product_category_name_translation' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.product_category_name_translation;

SELECT  *
  FROM  olist_raw.product_category_name_translation
 LIMIT  5;


-- 전체 row count 검증
SELECT  'orders' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.orders

UNION ALL

SELECT  'order_items' AS  tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.order_items

UNION ALL

SELECT  'order_payments' AS  tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.order_payments

UNION ALL

SELECT  'order_reviews' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.order_reviews

UNION ALL

SELECT  'customers' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.customers

UNION ALL

SELECT  'products' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.products

UNION ALL

SELECT  'sellers' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.sellers

UNION ALL

SELECT  'geolocation' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.geolocation

UNION ALL

SELECT  'product_category_name_translation' AS tbl
		,COUNT(*) AS cnt
  FROM  olist_raw.product_category_name_translation;
  