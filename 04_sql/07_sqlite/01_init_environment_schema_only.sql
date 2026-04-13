/****************************************************************************************************************************************/


/*
 * File: 01_init_environment_schema_only.sql
 * Description:
 * 	- 데이터베이스 레이어 생성 (raw / stg / dm / am)
 * 	- raw 레이어 테이블을 정의 (olist의 원본 데이터를 바탕으로 9개의 테이블 생성)
 * 	- raw 레이어 테이블에 원본 데이터 적재
 * 
 * Notes:
 * 	- 기본적인 레이어(스키마)를 구성하기 위한 스크립트로 모든 스크립트 중 가장 먼저 실행되어야 합니다.
 * 	- raw 스키마에 대한 적재이기 때문에 데이터 가공이나 전처리는 진행되지 않은 상태입니다.
 */


/****************************************************************************************************************************************/


/*
 * 데이터베이스 생성: raw / staging / datamart 세 개의 레이어로 구성 + KPI 집계를 위한 분석 모듈(am)
 */


-- raw schema 생성
CREATE DATABASE IF NOT EXISTS olist_raw
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_0900_ai_ci;

-- stg schema 생성
CREATE DATABASE IF NOT EXISTS olist_stg
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_0900_ai_ci;


-- dm schema 생성
CREATE DATABASE IF NOT EXISTS olist_dm
	CHARACTER SET utf8mb4
	COLLATE utf8mb4_0900_ai_ci;

-- am schema 생성
CREATE DATABASE IF NOT EXISTS olist_am
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