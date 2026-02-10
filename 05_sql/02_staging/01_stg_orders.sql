/****************************************************************************************************************************************/


/*
 * File: 01_stg_orders.sql
 * Description:
 * 	- Source 데이터: olist_raw.orders
 *  - timestamp 문자열을 DATETIME/DATE로 표준화
 *  - 주문 시간 파생 컬럼 + 배송/승인 리드타임 파생 컬럼 생성 
 *  - order_status 컬럼에 대하여 표준화 적용
 * 	- 정합성 위반/결측은 삭제하지 않고 플래그로 파생 컬럼 생성
 * Notes:
 * 	- order_purchase_dt는 이후 분석에서 필수적으로 활용되므로 파싱 실패 row는 적재 대상에서 제외하였습니다.
 *  - order_status는 LOWER + TRIM만 적용하여 표준화하였습니다.
 * 	- 조인 정합성에 이상이 발견(orders에는 있는데 order_items에는 없는 주문(775건)/orders에는 있는데 order_payments에는 없는 주문(1건))되었으나, stg 레이어에서는 따로 파생 컬럼으로 저장하지 않았습니다.
 * 	- 해당 정합성은 추후 DM 레이어에서 통합 정합성 관리 뷰로 관리할 예정입니다.
 */


/****************************************************************************************************************************************/

USE olist_stg;


/*
 * orders 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포멧 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- orders 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 orders 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */


-- 샘플
SELECT  *
  FROM  olist_raw.orders
 LIMIT  10;

-- 원본 데이터 타입
DESCRIBE olist_raw.orders;

-- row count (99,441 행)
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.orders;

-- order_id의 유니크 확인(PK 사용 가능 여부) -> cnt: 99,441 / distinct_cnt: 99,441 / 중복 개수: 0건 / 공백 개수: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_cnt
  FROM  olist_raw.orders;

-- 필수 키로 사용될 컬럼들의 결측치 확인 (결측치 없음)
SELECT  SUM(order_id IS NULL OR order_id = '') AS order_id_blank_cnt
		,SUM(customer_id IS NULL OR customer_id = '') AS customer_id_blank_cnt
		,SUM(order_status IS NULL OR order_status = '') AS order_status_blank_cnt
		,SUM(order_purchase_timestamp IS NULL OR order_purchase_timestamp = '') AS order_purchase_timestamp_blank_cnt
  FROM  olist_raw.orders;

-- order_status 분포 (delivered: 96,478 / shipped: 1,107 / canceled: 625 / unavailable: 609 / invoiced: 314 / processing: 301 / created: 5 / approved: 2 => 99,441)
SELECT  LOWER(TRIM(REPLACE(order_status, '\r', ''))) AS order_status_norm
		,COUNT(*) AS cnt
  FROM  olist_raw.orders
 GROUP
    BY  1
 ORDER
    BY  2 DESC;

-- purchase timestamp 파싱 실패 건수 (0건)
SELECT  COUNT(*) AS purchase_parse_fail_cnt
  FROM  olist_raw.orders
 WHERE  STR_TO_DATE(REPLACE(order_purchase_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL;

-- 기타 시간 관련 컬럼 파싱 싪패 건수 (0 / 0 / 0 / 0 건)
SELECT  SUM(order_approved_at <> '' AND STR_TO_DATE(REPLACE(order_approved_at, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL) AS approved_parse_fail_cnt
		,SUM(order_delivered_carrier_date <> '' AND STR_TO_DATE(REPLACE(order_delivered_carrier_date, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL) AS carrier_parse_fail_cnt
		,SUM(order_delivered_customer_date <> '' AND STR_TO_DATE(REPLACE(order_delivered_customer_date, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL) AS customer_parse_fail_cnt
		,SUM(order_estimated_delivery_date <> '' AND STR_TO_DATE(REPLACE(order_estimated_delivery_date, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL) AS estimated_parse_fail_cnt
  FROM  olist_raw.orders;

-- 시간 순서 이상치 (승인<구매, 배송<구매 등)
-- 주문 시간(purchase)보다 주문 승인 시간(approved)이 빠른 데이터: 0 건
-- 주문 시간(purchase)보다 배송 출발 시간(order_delivered_carrier_date)이 빠른 데이터가 166건 존재
-- 주문 시간(purchase)보다 배송 완료 시간(deliver)이 빠른 데이터: 0건
WITH cte AS (
	SELECT  order_id
			,STR_TO_DATE(REPLACE(order_purchase_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s') AS purchase_dt
			,STR_TO_DATE(REPLACE(order_approved_at, '\r', ''), '%Y-%m-%d %H:%i:%s') AS approved_dt
			,STR_TO_DATE(REPLACE(order_delivered_carrier_date, '\r', ''), '%Y-%m-%d %H:%i:%s') AS carrier_dt
			,STR_TO_DATE(REPLACE(order_delivered_customer_date, '\r', ''), '%Y-%m-%d %H:%i:%s') AS customer_dt
	  FROM  olist_raw.orders
)
SELECT  SUM(approved_dt IS NOT NULL AND approved_dt < purchase_dt) AS approved_before_purchase_cnt
		,SUM(carrier_dt IS NOT NULL AND carrier_dt < purchase_dt) AS carrier_before_purchase_cnt
		,SUM(customer_dt IS NOT NULL AND customer_dt < purchase_dt) AS delivered_before_purchase_cnt
  FROM  cte;

SELECT  *
  FROM  olist_raw.orders
 WHERE  STR_TO_DATE(REPLACE(order_delivered_carrier_date, '\r', ''), '%Y-%m-%d %H:%i:%s') < STR_TO_DATE(REPLACE(order_purchase_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s')
 LIMIT  10;

-- 배송 상태와 시간 사이의 이상치
-- 배송 완료된 상품인데 배송 완료 시각이 없는 상품 건수: 8 건
-- 취소된 상품인데 배송 완료 시각이 있는 상품 건수: 6 건
-- 결제 승인된 상품인데 결제 승인 시각이 없는 상품 건수: 0건
-- 배송중이거나 송장 발행된 상품인데 배송 시각이 없는 상품 건수: 314 건
WITH cte AS (
	SELECT  LOWER(TRIM(REPLACE(order_status, '\r', ''))) AS status
			,STR_TO_DATE(NULLIF(REPLACE(order_approved_at, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS approved_dt
			,STR_TO_DATE(NULLIF(REPLACE(order_delivered_carrier_date, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS carrier_dt
			,STR_TO_DATE(NULLIF(REPLACE(order_delivered_customer_date, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS customer_dt
	  FROM  olist_raw.orders
)
SELECT  SUM(status = 'delivered' AND customer_dt IS NULL) AS delivered_but_no_delivered_dt_cnt
		,SUM(status IN ('canceled', 'unavailable') AND customer_dt IS NOT NULL) AS canceled_but_has_delivered_dt_cnt
		,SUM(status = 'approved' AND approved_dt IS NULL) AS approved_but_no_approved_dt_cnt
		,SUM(status IN ('shipped', 'invoiced') AND carrier_dt IS NULL) AS shipped_or_invoiced_but_no_carrier_dt_cnt
  FROM  cte;

-- 조인 정합성 검증(1): orders에는 있는데 order_items에는 없는 주문 -> 775건
-- 해당 주문들의 order_status 분포: unavailable: 603건 / canceled: 164건 / created: 5건 / invoiced: 2건 / shipped: 1건
SELECT  COUNT(*) AS orders_without_items
  FROM  olist_raw.orders AS o
  LEFT
  JOIN  olist_raw.order_items AS oi
    ON  oi.order_id = o.order_id
 WHERE  oi.order_id IS NULL;

-- 조인 정합성 불일치 샘플
SELECT  o.order_id
  FROM  olist_raw.orders AS o
  LEFT
  JOIN  olist_raw.order_items AS oi
    ON  oi.order_id = o.order_id
 WHERE  oi.order_id IS NULL;

SELECT  *
  FROM  olist_raw.orders
 WHERE  order_id = 'c272bcd21c287498b4883c7512019702';

-- 조인 정합성 불일치 row의 order_status 분포
WITH CTE AS (
	SELECT  o.order_id
			,o.order_status
	  FROM  olist_raw.orders AS o
	  LEFT
	  JOIN  olist_raw.order_items AS oi
	    ON  oi.order_id = o.order_id
	 WHERE  oi.order_id IS NULL
)
SELECT  order_status
		,COUNT(*) AS cnt
  FROM  CTE
 GROUP
    BY  order_status
 ORDER
    BY  cnt DESC;

-- 조인 정합성 검증(2): orders에는 있는데 order_payments에는 없는 주문 -> 1건(bfbd0f9bdef84302105ad712db648a6c)
SELECT  COUNT(*) AS order_without_payments
  FROM  olist_raw.orders AS o
  LEFT
  JOIN  olist_raw.order_payments AS op
    ON  op.order_id = o.order_id
 WHERE  op.order_id IS NULL;

SELECT  o.order_id
  FROM  olist_raw.orders AS o
  LEFT
  JOIN  olist_raw.order_payments AS op
    ON  op.order_id = o.order_id
 WHERE  op.order_id IS NULL;

-- 해당 주문 번호의 주문 내역 -> order_status: delivered / order_purchase_timestamp: 2016-09-15 12:16:38 / order_delivered_customer_date: 2016-11-09 07:47:38
-- 주문 상태는 delivered이며 order_items 및 금액 합계가 존재하나, order_payments row가 존재하지 않는 원천 데이터 불일치 케이스로 확인됨
SELECT  order_id	
		,order_status
		,order_purchase_timestamp
		,order_approved_at
		,order_delivered_carrier_date
		,order_delivered_customer_date
  FROM  olist_raw.orders
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 해당 주문 번호의 결제 건수: 0건
SELECT  COUNT(*) AS pay_cnt
  FROM  olist_raw.order_payments
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 해당 주문 번호의 주문 상품 건수: 3건
SELECT  COUNT(*) AS item_cnt
  FROM  olist_raw.order_items
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 해당 주문 번호의 결제 금액: 정상
SELECT  SUM(price) AS sum_price
		,SUM(freight_value) AS sum_freight
		,SUM(price + freight_value) AS sum_item_total
  FROM  olist_raw.order_items
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 조인 정합성 검증(3): orders의 customer_id가 customers에는 없는 경우 -> 0건
SELECT  COUNT(*) AS orders_with_missing_customer
  FROM  olist_raw.orders AS o
  LEFT
  JOIN  olist_raw.customers AS c
    ON  c.customer_id = o.customer_id
 WHERE  c.customer_id IS NULL;


/*
 * stg_orders 테이블 ETL:
 * 	- olist_raw.orders 데이터의 타입을 변환 -> timestamp 문자열을 DATETIME/DATE 타입으로 변환
 * 	- 주문 기준 시점(purchase)을 중심으로 파생 컬럼 생성
 * 	- 데이터 삭제 없이 최소한의 정합성 검증 수행 -> 정합성 위반 row는 플래그로 표시하고 원본 데이터는 유지
 * 
 * Note:
 * 	- 정합성 위반 row를 삭제하지 않고 플래그로 관리함으로써 데이터 손실을 방지하고 추후 분석 단계에서 필터링이 가능하도록 설계하였습니다.
 * 	- 리드타임 등 분석에 직접적인 영향을 줄 수 있는 파생 값은 시간 정합성이 보장되는 경우에만 조건부 계산이 이루어져야 합니다.
 * 	- 비즈니스 정의에 따른 필터링은 이후 Data Mart 레이어에서 수행할 예정입니다.
 */

DROP TABLE IF EXISTS olist_stg.stg_orders;


-- 테이블 생성
CREATE TABLE olist_stg.stg_orders (
	order_id					 VARCHAR(50)  NOT NULL,
	customer_id					 VARCHAR(50)  NOT NULL,
	order_status				 VARCHAR(20)  NOT NULL,
	
	order_purchase_dt			 DATETIME	  NOT NULL,
	order_approved_dt			 DATETIME	  NULL,
	order_delivered_carrier_dt   DATETIME	  NULL,
	order_delivered_customer_dt  DATETIME	  NULL,
	order_estimated_delivery_dt  DATE		  NULL,
	
	-- 시간 관련 파생 컬럼(purchase 기준 파생)
	order_purchase_date			 DATE		  NOT NULL,
	order_year					 SMALLINT	  NOT NULL,
	order_month					 TINYINT	  NOT NULL,
	order_year_month			 CHAR(7)	  NOT NULL, -- YYYY-MM
	
	-- 배송/승인 관련 파생 지표(리드타임 파생)
	approve_lead_days			 INT		  NULL,
	delivery_lead_days			 INT		  NULL,
	delivery_delay_days			 INT		  NULL,
	
	-- 주문 상태 관련 파생 지표(status 기준 파생)
	is_delivered				 TINYINT	  NOT NULL,
	is_canceled					 TINYINT	  NOT NULL,
	
	-- 정합성 관련 파생 컬럼(상태/시간 플래그)
	is_time_inconsistent		 TINYINT	  NOT NULL,
	is_status_inconsistent		 TINYINT	  NOT NULL,
	is_carrier_dt_missing		 TINYINT	  NOT NULL,
	
	-- PK 지정(중복이 없고 각 행을 대표하는 order_id로 지정)
	PRIMARY KEY (order_id),
	-- INDEX 설정(customer_id: 고객 테이블과의 조인 목적 / order_purchase_dt: 기간 필터/집계 목적)
	INDEX idx_stg_orders_customer_id (customer_id),
	INDEX idx_stg_orders_purchase_dt (order_purchase_dt)
);


-- 데이터 적재
-- 	- LOWER(TRIM(REPLACE('\r', ''))): 공백, 줄 바꿈 제거 및 소문자로 표준화
-- 	- NULLIF('', NULL): 빈 문자열을 NULL로 변환하여 DATETIME 파싱
-- 	- purchase_dt는 분석 기준 시점이므로 NOT NULL 보장 (파싱 실패 row는 제외해야 하지만 사전 DQ상 0건)
-- 	- estimated는 시간보다는 '일자'가 핵심인 것으로 생각되어 DATE로 표준화
TRUNCATE TABLE olist_stg.stg_orders;

WITH parsed AS (
	SELECT  order_id
			,customer_id
			,LOWER(TRIM(REPLACE(order_status, '\r', ''))) AS order_status
			
			,STR_TO_DATE(REPLACE(order_purchase_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s') AS purchase_dt
			,STR_TO_DATE(NULLIF(REPLACE(order_approved_at, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS approved_dt
			,STR_TO_DATE(NULLIF(REPLACE(order_delivered_carrier_date, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS  delivered_carrier_dt
			,STR_TO_DATE(NULLIF(REPLACE(order_delivered_customer_date, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS delivered_customer_dt
			,STR_TO_DATE(NULLIF(REPLACE(order_estimated_delivery_date, '\r', ''), ''), '%Y-%m-%d %H:%i:%s') AS estimated_delivery_dt
	  FROM  olist_raw.orders
)
INSERT INTO olist_stg.stg_orders (
	order_id,
	customer_id,
	order_status,
	
	order_purchase_dt,
	order_approved_dt,
	order_delivered_carrier_dt,
	order_delivered_customer_dt,
	order_estimated_delivery_dt,
	
	order_purchase_date,
	order_year,
	order_month,
	order_year_month,
	
	approve_lead_days,
	delivery_lead_days,
	delivery_delay_days,
	
	is_delivered,
	is_canceled,
	
	is_time_inconsistent,
	is_status_inconsistent,
	is_carrier_dt_missing
)
SELECT  order_id
		,customer_id
		,order_status
		
		,purchase_dt AS order_purchase_dt
		,approved_dt AS order_approved_dt
		,delivered_carrier_dt AS order_delivered_carrier_dt
		,delivered_customer_dt AS order_delivered_customer_dt
		,DATE(estimated_delivery_dt) AS order_estimated_delivery_dt
		
		,DATE(purchase_dt) AS  order_purchase_date
		,YEAR(purchase_dt) AS order_year
		,MONTH(purchase_dt) AS order_month
		,DATE_FORMAT(purchase_dt, '%Y-%m') AS order_year_month
		
		-- 리드타임 파생: 시간 정합성이 보장되는 경우에만 계산
		,CASE WHEN approved_dt IS NULL THEN NULL
			  WHEN approved_dt < purchase_dt THEN NULL
			  ELSE DATEDIFF(approved_dt, purchase_dt)
			  END AS approve_lead_days
		
		,CASE WHEN delivered_customer_dt IS NULL THEN NULL
			  WHEN delivered_customer_dt < purchase_dt THEN NULL
			  ELSE DATEDIFF(delivered_customer_dt, purchase_dt)
			  END AS delivery_lead_days
		
		,CASE WHEN delivered_customer_dt IS NULL OR estimated_delivery_dt IS NULL THEN NULL
			  WHEN delivered_customer_dt < purchase_dt THEN NULL
			  ELSE DATEDIFF(delivered_customer_dt, estimated_delivery_dt)
			  END AS delivery_delay_days
		
		,CASE WHEN delivered_customer_dt IS NULL THEN 0 ELSE 1 END AS is_delivered
		,CASE WHEN order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END AS is_canceled
		
		-- 정합성 플래그 - 시간 흐름
		,CASE WHEN (approved_dt IS NOT NULL AND approved_dt < purchase_dt)
			    OR (delivered_carrier_dt IS NOT NULL AND delivered_carrier_dt < purchase_dt)
			    OR (delivered_customer_dt IS NOT NULL AND delivered_customer_dt < purchase_dt) THEN 1
			  ELSE 0
			  END AS is_time_inconsistent
		
		-- 정합성 플래그 - 상태, 시간
		,CASE WHEN (order_status = 'delivered' AND delivered_customer_dt IS NULL)
				OR (order_status IN ('canceled', 'unavailable') AND delivered_customer_dt IS NOT NULL) THEN 1
			  ELSE 0
			  END AS is_status_inconsistent
		
		-- 정합성 플래그 - shipped/invoiced 상태에서 carrier_dt 누락
		,CASE WHEN order_status IN ('shipped', 'invoiced') AND delivered_carrier_dt IS NULL THEN 1
			  ELSE 0
			  END AS is_carrier_dt_missing

  FROM  parsed
 WHERE  purchase_dt IS NOT NULL; -- 혹시 모를 오류 방지
 
 

-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_orders
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_orders;

-- 행 수 확인: raw.orders: 99,441 행 / stg_orders: 99,441 행
SELECT  (SELECT COUNT(*) FROM olist_raw.orders) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_orders) AS stg_cnt;

-- PK 중복 재확인 (전체 행: 99,441 행 / order_id의 distinct count: 99,441 / 중복 개수: 0 건)
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS  distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
  FROM  olist_stg.stg_orders;

-- 리드타임 음수 확인 (approve_lead_days: 0건 / delivery_lead_days: 0건)
SELECT  SUM(approve_lead_days < 0) AS neg_approve_lead_cnt
		,SUM(delivery_lead_days < 0) AS neg_delivery_lead_cnt
  FROM  olist_stg.stg_orders;

-- 정합성 플래그 수 확인:
-- 	- 시간 순서 이상치: raw.orders: 166건 / stg_orders: 166건
-- 	- 상태와 시간 존재 간 이상치: raw.orders: 14건 / stg_orders: 14건
-- 	- 상태와 시간 존재 간 이상치 여부 확정 불가(shipped/invoiced): raw.orders: 314건 / stg_orders: 314건
SELECT  SUM(is_time_inconsistent = 1) AS time_inconsistent_cnt
		,SUM(is_status_inconsistent = 1) AS status_inconsistent_cnt
		,SUM(is_carrier_dt_missing = 1) AS carrier_dt_missing_cnt
  FROM  olist_stg.stg_orders;