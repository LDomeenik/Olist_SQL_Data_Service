/****************************************************************************************************************************************/


/*
 * File: 03_stg_order_items.sql
 * Description:
 * 	- Source 데이터: olist_raw.order_items
 *  - 주문 아이템(order_items) 단위의 Staging 테이블 생성
 * 	- 주문(order) 기준 조인 안전성 확보 및 분석용 타입 표준화 수행
 * 	- 날짜 컬럼과 가격 컬럼을 통해 파생 컬럼 생성 (날짜 단위 컬럼(shipping_limit_dt/date) / 주문 아이템의 총 가격 컬럼(item_total_value))
 * 	- 
 * Notes:
 * 	- order_id와 order_item_id를 결합하여 PK를 생성하였습니다.
 * 	- 식별자 성격을 지닌 ID 컬럼(order_id, order_item_id, product_id, seller_id)은 NOT NULL을 적용하였습니다.
 * 	- 그 외의 컬럼에 대하여는 NULL을 허용하였습니다. (추후 데이터 확장 고려)
 * 	- 정합성 위반이나 결측 값 및 빈 문자열은 없는 것으로 확인되어, 본 테이블에 플래그로 저장하지 않고, 통합 DQ 요약 스크립트에서 계산하여 관리합니다.
 */


/****************************************************************************************************************************************/

USE olist_stg;


/*
 * order_items 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포맷 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- order_items 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 order_items 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */


-- 샘플
SELECT  *
  FROM  olist_raw.order_items
 LIMIT  10;

-- 원본 데이터 타입
DESCRIBE olist_raw.order_items;

-- row count (112,650건)
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.order_items;

-- order_id / order_item_id / product_id / seller_id의 유니크 값 확인
-- order_id -> cnt: 112,650 / distinct_cnt: 98,666 / 중복: 13,984건 / 공백, 결측: 0건 
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_cnt
  FROM  olist_raw.order_items;

-- order_item_id -> cnt: 112,650 / distinct_cnt: 21 / 중복: 112,629 / 공백, 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_item_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_item_id) AS dup_cnt
		,SUM(order_item_id IS NULL OR order_item_id = '') AS blank_cnt
  FROM  olist_raw.order_items;

-- product_id -> cnt: 112,650 / distinct_cnt: 32,951 / 중복: 79,699건 / 공백, 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT product_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT product_id) AS dup_cnt
		,SUM(product_id IS NULL OR product_id = '') AS blank_cnt
  FROM  olist_raw.order_items;

-- seller_id -> cnt:112,650 / distinct_cnt: 3,095 / 중복: 109,555 / 공백, 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT seller_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT seller_id) AS dup_cnt
		,SUM(seller_id IS NULL OR seller_id = '') AS blank_cnt
  FROM  olist_raw.order_items;

-- order_id, order_item_id 중복 여부 -> (order_id, order_item_id) 중복 건수: 0건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  order_id	
  				,order_item_id
  				,COUNT(*) AS cnt
  		  FROM  olist_raw.order_items
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- order_item_id 분포 확인 -> 1~21까지의 주문 순번으로 이루어짐
SELECT  order_item_id
		,COUNT(*)
  FROM  olist_raw.order_items
 GROUP
    BY  order_item_id
 ORDER
    BY  COUNT(*) DESC;

-- price 이상치 및 분포 -> NULL: 0건 / 음수: 0건 / 0원: 0건 / 최솟값: 0.85 / 최댓값: 6,735
SELECT  SUM(price IS NULL) AS price_null
		,SUM(price < 0) AS price_negative
		,SUM(price = 0 ) AS price_zero
		,MIN(price) AS price_min
		,MAX(price) AS price_max
  FROM  olist_raw.order_items;

-- freight_value 이상치 및 분포 -> NULL: 0건 / 음수: 0건 / 0원: 383건 / 최솟값: 0 / 최댓값: 409.68
SELECT  SUM(freight_value IS NULL) AS freight_null
		,SUM(freight_value < 0 ) AS freight_negative
		,SUM(freight_value = 0) AS freight_zero
		,MIN(freight_value) AS freight_min
		,MAX(freight_value) AS freight_max
  FROM  olist_raw.order_items;

-- shipping_limit_date 파싱 실패 건수 -> 0건
SELECT  SUM(shipping_limit_date IS NULL OR TRIM(shipping_limit_date) = '') AS ship_dt_null
		,SUM(STR_TO_DATE(REPLACE(shipping_limit_date, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL
			AND shipping_limit_date IS NOT NULL
			AND TRIM(shipping_limit_date) <> '') AS ship_dt_parse_fail
  FROM  olist_raw.order_items;

-- 주문 구매시각 대비 배송 제한시각 범위(orders 테이블과 조인) -> 배송시각이 주문 구매 시각보다 빠른 건수: 0건
SELECT  COUNT(*) AS ship_before_purchase_cnt
  FROM  olist_raw.order_items oi
  JOIN  olist_stg.stg_orders so
    ON  so.order_id = oi.order_id
 WHERE  STR_TO_DATE(REPLACE(TRIM(oi.shipping_limit_date), '\r', ''), '%Y-%m-%d %H:%i:%s') < so.order_purchase_dt;



/*
 * stg_order_items 테이블 ETL:
 * 	- olist_raw.order_items 데이터의 타입을 변환 -> shipping_limit_dt(원본: shipping_limit_date/문자열): DATETIME / price, freight_value: DECIMAL(10, 2)
 * 	- 분석 편의를 위해 날짜/금액/주문 상품 순번 관련 파생 컬럼 생성 -> shipping_limit_date(DATE) / item_total_value(DECIMAL(10, 2)) / order_item_seq(INT)
 * 
 * Note:
 * 	- 단독 PK가 불가능해 (order_id, order_item_id)로 복합 PK를 지정하였습니다.
 * 	- 식별자 성격의 ID 컬럼(order_id, order_item_id, product_id, seller_id)은 NOT NULL을 지정하였습니다.
 * 	- 주문 상품 순번(order_item_id)의 경우 기존 컬럼은 그대로 보존한 후, 계산의 편의성을 위해 파생 컬럼(order_item_seq)을 INT로 생성하였습니다.
 * 	- shipping_limit_dt를 기준으로 일 단위 파생 컬럼(shipping_limit_date)을 추가 생성하였습니다.
 * 	- 주문 상품 단위 금액의 합(item_total_value)은 price 또는 freight_value가 NULL인 경우 NULL로 계산하여 의미를 보존하였습니다.
 * 	- 사전 DQ 결과, 정합성 위반 row는 따로 발견되지 않아 플래그 컬럼은 생성하지 않았습니다. (전체 DQ 스크립트를 통해 다시 한번 점검 예정)
 */

DROP TABLE IF EXISTS olist_stg.stg_order_items;

-- 테이블 생성
CREATE TABLE olist_stg.stg_order_items (
	order_id		     VARCHAR(50)     NOT NULL,
	order_item_id	     VARCHAR(50)     NOT NULL,
	product_id		     VARCHAR(50)     NOT NULL,
	seller_id		     VARCHAR(50)     NOT NULL,
	shipping_limit_dt    DATETIME        NULL,
	price			     DECIMAL(10, 2)  NULL,
	freight_value	     DECIMAL(10, 2)  NULL,
	
	-- 시간 관련 파생 컬럼
	shipping_limit_date  DATE 			 NULL,
	
	-- 기타 파생 컬럼
	order_item_seq		 INT 			 NULL,
	item_total_value	 DECIMAL(10, 2)  NULL,
	
	-- 복합 PK
	PRIMARY KEY (order_id, order_item_id),
	INDEX idx_stg_order_items_product_id (product_id),
	INDEX idx_stg_order_items_seller_id (seller_id),
	INDEX idx_stg_order_items_shipping_limit_dt (shipping_limit_dt)
);

-- 데이터 적재
TRUNCATE TABLE olist_stg.stg_order_items;


INSERT INTO olist_stg.stg_order_items (
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_dt,
	price,
	freight_value,
	
	shipping_limit_date,
	
	order_item_seq,
	item_total_value
)
SELECT  order_id
		,order_item_id
		,product_id
		,seller_id
		,shipping_limit_dt
		,price
		,freight_value
		,DATE(shipping_limit_dt) AS shipping_limit_date
		,CAST(TRIM(order_item_id) AS UNSIGNED) AS order_item_seq
		,CASE WHEN price IS NULL OR freight_value IS NULL THEN NULL
			  ELSE ROUND(price + freight_value, 2) END AS item_total_value
  FROM  (
 		SELECT  order_id
			    ,order_item_id
				,product_id
				,seller_id
				,STR_TO_DATE(REPLACE(TRIM(shipping_limit_date), '\r', ''), '%Y-%m-%d %H:%i:%s') AS shipping_limit_dt
				,price
				,freight_value
	  	  FROM  olist_raw.order_items
  		) cleaned
 WHERE  order_id IS NOT NULL
   AND  order_item_id IS NOT NULL
   AND  product_id IS NOT NULL
   AND  seller_id IS NOT NULL;


-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_order_items
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_order_items;

-- 행 수 확인 -> raw_order_items: 112,650 / stg_order_items: 112,650
SELECT  (SELECT COUNT(*) FROM olist_raw.order_items) AS raw_cnt
		,(SELECT COUNT(*) FROM olist_stg.stg_order_items) AS stg_cnt;

-- PK 중복 재확인 -> 중복 행 수: 0건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  order_id
  				,order_item_id
  				,COUNT(*)
  		  FROM  olist_stg.stg_order_items
  		 GROUP
  		    BY  1,2
  		HAVING   COUNT(*) > 1
  		)AS t;

-- 정합성 재확인(1): 식별자 성격 NULL 값 및 공백 값 -> order_id: 0 / order_item_id: 0 / product_id: 0 / seller_id: 0
SELECT  SUM(order_id IS NULL OR TRIM(order_id) = '') AS order_id_blank_cnt
		,SUM(order_item_id IS NULL OR TRIM(order_item_id) = '') AS order_item_id_blank_cnt
		,SUM(product_id IS NULL OR TRIM(product_id) = '') AS product_id_blank_cnt
		,SUM(seller_id IS NULL OR TRIM(seller_id) = '') AS seller_id_blank_cnt
  FROM  olist_stg.stg_order_items;

-- 정합성 재확인(2): 기타/파생 컬럼 NULL 값 -> price: 0 / freight_value: 0 / shipping_limit_dt: 0 / shipping_limit_date: 0 / order_item_seq: 0 / item_total_value: 0
SELECT  SUM(price IS NULL) AS price_null_cnt
		,SUM(freight_value IS NULL) AS freight_value_null_cnt
		,SUM(shipping_limit_dt IS NULL) AS ship_dt_null_cnt
		,SUM(shipping_limit_date IS NULL) AS ship_date_null_cnt
		,SUM(order_item_seq IS NULL) AS seq_null_cnt
		,SUM(item_total_value IS NULL) AS total_null_cnt
  FROM  olist_stg.stg_order_items;

-- 정합성 재확인(3): 파생 일자 일관성 체크 -> 변환 실패 건수: 0건
SELECT  COUNT(*) AS ship_date_mismatch_cnt
  FROM  olist_stg.stg_order_items
 WHERE  shipping_limit_date IS NOT NULL
   AND  shipping_limit_date <> DATE(shipping_limit_dt);

-- 정합성 재확인(4): 파생 금액 일관성 체크 -> 불일치 건수(price 혹은 freight_value가 NULL인데 item_total_value가 NULL이 아닌 경우): 0건
SELECT  COUNT(*) AS total_value_mismatch_cnt
  FROM  olist_stg.stg_order_items
 WHERE  (price IS NULL OR freight_value IS NULL)
   AND  item_total_value IS NOT NULL;

-- 값 범위/이상치
-- price < 0: 0 / price = 0: 0 / min_price: 0.85 / max_price: 6,735
-- freight_value < 0: 0 / freight_value = 0: 383 (정상 건수) / freight_min: 0 / freight_max: 409.68
SELECT  SUM(price < 0) AS price_negative_cnt
		,SUM(freight_value < 0) AS freight_negative_cnt
		,SUM(price = 0) AS price_zero_cnt
		,SUM(freight_value  = 0) AS freight_zero_cnt
		,MIN(price) AS price_min
		,MAX(price) AS price_max
		,MIN(freight_value) AS freight_min
		,MAX(freight_value) AS freight_max
  FROM  olist_stg.stg_order_items;

-- 조인 정합성
-- order_id 불일치: 0건
-- product_id 불일치: 0건
-- seller_id 불일치: 0건
SELECT  COUNT(*) AS missing_in_orders_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = oi.order_id
 WHERE  o.order_id IS NULL;

SELECT  COUNT(*) AS missing_in_products_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_raw.products AS p
    ON  p.product_id = oi.product_id
 WHERE  p.product_id IS NULL;

SELECT  COUNT(*) AS missing_in_sellers_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_raw.sellers AS s
    ON  s.seller_id = oi.seller_id
 WHERE  s.seller_id IS NULL;

-- 타임라인 정합성 -> 배송 시간이 주문 시간보다 빠른 건수: 0건
SELECT  COUNT(*) AS ship_before_purchase_cnt
  FROM  olist_stg.stg_order_items AS oi
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = oi.order_id
 WHERE  oi.shipping_limit_dt IS NOT NULL
   AND  oi.shipping_limit_dt < o.order_purchase_dt;