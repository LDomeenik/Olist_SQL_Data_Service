/****************************************************************************************************************************************/


/*
 * File: 06_stg_order_payments.sql
 * Description:
 * 	- Source 데이터: olist_raw.order_payments
 *  - 주문 결제(order_payment) 단위의 Staging 테이블 생성
 * 	- 주문 식별자(order_id) 기준 조인 안전성 확보  
 * 	- 문자열 컬럼(payment_type) 표준화(LOWER / TRIM 적용)
 * 	- 정합성 위반 row는 삭제하지 않고 플래그로 관리 (is_installments_zero / is_payment_value_zero)
 * Notes:
 * 	- order_id와 payment_sequential을 결합하여 복합 PK로 사용하였습니다.
 * 	- 식별자 성격을 지닌 ID 컬럼(order_id)과 복합 PK로 사용한 payment_sequential은 NOT NULL을 적용하였습니다.
 * 	- 그 외의 컬럼에 대하여는 NULL을 허용하였습니다. (추후 데이터 확장 고려)
 * 	- 카드로 결제하였으나, 할부 기간이 0인 row들(payment_installments = 0)이 확인되어, 해당 내용을 삭제/보정하지 않고 플래그로 관리하였습니다.
 * 	- 결제는 있지만 결제 금액이 0원인 row들(payment_value = 0)이 확인되어, 해당 내용을 삭제/보정하지 않고 플래그로 관리하였습니다.
 * 	- payment_value = 0인 row들은 결제 수단(payment_type)이 voucher/not_defined로 확실한 이상치라고 볼 수는 없다고 판단했습니다. (쿠폰/바우처 등으로 인한 0원 가능성)
 * 	- 조인 정합성 과정에서 orders에는 있으나, order_payments에는 없는 주문 내역(1건)이 발견되었으나, 따로 처리하지 않고 추후 DM 레이어에서 관리할 예정입니다. (order_payments에는 없는 row이기 때문)
 */


/****************************************************************************************************************************************/

USE olist_stg;


/*
 * order_payments 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포맷 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- order_payments 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 order_payments 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */

-- 샘플
SELECT  *
  FROM  olist_raw.order_payments
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_raw.order_payments;

-- row count: 103,886행
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.order_payments;

-- order_id의 유니크 확인 -> cnt: 103,886 / distinct_cnt: 99,440 / 중복: 4,446건 / 공백 및 결측치: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id  = '') AS blank_cnt
  FROM  olist_raw.order_payments;

-- payment_type 공백 및 결측치: 0건
SELECT  SUM(payment_type IS NULL  OR TRIM(REPLACE(payment_type, '\r', '')) = '') AS type_blank_cnt
  FROM  olist_raw.order_payments;

-- (order_id, payment_sequential) 유니크 확인(PK 가능 여부) -> 중복: 0건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  order_id	
  				,payment_sequential
  				,COUNT(*) AS cnt
  		  FROM  olist_raw.order_payments
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- payment_type 분포 -> credit_card: 73.92%(76,795건) / boleto: 19.04%(19,784건) / voucher: 5.56%(5,775건) / debit_card: 1.47%(1,529건) / not_defined: 0.00%(3건)
WITH CTE AS (
	SELECT  payment_type
			,COUNT(*) AS cnt
	  FROM  olist_raw.order_payments
	 GROUP
	    BY  payment_type
)
SELECT  payment_type
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- payment_sequential 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 0건 / 최솟값: 1 / 최댓값: 29
SELECT  SUM(payment_sequential IS NULL) AS null_cnt
		,SUM(payment_sequential < 0) AS negative_cnt
		,SUM(payment_sequential = 0) AS zero_cnt
		,MIN(payment_sequential) AS value_min
		,MAX(payment_sequential) AS value_max
  FROM  olist_raw.order_payments;

-- payment_installments 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 2건 / 최솟값: 0 / 최댓값: 24
-- 	- payment_installments가 0인 row는 모두 payment_type이 credit_card이고, payment_sequential이 2
SELECT  SUM(payment_installments IS NULL) AS null_cnt
		,SUM(payment_installments < 0) AS negative_cnt
		,SUM(payment_installments = 0) AS zero_cnt
		,MIN(payment_installments) AS value_min
		,MAX(payment_installments) AS value_max
  FROM  olist_raw.order_payments;

SELECT  *
  FROM  olist_raw.order_payments
 WHERE  payment_installments = 0;

-- payment_value 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 9건 / 최솟값: 0 / 최댓값: 13,664.08
-- 	- payment_value가 0인 값들은 모두 voucher/not_defined이며, payment_installments는 1
SELECT  SUM(payment_value IS NULL) AS null_cnt
		,SUM(payment_value < 0) AS negative_cnt
		,SUM(payment_value = 0) AS zero_cnt
		,MIN(payment_value) AS value_min
		,MAX(payment_value) AS value_max
  FROM  olist_raw.order_payments;

SELECT  *
  FROM  olist_raw.order_payments
 WHERE  payment_value = 0;

-- order_id당 결제 row 수 분포 -> 최대 29건의 결제 row를 가진 주문이 있음
SELECT  order_id
		,COUNT(*) AS cnt
  FROM  olist_raw.order_payments
 GROUP
    BY  order_id
 ORDER
    BY  COUNT(*) DESC;

-- order_id당 결제 row 수 분포 -> 1: 96,479 / 2: 2,382 / 3: 310 / 4: 108 / 이외 5~29는 10 이하의 결제 row를 가짐
WITH t AS (
	SELECT  order_id
			,COUNT(*) AS pay_rows
	  FROM  olist_raw.order_payments
	 GROUP
	    BY  order_id
)
SELECT  pay_rows
		,COUNT(*) AS order_cnt
  FROM  t
 GROUP
    BY  pay_rows
 ORDER
    BY  pay_rows DESC;

-- 조인 정합성(1): orders에는 있지만 payments에는 없는 주문 -> 1건
SELECT  COUNT(*) AS order_without_payment
  FROM  olist_raw.orders AS o
  LEFT
  JOIN  olist_raw.order_payments AS op
    ON  op.order_id = o.order_id
 WHERE  op.order_id IS NULL;

-- 해당 주문의 주문 번호: 'bfbd0f9bdef84302105ad712db648a6c'
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

-- 조인 정합성(2): payments에는 있지만 orders에는 없는 주문 -> 0건
SELECT  COUNT(*) AS payments_without_order
  FROM  olist_raw.order_payments AS op
  LEFT
  JOIN  olist_raw.orders AS o
    ON  o.order_id = op.order_id
 WHERE  o.order_id IS NULL;


/*
 * stg_order_payments 테이블 ETL:
 * 	- order_id와 payment_sequential을 결합하여 PK로 지정
 * 	- payment_type 컬럼 표준화(LOWER / TRIM 적용)
 * 	- payment_installments와 payment_value에 0값이 발견되어, 플래그 컬럼 생성(is_installments_zero/is_payment_value_zero)
 */

DROP TABLE IF EXISTS olist_stg.stg_order_payments;


-- 테이블 생성
CREATE TABLE olist_stg.stg_order_payments (
	order_id			   VARCHAR(50)	   NOT NULL,
	payment_sequential	   INT			   NOT NULL,
	payment_type		   VARCHAR(20)	   NULL,
	payment_installments   INT			   NULL,
	payment_value		   DECIMAL(10, 2)  NULL,
	
	-- 플래그 컬럼
	is_installments_zero   TINYINT		   NOT NULL,
	is_payment_value_zero  TINYINT		   NOT NULL,
	
	-- PK 및 INDEX 지정
	PRIMARY KEY (order_id, payment_sequential),
	INDEX idx_stg_order_payments_order_id (order_id),
	INDEX idx_stg_order_payments_type (payment_type)
);

-- 데이터 적재
TRUNCATE TABLE olist_stg.stg_order_payments;


INSERT INTO olist_stg.stg_order_payments (
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value,
	
	is_installments_zero,
	is_payment_value_zero
)
SELECT  order_id
		,payment_sequential
		,LOWER(TRIM(payment_type)) AS payment_type
		,payment_installments
		,payment_value
		,CASE WHEN payment_installments = 0 THEN 1
			  ELSE 0
			  END AS is_installments_zero
		,CASE WHEN payment_value = 0 THEN 1
			  ELSE 0
			  END AS is_payment_value_zero
  FROM  olist_raw.order_payments
 WHERE  order_id IS NOT NULL
   AND  order_id <> ''
   AND  payment_sequential IS NOT NULL; -- PK 명시
	

-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_order_payments
 LIMIT  10;

-- row count -> 103,886행
SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_order_payments;

-- order_id의 유니크 확인 -> cnt: 103,886 / distinct_cnt: 99,440 / 중복: 4,446건 / 공백 및 결측치: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_cnt
  FROM  olist_stg.stg_order_payments;

-- payment_type 공백 및 결측치: 0건
SELECT  SUM(payment_type IS NULL  OR payment_type = '') AS type_blank_cnt
  FROM  olist_stg.stg_order_payments;

-- (order_id, payment_sequential) 유니크 확인(PK 가능 여부) -> 중복: 0건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  order_id	
  				,payment_sequential
  				,COUNT(*) AS cnt
  		  FROM  olist_stg.stg_order_payments
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- payment_type 분포 -> credit_card: 73.92%(76,795건) / boleto: 19.04%(19,784건) / voucher: 5.56%(5,775건) / debit_card: 1.47%(1,529건) / not_defined: 0.00%(3건)
WITH CTE AS (
	SELECT  payment_type
			,COUNT(*) AS cnt
	  FROM  olist_stg.stg_order_payments
	 GROUP
	    BY  payment_type
)
SELECT  payment_type
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- payment_sequential 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 0건 / 최솟값: 1 / 최댓값: 29
SELECT  SUM(payment_sequential IS NULL) AS null_cnt
		,SUM(payment_sequential < 0) AS negative_cnt
		,SUM(payment_sequential = 0) AS zero_cnt
		,MIN(payment_sequential) AS value_min
		,MAX(payment_sequential) AS value_max
  FROM  olist_stg.stg_order_payments;

-- payment_installments 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 2건 / 최솟값: 0 / 최댓값: 24
-- 	- payment_installments가 0인 row는 모두 payment_type이 credit_card이고, payment_sequential이 2
--  - is_installments_zero 컬럼이 1인(payment_installments = 0) row가 총 2행 -> 이상 없음
SELECT  SUM(payment_installments IS NULL) AS null_cnt
		,SUM(payment_installments < 0) AS negative_cnt
		,SUM(payment_installments = 0) AS zero_cnt
		,MIN(payment_installments) AS value_min
		,MAX(payment_installments) AS value_max
  FROM  olist_stg.stg_order_payments;

SELECT  *
  FROM  olist_stg.stg_order_payments
 WHERE  payment_installments = 0;

SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_order_payments
 WHERE  is_installments_zero = 1;

-- payment_value 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 9건 / 최솟값: 0 / 최댓값: 13,664.08
-- 	- payment_value가 0인 값들은 모두 voucher/not_defined이며, payment_installments는 1
-- 	- is_payment_value_zero 컬럼이 1인(payment_value = 0) row가 총 9행 -> 이상 없음
SELECT  SUM(payment_value IS NULL) AS null_cnt
		,SUM(payment_value < 0) AS negative_cnt
		,SUM(payment_value = 0) AS zero_cnt
		,MIN(payment_value) AS value_min
		,MAX(payment_value) AS value_max
  FROM  olist_stg.stg_order_payments;

SELECT  *
  FROM  olist_stg.stg_order_payments
 WHERE  payment_value = 0;

SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_order_payments
 WHERE  is_payment_value_zero = 1;

-- order_id당 결제 row 수 분포 -> 최대 29건의 결제 row를 가진 주문이 있음
SELECT  order_id
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_order_payments
 GROUP
    BY  order_id
 ORDER
    BY  COUNT(*) DESC;

-- order_id당 결제 row 수 분포 -> 1: 96,479 / 2: 2,382 / 3: 310 / 4: 108 / 이외 5~29는 10 이하의 결제 row를 가짐
WITH t AS (
	SELECT  order_id
			,COUNT(*) AS pay_rows
	  FROM  olist_stg.stg_order_payments
	 GROUP
	    BY  order_id
)
SELECT  pay_rows
		,COUNT(*) AS order_cnt
  FROM  t
 GROUP
    BY  pay_rows
 ORDER
    BY  pay_rows DESC;

-- 조인 정합성(1): orders에는 있지만 payments에는 없는 주문 -> 1건
SELECT  COUNT(*) AS order_without_payment
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_stg.stg_order_payments AS op
    ON  op.order_id = o.order_id
 WHERE  op.order_id IS NULL;

-- 해당 주문의 주문 번호: 'bfbd0f9bdef84302105ad712db648a6c'
SELECT  o.order_id
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_stg.stg_order_payments AS op
    ON  op.order_id = o.order_id
 WHERE  op.order_id IS NULL;

-- 해당 주문 번호의 주문 내역 -> order_status: delivered / order_purchase_timestamp: 2016-09-15 12:16:38 / order_delivered_customer_date: 2016-11-09 07:47:38
-- 주문 상태는 delivered이며 order_items 및 금액 합계가 존재하나, order_payments row가 존재하지 않는 원천 데이터 불일치 케이스로 확인됨
SELECT  order_id	
		,order_status
		,order_purchase_dt
		,order_approved_dt
		,order_delivered_carrier_dt
		,order_delivered_customer_dt
  FROM  olist_stg.stg_orders
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 해당 주문 번호의 결제 건수: 0건
SELECT  COUNT(*) AS pay_cnt
  FROM  olist_stg.stg_order_payments
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 해당 주문 번호의 주문 상품 건수: 3건
SELECT  COUNT(*) AS item_cnt
  FROM  olist_stg.stg_order_items
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 해당 주문 번호의 결제 금액: 정상값으로 존재
SELECT  SUM(price) AS sum_price
		,SUM(freight_value) AS sum_freight
		,SUM(price + freight_value) AS sum_item_total
  FROM  olist_stg.stg_order_items
 WHERE  order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-- 조인 정합성(2): payments에는 있지만 orders에는 없는 주문 -> 0건
SELECT  COUNT(*) AS payments_without_order
  FROM  olist_stg.stg_order_payments AS op
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = op.order_id
 WHERE  o.order_id IS NULL;














