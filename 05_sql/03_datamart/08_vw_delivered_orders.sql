/******************************************************************************************************************************************************/


/*
 * File: 08_dm_vw_delivered_orders.sql
 * Description:
 * 	- 배송 완료 주문 기준 View
 * 	- KPI 산출 시 배송 완료 조건을 일관되게 적용하기 위한 기준 View
 * 	- Grain: 1 row = 1 order_id
 * 	- 필터 규칙:
 * 		- order_status = 'delivered'
 * 		- is_delivered = 1
 * 
 * Note:
 * 	- 해당 View는 필터 기준 고정과 KPI 쿼리 단순화가 목적이며, 매출 집계는 포함되지 않습니다.
 * 	- 배송 완료 여부를 필터링 하기 위해 order_status='delivered'인 조건과 order_status와 실제 배송 완료 여부(시간)이 일치하지 않는 경우를 방지하기 위해 is_delivered=1 조건을 걸었습니다.
 * 
 */


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_dm.vw_delivered_orders AS
SELECT  fo.order_id
		,fo.customer_id
		,fo.customer_zip_code_prefix
		,fo.order_purchase_date_key
		,dd.`year_month`
		,fo.order_status
		,fo.is_delivered
		,fo.is_canceled
		,fo.order_purchase_dt
		,fo.order_approved_dt
		,fo.order_delivered_carrier_dt
		,fo.order_delivered_customer_dt
		,fo.order_estimated_delivery_dt
		,fo.approve_lead_days
		,fo.delivery_lead_days
		,fo.delivery_delay_days
  FROM  olist_dm.fact_orders AS fo
 INNER
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
 WHERE  fo.order_status = 'delivered'
   AND  fo.is_delivered = 1;


-- ===========================================================================================================================================


-- QC

-- 샘플
SELECT  *
  FROM  olist_dm.vw_delivered_orders
 LIMIT  50;

-- 데이터 타입
DESCRIBE olist_dm.vw_delivered_orders;

-- row count: 96,470행 
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.vw_delivered_orders;

-- PK 유니크 확인 -> cnt: 96,470 / distinct_cnt: 96,470 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_cnt
  FROM  olist_dm.vw_delivered_orders;

-- INNER JOIN 연결 정합성 확인 -> year_month가 NULL인 row: 0건
SELECT  COUNT(*) AS null_year_month_cnt
  FROM  olist_dm.vw_delivered_orders
 WHERE  `year_month` IS NULL;

-- delivered인데 delivered_customer_dt가 NULL인 row: 0건
SELECT  COUNT(*) AS bad_cnt
  FROM  olist_dm.vw_delivered_orders
 WHERE  order_delivered_customer_dt IS NULL;

-- 필터 규칙 검증 -> order_status가 delivered가 아니거나 is_delivered가 1이 아닌 row: 0건
SELECT  COUNT(*) AS bad_cnt
  FROM  olist_dm.vw_delivered_orders
 WHERE  order_status <> 'delivered'
    OR  is_delivered <> 1;




