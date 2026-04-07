/******************************************************************************************************************************************************/


/*
 * File: 09_dm_vw_delivered_order_items.sql
 * Description:
 * 	- 배송 완료 주문상품 기준 View
 * 	- 주문상품 단위 KPI 산출 시 배송 완료 조건을 일관되게 적용하기 위한 기준 View
 * 	- Grain: 1 row = 1 order item in 1 order id
 * 	- 필터 규칙:
 * 		- vw_delivered_orders에서 고정(배송 완료 기준)
 * 
 * Note:
 * 	- 해당 View는 필터 기준 고정과 KPI 쿼리 단순화가 목적이며, 매출 집계는 포함되지 않습니다.
 * 
 */


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_dm.vw_delivered_order_items AS
SELECT  foi.order_id
		,foi.order_item_id
		,foi.order_item_seq
		,vdo.customer_id
		,vdo.customer_zip_code_prefix
		,foi.product_id
		,foi.seller_id
		,foi.seller_zip_code_prefix
		,vdo.order_purchase_date_key
		,vdo.`year_month`
		,foi.price
		,foi.freight_value
		,foi.item_total_value
  FROM  olist_dm.fact_order_items AS foi
 INNER
  JOIN  olist_dm.vw_delivered_orders AS vdo
    ON  vdo.order_id = foi.order_id;


-- ===========================================================================================================================================


-- QC

-- 샘플
SELECT  *
  FROM  olist_dm.vw_delivered_order_items
 LIMIT  50;

-- 데이터 타입
DESCRIBE olist_dm.vw_delivered_order_items;

-- row count: 110,189행 
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.vw_delivered_order_items;

-- PK 유니크 확인 -> cnt: 110,189 / distinct_cnt: 110,189 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT CONCAT(order_id, '_', order_item_id)) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '_', order_item_id)) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_order_id_cnt
		,SUM(order_item_id IS NULL OR order_item_id = '') AS blank_order_item_id_cnt
  FROM  olist_dm.vw_delivered_order_items;

-- 조인으로 인한 row 이상 검증 -> 동일 (order_id, order_item_id)가 2회 이상 생성된 row: 0건
SELECT  COUNT(*) AS bad_dup_cnt
  FROM  (
  		SELECT  order_id
  				,order_item_id
  				,COUNT(*) AS cnt
  		  FROM  olist_dm.vw_delivered_order_items
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- 배송완료 기준 재사용 검증 -> vw_delivered_orders에 존재하지 않는 order_id 포함 여부: 0
SELECT  COUNT(*) AS bad_cnt
  FROM  olist_dm.vw_delivered_order_items AS vdoi
  LEFT
  JOIN  olist_dm.vw_delivered_orders AS vdo
    ON  vdo.order_id = vdoi.order_id
 WHERE  vdo.order_id IS NULL;

-- INNER JOIN 연결 정합성 확인 -> year_month가 NULL인 row: 0건
SELECT  COUNT(*) AS null_year_month_cnt
  FROM  olist_dm.vw_delivered_order_items
 WHERE  `year_month` IS NULL;






