/******************************************************************************************************************************************************/


/*
 * File: 10_dm_vw_customer_first_purchase_month.sql
 * Description:
 * 	- 고객별 첫 구매 월(cohort month)을 정의하는 기준 View
 * 	- 코호트 분석 및 월차 리텐션 분석의 기준점(first_purchase_month)을 제공
 * 	- 이후 코호트/리텐션/KPI View에서 공통 기준으로 재사용
 * 	- 첫 구매 월 정의는 배송이 완료된 고객들에 한해서 정의
 * 
 * 코호트 기준 정의:
 * 	- 배송 완료 주문(vw_delivered_orders)만을 대상으로 첫 구매를 정의
 * 	- 고객 식별 기준은 customer_unique_id
 *  - 고객별 최초 구매 일자는 MIN(order_purchase_date_key)로 정의
 * 	- 최초 구매 월은 dim_date.year_month 기준으로 정의
 * 
 * Note:
 * 	- 해당 View는 고객별 첫 구매 월을 정의하는 목적으로 매출/주문 수 등 KPI 집계를 수행하지 않습니다.
 * 	- 또한 같은 목적으로 최소 컬럼만을 포함하고 있습니다. (첫 구매 기준을 배송 완료 기준으로 고정함으로써 KPI 산출 조건과의 불일치를 방지)
 * 	- vw_delivered_orders를 기준 집합으로 사용하여 배송 완료 조건을 일관되게 유지하고 있습니다.
 * 
 */


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_dm.vw_customer_first_purchase_month AS
WITH first_purchase AS (
	SELECT  dc.customer_unique_id
			,MIN(vdo.order_purchase_date_key) AS first_purchase_date_key
	  FROM  olist_dm.vw_delivered_orders AS vdo
	 INNER
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdo.customer_id
	 GROUP
	    BY  dc.customer_unique_id
)
SELECT  fp.customer_unique_id
		,fp.first_purchase_date_key
		,dd.`year_month` AS first_purchase_year_month
  FROM  first_purchase AS fp
 INNER
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fp.first_purchase_date_key;


-- ===========================================================================================================================================


-- QC

-- 샘플
SELECT  *
  FROM  olist_dm.vw_customer_first_purchase_month
 LIMIT  50;

-- 데이터 타입
DESCRIBE olist_dm.vw_customer_first_purchase_month;

-- row count: 93,350행
SELECT  COUNT(*) AS row_cnt
  FROM  olist_dm.vw_customer_first_purchase_month;

-- PK 유니크 확인 -> cnt: 93,350 / distinct_cnt: 93,350 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT customer_unique_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT customer_unique_id) AS dup_cnt
		,SUM(customer_unique_id IS NULL OR customer_unique_id = '') AS blank_cnt
  FROM  olist_dm.vw_customer_first_purchase_month;

-- dim_date 조인 실패 여부 -> first_purchase_year_month가 NULL인 row: 0건
SELECT  COUNT(*) AS null_month_cnt
  FROM  olist_dm.vw_customer_first_purchase_month
 WHERE  first_purchase_year_month IS NULL;

-- 첫 구매월 상위 분포 확인
-- first_purchase_year_month		customer_cnt
-- =================================================
-- 			2016-09						1
-- 			2016-10						262
-- 			2016-12						1
--			2017-01						717
--			2017-02						1628
-- 			2017-03						2503
-- 			2017-04						2256
-- 			2017-05						3450
-- 			2017-06						3037
-- 			2017-07						3752
-- 			2017-08						4057
-- 			2017-09						4004
-- 			2017-10						4328
-- 			2017-11						7059
-- 			2017-12						5338
-- 			2018-01						6842
-- 			2018-02						6288
-- 			2018-03						6774
-- 			2018-04						6582
-- 			2018-05						6506
-- 			2018-06						5875
-- 			2018-07						5946
-- 			2018-08						6144
SELECT  first_purchase_year_month
		,COUNT(*) AS customer_cnt
  FROM  olist_dm.vw_customer_first_purchase_month
 GROUP
    BY  1
 ORDER
    BY  1;

-- 2016-09, 2016-12 코호트 고객들의 delivered 주문 이력 확인
SELECT  fpm.customer_unique_id
      , vdo.order_id
      , vdo.order_purchase_date_key
      , vdo.year_month
      , vdo.order_purchase_dt
      , vdo.order_delivered_customer_dt
  FROM  olist_dm.vw_customer_first_purchase_month AS fpm
 INNER
  JOIN  olist_dm.vw_delivered_orders AS vdo
    ON  vdo.customer_id IN (
          SELECT dc.customer_id
            FROM olist_dm.dim_customer AS dc
           WHERE dc.customer_unique_id = fpm.customer_unique_id
      )
 WHERE  fpm.first_purchase_year_month IN ('2016-09', '2016-12')
 ORDER
    BY  fpm.customer_unique_id, vdo.order_purchase_date_key;



