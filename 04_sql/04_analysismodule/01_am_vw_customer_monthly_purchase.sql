/******************************************************************************************************************************************************/


/*
 * File: 01_am_vw_base_customer_monthly_purchase.sql
 * Description:
 * 	- 고객 기준 월별 구매 성과를 집계하는 Base View
 * 	- (customer_unique_id, year_month) 단위로 월별 주문 수, 주문 상품 수. 매출을 제공
 * 	- 코호트 분석 및 월차 리텐션 분석의 공통 기준 집계 뷰로 재사용
 *	- 이후 KPI View의 계산 기준 데이터로 활용
 * 	- Grain: 1 row = (customer_unique_id, year_month)
 * 
 * 코호트 기준 정의:
 * 	- cohort_year_month는 vw_customer_first_purchase_month의 first_purchase_year_month를 사용
 * 	- month_n = cohort_month 대비 경과 월 수 (0 = 첫 구매 월)
 * 
 * Note:
 * 	- 해당 View는 고객x월 단위의 기준 집계이며, 코호트 매트릭스/리텐션율 계산은 수행하지 않습니다.
 * 	- 대상 주문/매출은 배송 완료 기준(vw_delivered_orders, vw_delivered_order_items)을 사용하였습니다.
 * 	- 미구매 월을 포함하지 않는 sparse 구조입니다. (구매가 발생한 월만 row 생성)
 * 	- 따라서 구매 여부 컬럼(is_active)는 항상 True(1)입니다. (이후 dense 뷰로 확장할 예정)
 */


/******************************************************************************************************************************************************/


USE olist_am;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_am.vw_base_customer_monthly_purchase AS
WITH base_orders AS (
	SELECT  dc.customer_unique_id
			,dd.`year_month`
			,COUNT(DISTINCT vdo.order_id) AS order_cnt
	  FROM  olist_dm.vw_delivered_orders AS vdo
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdo.customer_id
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = vdo.order_purchase_date_key
	 GROUP
	    BY  dc.customer_unique_id
	    	,dd.`year_month`
),
base_items AS (
	SELECT  dc.customer_unique_id
			,dd.`year_month`
			,COUNT(*) AS item_cnt
			,CAST(SUM(vdoi.item_total_value) AS DECIMAL(18,2)) AS gross_revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = vdoi.order_purchase_date_key
	 GROUP
	    BY  dc.customer_unique_id
	    	,dd.`year_month`
)
SELECT  o.customer_unique_id
		,o.`year_month`
		,cfpm.first_purchase_year_month AS cohort_year_month
		,TIMESTAMPDIFF(MONTH, STR_TO_DATE(CONCAT(cfpm.first_purchase_year_month, '-01'), '%Y-%m-%d'), STR_TO_DATE(CONCAT(o.`year_month`, '-01'), '%Y-%m-%d')) AS month_n
		,o.order_cnt
		,COALESCE(i.item_cnt, 0) AS item_cnt
		,COALESCE(i.gross_revenue, 0.00) AS gross_revenue
		,CASE WHEN o.order_cnt > 0 THEN 1 ELSE 0 END AS is_active
		,CASE WHEN o.`year_month` = cfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_new_buyer
		,CASE WHEN o.`year_month` > cfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_repeat_buyer
  FROM  base_orders AS o
  LEFT
  JOIN  base_items AS i
    ON  i.customer_unique_id = o.customer_unique_id
   AND  i.`year_month` = o.`year_month`
  JOIN  olist_dm.vw_customer_first_purchase_month AS cfpm
    ON  cfpm.customer_unique_id = o.customer_unique_id;


-- ===========================================================================================================================================


-- QC

-- 샘플
SELECT  *
  FROM  olist_am.vw_base_customer_monthly_purchase
 LIMIT  50;

-- 데이터 타입
DESCRIBE olist_am.vw_base_customer_monthly_purchase;

-- row count: 95,186행
SELECT  COUNT(*)
  FROM  olist_am.vw_base_customer_monthly_purchase;

-- PK 유니크 확인 -> row_cnt: 95,186 / distinct_cnt: 95,186 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS row_cnt
		,COUNT(DISTINCT CONCAT(customer_unique_id, '_', `year_month`)) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT CONCAT(customer_unique_id, '_', `year_month`)) AS dup_cnt
		,SUM(customer_unique_id IS NULL OR customer_unique_id = '') AS blank_customer_cnt
		,SUM(`year_month` IS NULL OR `year_month` = '') AS blank_year_month_cnt
  FROM  olist_am.vw_base_customer_monthly_purchase;

-- 주요 컬럼 결측 확인: 0건
SELECT  SUM(cohort_year_month IS NULL) AS null_cohort_year_mnth
		,SUM(month_n IS NULL) AS null_month_n
		,SUM(order_cnt IS NULL) AS null_order_cnt
  FROM  olist_am.vw_base_customer_monthly_purchase;

-- 주문은 있는데 아이템/매출이 0인 row: 0건
SELECT  COUNT(*) AS suspicious_cnt
  FROM  olist_am.vw_base_customer_monthly_purchase
 WHERE  order_cnt > 0
   AND  item_cnt = 0
   AND  gross_revenue = 0;

-- 음수/이상치 확인: 0건
SELECT  SUM(order_cnt < 0) AS neg_order_cnt
		,SUM(item_cnt < 0) AS neg_item_cnt
		,SUM(gross_revenue < 0) AS neg_gross_revenue
		,SUM(month_n < 0) AS neg_month_n
  FROM  olist_am.vw_base_customer_monthly_purchase;



-- 플래그 정합성(1) -> is_active가 1이 아닌 row: 0건
SELECT  is_active
		,COUNT(*) AS cnt
  FROM  olist_am.vw_base_customer_monthly_purchase
 GROUP
    BY  is_active;

-- 플래그 정합성(2) -> is_new_buyer와 is_repeat_buyer가 모두 1인 row: 0건
SELECT  COUNT(*) AS invalid_cnt
  FROM  olist_am.vw_base_customer_monthly_purchase
 WHERE  is_new_buyer = 1
   AND  is_repeat_buyer = 1;

-- 플래그 정합성(3) -> 신규 고객(is_new_buyer)이지만 month_n이 0이 아닌 row: 0건
SELECT  COUNT(*) AS invalid_cnt
  FROM  olist_am.vw_base_customer_monthly_purchase
 WHERE  is_new_buyer = 1
   AND  month_n <> 0;

-- year_month가 cohort_year_month보다 과거인 row: 0건
SELECT  COUNT(*) AS invalid_cnt
  FROM  olist_am.vw_base_customer_monthly_purchase
 WHERE  `year_month` < cohort_year_month;

-- vw_delivered_orders와의 비교 -> 월별 주문 수 차이: 없음
WITH src AS (
	SELECT  dc.customer_unique_id
			,dd.`year_month`
			,COUNT(DISTINCT vdo.order_id) AS order_cnt_src
	  FROM  olist_dm.vw_delivered_orders AS vdo
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdo.customer_id
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = vdo.order_purchase_date_key
	 GROUP
	    BY  dc.customer_unique_id
	    	,dd.`year_month`
)
SELECT  vcmp.customer_unique_id
		,vcmp.`year_month`
		,vcmp.order_cnt AS order_cnt_view
		,s.order_cnt_src
		,(vcmp.order_cnt - s.order_cnt_src) AS diff
  FROM  olist_am.vw_base_customer_monthly_purchase vcmp
  JOIN  src AS s
    ON  s.customer_unique_id = vcmp.customer_unique_id
   AND  s.`year_month` = vcmp.`year_month`
 WHERE  vcmp.order_cnt <> s.order_cnt_src
 LIMIT  50;

-- vw_delivered_order_items와의 비교 -> 월별 매출/아이템 수 차이: 없음
WITH src AS (
	SELECT  dc.customer_unique_id
			,dd.`year_month`
			,COUNT(*) AS item_cnt_src
			,CAST(SUM(vdoi.item_total_value) AS DECIMAL(18,2)) AS gross_revenue_src
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = vdoi.order_purchase_date_key
	 GROUP
	    BY  dc.customer_unique_id
	    	,dd.`year_month`
)
SELECT  vcmp.customer_unique_id
		,vcmp.`year_month`
		,vcmp.item_cnt AS item_cnt_view
		,s.item_cnt_src
		,(vcmp.item_cnt - s.item_cnt_src) AS item_diff
		,vcmp.gross_revenue AS gross_revenue_view
		,s.gross_revenue_src
		,(vcmp.gross_revenue - s.gross_revenue_src ) AS revenue_diff
  FROM  olist_am.vw_base_customer_monthly_purchase AS vcmp
  JOIN  src AS s
    ON  s.customer_unique_id = vcmp.customer_unique_id
   AND  s.`year_month` = vcmp.`year_month`
 WHERE  vcmp.item_cnt <> s.item_cnt_src
    OR  vcmp.gross_revenue <> s.gross_revenue_src
 LIMIT  50;





