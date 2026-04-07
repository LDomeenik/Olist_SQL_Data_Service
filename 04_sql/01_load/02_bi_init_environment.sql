/****************************************************************************************************************************************/


/*
 * File: 04_adhoc_operational_stability.sql
 * Description:
 * 	- 전체 주문 기준 플랫폼의 거래 안정성 분석
 * 	- 월별 거래 안정성 추이, 주문 상태 분포, 카테고리별 취소 위험, 지역별 취소 위험을 통해 취소 및 실패 주문이 구조적으로 집중되는지 여부를 진단
 * 
 * Notes:
 * 	- 거래 안정성 분석은 Delivered 주문이 아닌 전체 주문을 기준으로 진행하였습니다.
 * 	- 핵심 지표는 cancel_rate, unavailable_rate, failed_rate(cancel + unavailable)입니다.
 * 	- 분석 기간은 왜곡 방지를 위해 2017-01 ~ 2018-08로 제한하였습니다.
 * 	- 카테고리 분석은 최소 주문수를 100 이상으로 설정하였습니다. (지나치게 미미한 영향의 카테고리 제외 목적)
 */


/****************************************************************************************************************************************/


-- BI용 스케마 생성
CREATE SCHEMA IF NOT EXISTS olist_bi;


-- growth_structure 생성
CREATE OR REPLACE VIEW olist_bi.vw_growth_structure AS
WITH base AS (
	SELECT  `year_month`
			,gross_revenue
			,order_cnt
			,active_buyers
			,aov
			,repeat_buyers
			,repeat_buyer_rate
			,ROUND(gross_revenue / NULLIF(active_buyers, 0), 2) AS arpb
			,ROUND(order_cnt / NULLIF(active_buyers, 0), 4) AS orders_per_buyer
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  `year_month`
		,gross_revenue
		,order_cnt
		,active_buyers
		,aov
		,repeat_buyers
		,repeat_buyer_rate
		,arpb
		,orders_per_buyer
		,ROUND((gross_revenue - LAG(gross_revenue) OVER (ORDER BY `year_month`)) / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0), 6) AS mom_gross_revenue
		,ROUND((order_cnt - LAG(order_cnt) OVER (ORDER BY `year_month`)) / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0), 6) AS mom_order_cnt
		,ROUND((active_buyers - LAG(active_buyers) OVER (ORDER BY `year_month`)) / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0), 6) AS mom_active_buyers
		,ROUND((aov - LAG(aov) OVER (ORDER BY `year_month`)) / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0), 6) AS mom_aov
		,ROUND((arpb - LAG(arpb) OVER (ORDER BY `year_month`)) / NULLIF(LAG(arpb) OVER (ORDER BY `year_month`), 0), 6) AS mom_arpb
		,ROUND((orders_per_buyer - LAG(orders_per_buyer) OVER (ORDER BY `year_month`)) / NULLIF(LAG(orders_per_buyer) OVER (ORDER BY `year_month`), 0), 6) AS mom_orders_per_buyer
  FROM  base;

-- 검증
SELECT  COUNT(*) AS cnt
  FROM  olist_bi.vw_growth_structure;


-- growth_drill_down 생성
CREATE OR REPLACE VIEW olist_bi.vw_growth_drill_down AS
WITH base_customer AS (
	SELECT  `year_month`
			,CASE WHEN is_new_buyer = 1 THEN 'new'
				  WHEN is_repeat_buyer = 1 THEN 'repeat'
				  ELSE 'other'
			 END AS buyer_type
			,COUNT(DISTINCT customer_unique_id) AS buyers
			,SUM(order_cnt) AS order_cnt
			,SUM(item_cnt) AS item_cnt
			,SUM(gross_revenue) AS gross_revenue
	  FROM  olist_am.vw_base_customer_monthly_purchase
	 WHERE  `year_month` IN ('2017-11', '2017-12')
	 GROUP
	    BY  `year_month`
	    	,buyer_type
),
base_category AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dp.product_category_name_en, 'unknown') AS dimension_value
			,COUNT(DISTINCT vdoi.order_id) AS order_cnt
			,COUNT(*) AS item_cnt
			,SUM(vdoi.item_total_value) AS gross_revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  LEFT
	  JOIN  olist_dm.dim_product AS dp
	    ON  dp.product_id = vdoi.product_id
	 WHERE  vdoi.`year_month` IN ('2017-11', '2017-12')
	 GROUP
	    BY  vdoi.`year_month`
	    	,dimension_value
),
base_city AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dc.customer_city_state, 'unknown') AS dimension_value
			,COUNT(DISTINCT vdoi.order_id) AS order_cnt
			,COUNT(DISTINCT dc.customer_unique_id) AS buyers
			,COUNT(*) AS item_cnt
			,SUM(vdoi.item_total_value) AS gross_revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  LEFT
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	 WHERE  vdoi.`year_month` IN ('2017-11', '2017-12')
	 GROUP
	    BY  vdoi.`year_month`
	    	,dimension_value
)
SELECT  'category' AS section_type
		,`year_month`
		,dimension_value
		,NULL AS buyer_type
		,NULL AS buyers
		,order_cnt
		,item_cnt
		,ROUND(gross_revenue, 2) AS gross_revenue
  FROM  base_category

UNION ALL

SELECT  'city_state' AS section_type
		,`year_month`
		,dimension_value
		,NULL AS buyer_type
		,buyers
		,order_cnt
		,item_cnt
		,ROUND(gross_revenue, 2) AS gross_revenue
  FROM  base_city

UNION ALL

SELECT  'buyer_type' AS section_type
		,`year_month`
		,buyer_type AS dimension_value
		,buyer_type
		,buyers
		,order_cnt
		,item_cnt
		,ROUND(gross_revenue, 2) AS gross_revenue
  FROM  base_customer;

-- 검증
SELECT  section_type
		,`year_month`
		,COUNT(*)
  FROM  olist_bi.vw_growth_drill_down
 GROUP
    BY  section_type
    	,`year_month`
 ORDER
    BY  section_type
    	,`year_month`;


-- customer_value_structure 생성
CREATE OR REPLACE VIEW olist_bi.vw_customer_value_structure AS
WITH customer_month AS (
	SELECT  `year_month`
			,customer_unique_id
			,cohort_year_month
			,month_n
			,order_cnt
			,gross_revenue
			,is_new_buyer
			,is_repeat_buyer
	  FROM  olist_am.vw_base_customer_monthly_purchase
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
),
monthly_value AS (
	SELECT  `year_month`
			,SUM(gross_revenue) AS gross_revenue
			,COUNT(DISTINCT customer_unique_id) AS active_buyers
			,SUM(order_cnt) AS order_cnt
			,ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT customer_unique_id), 0), 2) AS arpb
			,ROUND(SUM(order_cnt) / NULLIF(COUNT(DISTINCT customer_unique_id), 0), 4) AS orders_per_buyer
			,ROUND(SUM(gross_revenue) / NULLIF(SUM(order_cnt), 0), 2) AS aov
	  FROM  customer_month
	 GROUP
	    BY  `year_month`
),
new_repeat AS (
	SELECT  `year_month`
			,CASE WHEN is_new_buyer = 1 THEN 'new'
				  WHEN is_repeat_buyer = 1 THEN 'repeat'
				  ELSE 'other'
			 END AS sub_type
			,COUNT(DISTINCT customer_unique_id) AS buyers
			,SUM(order_cnt) AS order_cnt
			,SUM(gross_revenue) AS gross_revenue
	  FROM  customer_month
	 GROUP
	    BY  `year_month`
	    	,sub_type
),
decile_base AS (
	SELECT  `year_month`
			,customer_unique_id
			,gross_revenue
			,NTILE(10) OVER (PARTITION BY `year_month` ORDER BY gross_revenue DESC) AS decile
	  FROM  customer_month
),
decile_share AS (
	SELECT  `year_month`
			,CONCAT('decile_', decile) AS sub_type
			,COUNT(DISTINCT customer_unique_id) AS buyers
			,SUM(gross_revenue) AS gross_revenue
	  FROM  decile_base
	 GROUP
	    BY  `year_month`
	    	,decile
),
cohort_size AS (
	SELECT  cohort_year_month
			,COUNT(DISTINCT customer_unique_id) AS cohort_size
	  FROM  customer_month
	 WHERE  month_n = 0
	 GROUP
	    BY  cohort_year_month
),
cohort_retention AS (
	SELECT  cm.cohort_year_month
			,cm.`year_month`
			,cm.month_n
			,cs.cohort_size
			,COUNT(DISTINCT cm.customer_unique_id) AS cohort_active_buyers
			,SUM(cm.order_cnt) AS cohort_order_cnt
			,SUM(cm.gross_revenue) AS cohort_gross_revenue
	  FROM  customer_month AS cm
	  JOIN  cohort_size AS cs
	    ON  cs.cohort_year_month = cm.cohort_year_month
	 GROUP
	    BY  cm.cohort_year_month
	    	,cm.`year_month`
	    	,cm.month_n
	    	,cs.cohort_size
)
SELECT  'monthly_value' AS section_type
		,`year_month`
		,NULL AS cohort_year_month
		,NULL AS month_n
		,'all' AS sub_type
		,active_buyers AS buyers
		,order_cnt
		,ROUND(gross_revenue, 2) AS gross_revenue
		,arpb
		,orders_per_buyer
		,aov
		,NULL AS revenue_share
		,NULL AS retention_rate
  FROM  monthly_value
  
UNION ALL
  
SELECT  'new_repeat_share' AS section_type
		,`year_month`
		,NULL AS cohort_year_month
		,NULL AS month_n
		,sub_type
		,buyers
		,order_cnt
		,ROUND(gross_revenue, 2) AS gross_revenue
		,NULL AS arpb
		,NULL AS orders_per_buyer
		,NULL AS aov
		,ROUND(gross_revenue / NULLIF(SUM(gross_revenue) OVER (PARTITION BY `year_month`), 0), 6) AS revenue_share
		,NULL AS retention_rate
  FROM  new_repeat

UNION ALL

SELECT  'decile_share' AS section_type
		,`year_month`
		,NULL AS cohort_year_month
		,NULL AS month_n
		,sub_type
		,buyers
		,NULL AS order_cnt
		,ROUND(gross_revenue, 2) AS gross_revenue
		,NULL AS arpb
		,NULL AS orders_per_buyer
		,NULL AS aov
		,ROUND(gross_revenue / NULLIF(SUM(gross_revenue) OVER (PARTITION BY `year_month`), 0), 6) AS revenue_share
		,NULL AS retention_rate
  FROM  decile_share

UNION ALL


SELECT  'cohort_retention' AS section_type
		,`year_month`
		,cohort_year_month
		,month_n
		,'cohort' AS sub_type
		,cohort_active_buyers AS buyers
		,cohort_order_cnt AS order_cnt
		,ROUND(cohort_gross_revenue, 2) AS gross_revenue
		,NULL AS arpb
		,NULL AS orders_per_buyer
		,NULL AS aov
		,NULL AS revenue_share
		,ROUND(cohort_active_buyers / NULLIF(cohort_size, 0), 6) AS retention_rate
  FROM  cohort_retention;

-- 검증
SELECT  section_type
		,COUNT(*)
  FROM  olist_bi.vw_customer_value_structure
 GROUP
    BY  section_type;

SELECT  *
  FROM  olist_bi.vw_customer_value_structure
 WHERE  section_type = 'cohort_retention'
 ORDER
    BY  cohort_year_month
    	,month_n;


-- operational_stability 생성
CREATE OR REPLACE VIEW olist_bi.vw_operational_stability AS
WITH monthly_kpi AS (
	SELECT  `year_month`
			,total_orders
			,canceled_orders
			,unavailable_orders
			,failed_orders
			,cancel_rate
			,unavailable_rate
			,failed_rate
	  FROM  olist_am.vw_kpi_monthly_cancellation
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
),
status_dist AS (
	SELECT  dd.`year_month`
			,fo.order_status
			,COUNT(*) AS order_cnt
	  FROM  olist_dm.fact_orders AS fo
	 INNER
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = fo.order_purchase_date_key
	 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
	 GROUP
	    BY  dd.`year_month`
	    	,fo.order_status
),
order_category AS (
	SELECT  DISTINCT dd.`year_month`
			,fo.order_id
			,fo.order_status
			,COALESCE(dp.product_category_name_en, 'unknown') AS dimension_value
	  FROM  olist_dm.fact_orders AS fo
	 INNER
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = fo.order_purchase_date_key
	 INNER
	  JOIN  olist_dm.fact_order_items AS foi
	    ON  foi.order_id = fo.order_id
	  LEFT
	  JOIN  olist_dm.dim_product AS dp
	    ON  dp.product_id = foi.product_id
	 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
),
category_cancel_risk AS (
	SELECT  `year_month`
			,dimension_value
			,COUNT(DISTINCT order_id) AS total_orders
			,COUNT(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END) AS canceled_orders
			,COUNT(DISTINCT CASE WHEN order_status = 'unavailable' THEN order_id END) AS unavailable_orders
			,COUNT(DISTINCT CASE WHEN order_status IN ('canceled', 'unavailable') THEN order_id END) AS failed_orders
            ,ROUND(COUNT(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END) / NULLIF(COUNT(DISTINCT order_id), 0), 6) AS cancel_rate
            ,ROUND(COUNT(DISTINCT CASE WHEN order_status = 'unavailable' THEN order_id END) / NULLIF(COUNT(DISTINCT order_id), 0), 6) AS unavailable_rate
            ,ROUND(COUNT(DISTINCT CASE WHEN order_status IN ('canceled', 'unavailable') THEN order_id END) / NULLIF(COUNT(DISTINCT order_id), 0), 6) AS failed_rate
	  FROM  order_category
	 GROUP
	    BY  `year_month`
	    	,dimension_value
),
city_cancel_risk AS (
	SELECT  dd.`year_month`
			,COALESCE(dc.customer_city_state, 'unknown') AS dimension_value
			,COUNT(DISTINCT fo.order_id) AS total_orders
			,COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN fo.order_id END) AS canceled_orders
            ,COUNT(DISTINCT CASE WHEN fo.order_status = 'unavailable' THEN fo.order_id END) AS unavailable_orders
            ,COUNT(DISTINCT CASE WHEN fo.order_status IN ('canceled', 'unavailable') THEN fo.order_id END) AS failed_orders
            ,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN fo.order_id END) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 6) AS cancel_rate
            ,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'unavailable' THEN fo.order_id END) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 6) AS unavailable_rate
            ,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status IN ('canceled', 'unavailable') THEN fo.order_id END) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 6) AS failed_rate
	  FROM  olist_dm.fact_orders AS fo
	 INNER
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = fo.order_purchase_date_key
	  LEFT
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = fo.customer_id
	 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
	 GROUP
	    BY  dd.`year_month`
	    	,COALESCE(dc.customer_city_state, 'unknown')
)
SELECT  'monthly_kpi' AS row_type
		,`year_month`
		,NULL AS order_status
		,NULL AS dimension_group
		,NULL AS dimension_value
		,total_orders
		,canceled_orders
		,unavailable_orders
		,failed_orders
		,cancel_rate
		,unavailable_rate
		,failed_rate
		,NULL AS status_order_cnt
		,NULL AS status_order_share
  FROM  monthly_kpi

UNION ALL

SELECT  'order_status_share' AS row_type
		,`year_month`
		,order_status
		,NULL AS dimension_group
		,NULL AS dimension_value
		,NULL AS total_orders
		,NULL AS canceled_orders
		,NULL AS unavailable_orders
		,NULL AS failed_orders
		,NULL AS cancel_rate
		,NULL AS unavailable_rate
		,NULL AS failed_rate
		,order_cnt AS status_order_cnt
		,ROUND(order_cnt / NULLIF(SUM(order_cnt) OVER (PARTITION BY `year_month`), 0), 6) AS status_order_share
  FROM  status_dist

UNION ALL

SELECT  'dimension_risk' AS row_type
		,`year_month`
		,NULL AS order_status
		,'category' AS dimension_group
		,dimension_value
		,total_orders
		,canceled_orders
		,unavailable_orders
		,failed_orders
		,cancel_rate
		,unavailable_rate
		,failed_rate
		,NULL AS status_order_cnt
		,NULL AS status_order_share
  FROM  category_cancel_risk

UNION ALL

SELECT  'dimension_risk' AS row_type
		,`year_month`
		,NULL AS order_status
		,'city_state' AS dimension_group
		,dimension_value
		,total_orders
		,canceled_orders
		,unavailable_orders
		,failed_orders
		,cancel_rate
		,unavailable_rate
		,failed_rate
		,NULL AS status_order_cnt
		,NULL AS status_order_share
  FROM  city_cancel_risk;

-- 검증
SELECT  row_type
		,COALESCE(dimension_group, 'none') AS dimension_group
		,COUNT(*) AS row_cnt
  FROM  olist_bi.vw_operational_stability
 GROUP
    BY  row_type
    	,dimension_group
 ORDER
    BY  row_type
    	,dimension_group;







