/******************************************************************************************************************************************************/


/*
 * File: 02_am_vw_kpi_monthly_core.sql
 * Description:
 * 	- KPI 정의서에 명시된 월별 핵심 KPI를 표준화하여 제공하는 View
 * 	- BI/대시보드에서 직접 사용하는 월 단위 Core KPI 집계
 * 	- KPI 계산 기준을 고정하여 지표 일관성 확보
 * 
 * 포함 KPI:
 * 	- Gross Revenue (총 매출)
 *	- Order Count (주문 수)
 *	- Active Buyers (구매자 수)
 *	- AOV (Average Order Value)
 *	- Repeat Buyers (월 기준 반복 구매 고객 수)
 *	- Repeat Buyer Rate (월 기준 반복 구매 고객 비율)
 * 
 * Note:
 * 	- 해당 View는 Sparse View가 아닌 월 단위 Full 집계 View입니다.
 * 	- dim_date를 기준으로 year_month를 작성하였으며, 주문 완료된 건수가 없는 월은 0 또는 NULL로 표시됩니다.
 * 	- 매출/주문/구매자 KPI는 배송 완료 주문을 기준으로 집계하였습니다.
 * 	- repeat buyer는 해당 월 구매 고객 중 cohort_month < year_month인 고객 수로 정의하였습니다.
 * 	- repeat buyer rate는 repeat_buyers / active_buyers (0~1) 비율로 저장하였습니다.
 * 	- 월별 재구매율은 월 기준 repeat buyer 비중이며, 전체 기간 기준 재구매율과는 다릅니다.
 * 	- 전체 기간 기준 재구매율은 BI 툴에서 직접 계산으로 다루어야 합니다.
 */


/******************************************************************************************************************************************************/


USE olist_am;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_am.vw_kpi_monthly_core AS
WITH months AS (
	SELECT  DISTINCT dd.`year_month`
	  FROM  olist_dm.dim_date AS dd
),
monthly_base AS (
	SELECT  vbcmp.`year_month`
			,SUM(vbcmp.gross_revenue) AS gross_revenue
			,SUM(vbcmp.order_cnt) AS order_cnt
			,COUNT(DISTINCT vbcmp.customer_unique_id) AS active_buyers
			,COUNT(DISTINCT CASE WHEN vbcmp.is_repeat_buyer = 1 THEN vbcmp.customer_unique_id END) AS repeat_buyers
	  FROM  olist_am.vw_base_customer_monthly_purchase AS vbcmp
	 GROUP
	    BY  vbcmp.`year_month`
)
SELECT  m.`year_month`
		,COALESCE(b.gross_revenue, 0) AS gross_revenue
		,COALESCE(b.order_cnt, 0) AS order_cnt
		,COALESCE(b.active_buyers, 0) AS active_buyers
		,CAST(CASE WHEN COALESCE(b.order_cnt, 0) = 0 THEN NULL ELSE b.gross_revenue / b.order_cnt END AS DECIMAL(18,2)) AS aov
		,COALESCE(b.repeat_buyers, 0) AS repeat_buyers
		,CAST(CASE WHEN COALESCE(b.active_buyers, 0) = 0 THEN NULL ELSE b.repeat_buyers/b.active_buyers END AS DECIMAL(10,6)) AS repeat_buyer_rate
  FROM  months AS m
  LEFT
  JOIN  monthly_base AS b
    ON  b.`year_month` = m.`year_month`
 ORDER
    BY  m.`year_month`;


-- ===========================================================================================================================================


-- QC

-- Snapshot

-- year_month	gross_revenue	order_cnt		active_buyers		aov			repeat_buyers		repeat_buyer_rate
-- ===================================================================================================================================
-- 2016-08			0.00			0				0				NULL			0					NULL
-- 2016-09			143.46			1				1				143.46			0					0.0000
-- 2016-10			46490.66		265				262				175.44			0					0.0000
-- 2016-11			0.00			0				0				NULL			0					NULL
-- 2016-12			19.62			1				1				19.62			0					0.0000
-- 2017-01			127482.37		750				718				169.98			1					0.0014
-- 2017-02			271239.32		1653			1630			164.09			2					0.0012
-- 2017-03			414330.95		2546			2508			162.74			5					0.0020
-- 2017-04			390812.40		2303			2274			169.70			18					0.0079
-- 2017-05			566657.40		3545			3478			159.85			28					0.0081
-- 2017-06			490050.37		3135			3076			156.32			39					0.0127
-- 2017-07			566299.08		3872			3802			146.25			50					0.0132
-- 2017-08			645832.36		4193			4114			154.03			57					0.0139
-- 2017-09			701077.49		4150			4083			168.93			79					0.0193
-- 2017-10			751117.01		4478			4417			167.73			89					0.0201
-- 2017-11			1153229.37		7288			7182			158.24			123					0.0171
-- 2017-12			843078.29		5513			5450			152.93			112					0.0206
-- 2018-01			1077887.46		7069			6974			152.48			132					0.0189
-- 2018-02			966168.41		6555			6400			147.39			112					0.0175
-- 2018-03			1120598.24		7003			6914			160.02			140					0.0202
-- 2018-04			1132878.93		6798			6744			166.65			162					0.0240
-- 2018-05			1128774.52		6749			6693			167.25			187					0.0279
-- 2018-06			1011448.96		6096			6058			165.92			183					0.0302
-- 2018-07			1027286.52		6156			6097			166.88			151					0.0248
-- 2018-08			985491.64		6351			6310			155.17			166					0.0263
-- 2018-09			0.00			0				0				NULL			0					NULL
-- 2018-10			0.00			0				0				NULL			0					NULL
-- 2018-11			0.00			0				0				NULL			0					NULL
-- 2018-12			0.00			0				0				NULL			0					NULL

SELECT  *
  FROM  olist_am.vw_kpi_monthly_core;

-- 데이터 타입
DESCRIBE olist_am.vw_kpi_monthly_core;

-- 날짜 범위 -> 날짜 수(월 수): 29 / 날짜 범위: 2016-08 ~ 2018-12 (스냅샷 기준 배송 완료된 주문 dm 레이어 날짜 범위와 동일 / olist_dm.dim_date 날짜 범위와 동일)
SELECT  COUNT(*) AS month_rows
		,MIN(`year_month`) AS min_ym
		,MAX(`year_month`) AS max_ym
  FROM  olist_am.vw_kpi_monthly_core;

SELECT  COUNT(DISTINCT `year_month`) AS month_rows
		,MIN(`year_month`) AS min_ym
		,MAX(`year_month`) AS max_ym
  FROM  olist_dm.vw_delivered_orders;

SELECT  COUNT(DISTINCT `year_month`) AS month_rows
		,MIN(`year_month`) AS min_ym
		,MAX(`year_month`) AS max_ym
  FROM  olist_dm.dim_date;

-- gross_revenue 총합: 15,418,394.83 (배송 완료된 주문 dm 레이어 매출 총합과 동일)
SELECT  SUM(gross_revenue) AS am_sum_revenue
  FROM  olist_am.vw_kpi_monthly_core;

SELECT  CAST(SUM(item_total_value) AS DECIMAL(18,2)) AS dm_sum_revenue
  FROM  olist_dm.vw_delivered_order_items;

-- order_cnt 총합: 96,470 (배송 완료된 주문 dm 레이어 주문 수와 동일)
SELECT  SUM(order_cnt) AS am_sum_orders
  FROM  olist_am.vw_kpi_monthly_core;

SELECT  COUNT(DISTINCT order_id) AS dm_orders
  FROM  olist_dm.vw_delivered_orders;

-- 월별 매출 비교 -> 배송 완료된 주문 dm 레이어의 gross_revenue(SUM(item_total_value))와 다른 row: 0건
WITH dm AS (
	SELECT  `year_month`
			,CAST(SUM(item_total_value) AS DECIMAL(18,2)) AS revenue_dm
	  FROM  olist_dm.vw_delivered_order_items
	 GROUP
	    BY  `year_month`
)
SELECT  am.`year_month`
		,am.gross_revenue AS revenue_am
		,dm.revenue_dm
		,(am.gross_revenue - dm.revenue_dm) AS diff
  FROM  olist_am.vw_kpi_monthly_core AS am
  LEFT
  JOIN  dm
    ON  dm.`year_month` = am.`year_month`
 WHERE  am.gross_revenue <> COALESCE(dm.revenue_dm, 0)
 ORDER
    BY  am.`year_month`;

-- 월별 주문 수 비교 -> 배송 완료된 주문 dm 레이어의 order_cnt(COUNT(DISTINCT order_id))와 다른 row: 0건
WITH dm AS (
	SELECT  `year_month`
			,COUNT(DISTINCT order_id) AS orders_dm
	  FROM  olist_dm.vw_delivered_orders
	 GROUP
	    BY  `year_month`
)
SELECT  am.`year_month`
		,am.order_cnt AS orders_am
		,dm.orders_dm
		,(am.order_cnt - dm.orders_dm) AS diff
  FROM  olist_am.vw_kpi_monthly_core AS am
  LEFT
  JOIN  dm
    ON  dm.`year_month` = am.`year_month`
 WHERE  am.order_cnt <> COALESCE(dm.orders_dm, 0)
 ORDER
    BY  am.`year_month`;

-- Repeat Buyers 계산 검증 -> 배송 완료된 주문 dm 레이어의 repeat_buyers(COUNT(DISTINCT is_repeat_buyer=1))와 다른 row: 0건
WITH dm AS (
	SELECT  vbcmp.`year_month`
			,COUNT(DISTINCT CASE WHEN vbcmp.is_repeat_buyer = 1 THEN vbcmp.customer_unique_id END) AS repeat_buyers_dm
	  FROM  olist_am.vw_base_customer_monthly_purchase vbcmp 
	 GROUP
	    BY  vbcmp.`year_month`
)
SELECT  am.`year_month`
		,am.repeat_buyers AS repeat_buyers_am
		,dm.repeat_buyers_dm
		,(am.repeat_buyers - dm.repeat_buyers_dm) AS diff
  FROM  olist_am.vw_kpi_monthly_core AS am
  LEFT
  JOIN  dm
    ON  dm.`year_month` = am.`year_month`
 WHERE  am.repeat_buyers <> COALESCE(dm.repeat_buyers_dm, 0)
 ORDER
    BY  am.`year_month`;

-- 주요 KPI 결측치(정의 불가): aov: 6건 / repeat_buyer_rate: 6건 -> 6개월은 배송 완료 주문자가 없는 월
SELECT  SUM(gross_revenue IS NULL) AS null_revenue
		,SUM(order_cnt IS NULL) AS null_orders
		,SUM(active_buyers IS NULL) AS null_buyers
		,SUM(aov IS NULL) AS null_aov
		,SUM(repeat_buyers IS NULL) AS null_repeat_buyers
		,SUM(repeat_buyer_rate IS NULL) AS null_repeat_rate
  FROM  olist_am.vw_kpi_monthly_core;

-- 주요 KPI 이상치(음수 값): 0건
SELECT  SUM(gross_revenue < 0) AS neg_revenue
		,SUM(order_cnt < 0) AS neg_orders
		,SUM(active_buyers < 0) AS neg_buyers
		,SUM(aov < 0) AS neg_aov
		,SUM(repeat_buyers < 0) AS neg_repeat_buyers
		,SUM(repeat_buyer_rate < 0) AS neg_repeat_buyer_rate
  FROM  olist_am.vw_kpi_monthly_core;

-- 주요 KPI 값 범위: repeat_buyers와 repeat_buyer_rate는 0인 값이 있을 수 있음 / aov, repeat_buyer_rate를 제외한 kpi는 6개월의 0값이 있어야 함

-- kpi				zero_kpi_cnt		min_kpi			max_kpi
-- =======================================================================
-- gross_revenue		6				19.6200		 1153229.3700
-- order_cnt			6				1.0000		 7288.0000
-- active_buyers		6				1.0000		 7182.0000
-- aov					0				19.6200		 175.4400
-- repeat_buyers		9				0.0000		 187.0000
-- repeat_buyer_rate	3				0.0000		 0.0302

SELECT  'gross_revenue' AS kpi
		,SUM(gross_revenue = 0) AS zero_kpi_cnt
		,MIN(gross_revenue) AS min_kpi
		,MAX(gross_revenue) AS max_kpi
  FROM  olist_am.vw_kpi_monthly_core
  
UNION ALL

SELECT  'order_cnt' AS kpi
		,SUM(order_cnt = 0)
		,MIN(order_cnt)
		,MAX(order_cnt)
  FROM  olist_am.vw_kpi_monthly_core
  
UNION ALL
 
SELECT  'active_buyers' AS kpi
		,SUM(active_buyers = 0)
		,MIN(active_buyers)
		,MAX(active_buyers)
  FROM  olist_am.vw_kpi_monthly_core

UNION ALL

SELECT  'aov' AS kpi
		,SUM(aov = 0)
		,MIN(aov)
		,MAX(aov)
  FROM  olist_am.vw_kpi_monthly_core

UNION ALL

SELECT  'repeat_buyers' AS kpi
		,SUM(repeat_buyers = 0)
		,MIN(repeat_buyers)
		,MAX(repeat_buyers)
  FROM  olist_am.vw_kpi_monthly_core

UNION ALL

SELECT  'repeat_buyer_rate' AS kpi
		,SUM(repeat_buyer_rate = 0)
		,MIN(repeat_buyer_rate)
		,MAX(repeat_buyer_rate)
  FROM  olist_am.vw_kpi_monthly_core;

