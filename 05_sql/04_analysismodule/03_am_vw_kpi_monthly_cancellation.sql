/******************************************************************************************************************************************************/


/*
 * File: 03_am_vw_kpi_monthly_cancellation.sql
 * Description:
 * 	- KPI 정의서에 명시된 월별 주문 취소 관련 KPI를 표준화하여 제공하는 View
 * 	- BI/대시보드에서 직접 사용하는 월 단위 Cancellation KPI 집계
 * 	- 주문 프로세스 안정성 및 거래 실패 비율을 시계열 관점에서 분석하기 위한 기준 데이터
 * 
 * 포함 KPI:
 * 	- Total Orders (전체 주문 수)
 * 	- Canceled Orders (취소 주문 수)
 * 	- Unavailable Orders (미수/거래 실패 주문 수)
 * 	- Failed Orders (취소 + 미수 주문 수)
 * 	- Cancel Rate (취소율)
 * 	- Unavailable Rate (미수율)
 * 	- Failed Rate (취소+미수율)
 * 
 * Note:
 * 	- 해당 View는 Sparse View가 아닌 월 단위 Full 집계 View입니다.
 * 	- dim_date를 기준으로 year_month를 작성하였으며, 주문 완료된 건수가 없는 월은 0 또는 NULL로 표시됩니다.
 * 	- 전체 주문은 배송 완료 필터를 적용하지 않았습니다.
 * 	- 취소 주문은 order_status = 'canceled' 기준 / 미수 주문은 order_status = 'unavailable' 기준을 적용했습니다.
 * 	- 참고용 지표로 두 경우를 모두 포함한 failed orders를 작성하였습니다.
 * 	- 각 비율 KPI는 상태별 주문 수 / 전체 주문 수 (0~1)로 계산하였습니다.
 * 	- 원본 데이터 초기와 말미(2016-08~09/2018-09~12)는 표본 수가 매우 작을 수 있어 비율 KPI가 급격히 변동될 수 있으므로 해석 시 주의가 필요합니다.
 */


/******************************************************************************************************************************************************/


USE olist_am;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_am.vw_kpi_monthly_cancellation AS
WITH months AS (
	SELECT  DISTINCT dd.`year_month`
	  FROM  olist_dm.dim_date AS dd
), cnt AS (
SELECT  d.`year_month`
		,COUNT(DISTINCT o.order_id) AS total_orders
		,COUNT(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END) AS canceled_orders
		,COUNT(DISTINCT CASE WHEN order_status = 'unavailable' THEN order_id END) AS unavailable_orders
		,COUNT(DISTINCT CASE WHEN order_status IN ('canceled', 'unavailable') THEN order_id END) AS failed_orders
  FROM  olist_dm.fact_orders AS o
  JOIN  olist_dm.dim_date AS d
    ON  d.date_key = o.order_purchase_date_key
 GROUP
    BY  d.`year_month`
 )
 SELECT  m.`year_month`
 		,COALESCE(c.total_orders, 0) AS total_orders
 		,COALESCE(c.canceled_orders, 0) AS canceled_orders
 		,COALESCE(c.unavailable_orders, 0) AS unavailable_orders
 		,COALESCE(c.failed_orders, 0) AS failed_orders
 		,CAST(CASE WHEN COALESCE(c.total_orders, 0) = 0 THEN NULL ELSE c.canceled_orders/c.total_orders END AS DECIMAL(10,6)) AS cancel_rate
 		,CAST(CASE WHEN COALESCE(c.total_orders, 0) = 0 THEN NULL ELSE c.unavailable_orders/c.total_orders END AS DECIMAL(10,6)) AS unavailable_rate
 		,CAST(CASE WHEN COALESCE(c.total_orders, 0) = 0 THEN NULL ELSE c.failed_orders/c.total_orders END AS DECIMAL(10,6)) AS failed_rate
   FROM  months AS m
   LEFT
   JOIN  cnt AS c
     ON  c.`year_month` = m.`year_month`
  ORDER
     BY  m.`year_month`;


-- ===========================================================================================================================================


-- QC

-- Snapshot

-- year_month	total_orders	canceled_orders		unavailable_orders		failed_orders		cancel_rate		unavailable_rate	failed_rate
-- ====================================================================================================================================================
-- 2016-08			0					0					0					  0					NULL			  NULL				NULL		
-- 2016-09			4					2					0					  2				  0.500000			   0			  0.500000
-- 2016-10		   324					24					7					  31			  0.074074		    0.021605		  0.095679
-- 2016-11			0					0					0					  0					NULL			  NULL				NULL
-- 2016-12			1					0					0					  0				  0.000000			0.000000		  0.000000
-- 2017-01			800					3					10					  13			  0.003750			0.012500		  0.016250
-- 2017-02			1780				17					45					  62			  0.009551			0.025281		  0.034831
-- 2017-03			2682				33					32					  65			  0.012304			0.011931		  0.024236
-- 2017-04			2404				18					9					  27			  0.007488			0.003744		  0.011231
-- 2017-05			3700				29					31					  60			  0.007838			0.008378		  0.016216
-- 2017-06			3245				16					24					  40			  0.004931			0.007396		  0.012327
-- 2017-07			4026				28					52					  80			  0.006955			0.012916		  0.019871
-- 2017-08			4331				27					32					  59			  0.006234			0.007389		  0.013623
-- 2017-09			4285				20					38					  58			  0.004667			0.008868		  0.013536
-- 2017-10			4631				26					58					  84			  0.005614			0.012524		  0.018139
-- 2017-11			7544				37					84					  121			  0.004905			0.011135		  0.016039
-- 2017-12			5673				11					42					  53			  0.001939			0.007403		  0.009342
-- 2018-01			7269				34					48					  82			  0.004677			0.006603		  0.011281
-- 2018-02			6728				73					30					  103			  0.010850			0.004459		  0.015309
-- 2018-03			7211				26					17					  43			  0.003606			0.002358		  0.005963
-- 2018-04			6939				15					5					  20			  0.002162			0.000721		  0.002882
-- 2018-05			6873				24					16					  40			  0.003492			0.002328		  0.005820
-- 2018-06			6167				18					4					  22			  0.002919			0.000649		  0.003567
-- 2018-07			6292				41					18					  59			  0.006516			0.002861		  0.009377
-- 2018-08			6512				84					7					  91			  0.012899			0.001075		  0.013874
-- 2018-09			16					15					0					  15			  0.937500			0.000000		  0.937500
-- 2018-10			4					4					0					  4				  1.000000			0.000000		  1.000000
-- 2018-11			0					0					0					  0				    NULL			  NULL				NULL
-- 2018-12			0					0					0					  0					NULL			  NULL				NULL

SELECT  *
  FROM  olist_am.vw_kpi_monthly_cancellation;

-- 데이터 타입
DESCRIBE olist_am.vw_kpi_monthly_cancellation;

-- 날짜 범위 -> 날짜 수(월 수): 29 / 날짜 범위: 2016-08 ~ 2018-12 (olist_dm.dim_date 날짜 범위와 동일)
SELECT  COUNT(*) AS month_rows
		,MIN(`year_month`) AS min_ym
		,MAX(`year_month`) AS max_ym
  FROM  olist_am.vw_kpi_monthly_cancellation;

SELECT  COUNT(DISTINCT `year_month`) AS month_rows
		,MIN(`year_month`) AS min_ym
		,MAX(`year_month`) AS max_ym
  FROM  olist_dm.dim_date;

-- total_orders 총합: 99,441 (fact_orders의 총 주문 수와 동일)
SELECT  SUM(total_orders) AS am_total_orders
  FROM  olist_am.vw_kpi_monthly_cancellation;

SELECT  COUNT(DISTINCT order_id) AS dm_total_orders
  FROM  olist_dm.fact_orders;

-- 월별 취소 주문 총합: 625건 (fact_orders의 취소 주문 총합과 동일)
SELECT  SUM(canceled_orders) AS am_canceled_orders
  FROM  olist_am.vw_kpi_monthly_cancellation;

SELECT  COUNT(DISTINCT CASE WHEN order_status = 'canceled' THEN order_id END) AS dm_canceled_orders
  FROM  olist_dm.fact_orders;

-- 월별 unavailable 주문 총합: 609건 (fact_orders의 unavailable 주문 총합과 동일)
SELECT  SUM(unavailable_orders) AS am_unavailable_orders
  FROM  olist_am.vw_kpi_monthly_cancellation;

SELECT  COUNT(DISTINCT CASE WHEN order_status = 'unavailable' THEN order_id END) AS dm_unavailable_orders
  FROM  olist_dm.fact_orders;

-- 월별 취소/미수 주문 총합: 1,234건 (fact_orders의 취소/미수 주문 총합과 동일])
SELECT  SUM(failed_orders) AS am_failed_orders
  FROM  olist_am.vw_kpi_monthly_cancellation;

SELECT  COUNT(DISTINCT CASE WHEN order_status IN ('canceled', 'unavailable') THEN order_id END) AS dm_failed_orders
  FROM  olist_dm.fact_orders;

-- KPI 값 범위

-- kpi					min_kpi			max_kpi
-- =======================================================
-- total_orders			0.000000	  7544.000000
-- canceled_orders		0.000000	   84.000000
-- unavailable_orders	0.000000	   84.000000
-- failed_orders		0.000000	  121.000000
-- canceled_rate		0.000000	   1.000000
-- unavailable_rate		0.000000	   0.025281
-- failed_rate			0.000000	   1.000000

SELECT  'total_orders' AS kpi
		,MIN(total_orders) AS min_kpi
		,MAX(total_orders) AS max_kpi
  FROM  olist_am.vw_kpi_monthly_cancellation

UNION ALL

SELECT  'canceled_orders' AS kpi
		,MIN(canceled_orders)
		,MAX(canceled_orders)
  FROM  olist_am.vw_kpi_monthly_cancellation

UNION ALL

SELECT  'unavailable_orders' AS kpi
		,MIN(unavailable_orders)
		,MAX(unavailable_orders)
  FROM  olist_am.vw_kpi_monthly_cancellation

UNION ALL

SELECT  'failed_orders' AS kpi
		,MIN(failed_orders)
		,MAX(failed_orders)
  FROM  olist_am.vw_kpi_monthly_cancellation

UNION ALL

SELECT  'canceled_rate' AS kpi
		,MIN(cancel_rate)
		,MAX(cancel_rate)
  FROM  olist_am.vw_kpi_monthly_cancellation

UNION ALL

SELECT  'unavailable_rate' AS kpi
		,MIN(unavailable_rate)
		,MAX(unavailable_rate)
  FROM  olist_am.vw_kpi_monthly_cancellation

UNION ALL

SELECT  'failed_rate' AS kpi
		,MIN(failed_rate)
		,MAX(failed_rate)
  FROM  olist_am.vw_kpi_monthly_cancellation;

-- failed_orders 논리 확인 -> failed_orders의 order_status가 canceled나 unavailable이 아닌 row: 0건
SELECT  `year_month`
		,canceled_orders
		,unavailable_orders
		,failed_orders
		,(canceled_orders + unavailable_orders - failed_orders) AS diff
  FROM  olist_am.vw_kpi_monthly_cancellation
 WHERE  (canceled_orders + unavailable_orders) <> failed_orders
 ORDER
    BY  `year_month`;


