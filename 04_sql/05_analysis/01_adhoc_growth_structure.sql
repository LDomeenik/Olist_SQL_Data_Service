/****************************************************************************************************************************************/


/*
 * File: 01_adhoc_growth_structure.sql
 * Description:
 * 	- Delivered 주문 기준 월별 핵심 KPI(gross_revenue, order_cnt, active_buyers, aov, repeat_buyer_rate)를 시계열 분석
 * 	- 성장 상태(급등/급락) 구간을 탐지하고, 성장 구조(주문/사용자/AOV 기여)를 분해하여 매출 상태를 분석
 * 
 * Notes:
 * 	- 분석 기간은 거래가 없는 월과 극소수인 월을 제외하였습니다. (설정 분석 기간: 2017-01 ~ 2018-08)
 * 	- 해당 Adhoc 쿼리는 olist_am.vw_kpi_monthly_core를 기준으로 한 KPI 분석 쿼리입니다. (Delivered 기반 KPI)
 * 	- MoM 계산은 NULLIF로 0으로 나뉘는 것을 방지하였습니다.
 */


/****************************************************************************************************************************************/


-- base 월별 KPI 지표 샘플
SELECT  *
  FROM  olist_am.vw_base_customer_monthly_purchase
 LIMIT  10;

-- 거래가 없는 월 탐색(거래는 배송이 완료된(delivered order) 거래 기준)
-- 	- 2016-08: 0 / 2016-09: 1 /2016-10: 265 / 2016-11: 0 / 2016-12: 1
-- 	- 2018-09: 0 / 2018-10: 0 / 2018-11: 0 / 2018-12: 0
-- 	- 2016년도의 거래 건수는 2016-10을 제외하고는 모두 0 또는 1이기 때문에 분석에서 제외
-- 	- 2018년도 9, 10, 11, 12월의 거래 건수는 모두 0건으로 분석에서 제외
WITH month_cal AS (
	SELECT  DISTINCT `year_month`
	  FROM  olist_dm.dim_date
),
delivered_by_month AS (
	SELECT  `year_month`
			,COUNT(DISTINCT order_id) AS delivered_orders
	  FROM  olist_dm.vw_delivered_orders
	 GROUP
	    BY  `year_month`
)
SELECT  m.`year_month`
		,COALESCE(d.delivered_orders, 0) AS delivered_orders
  FROM  month_cal AS m
  LEFT
  JOIN  delivered_by_month AS d
    ON  d.`year_month` = m.`year_month`
 ORDER
    BY  m.`year_month`;


-- ===================================================================================================================================================================


/*
 * 성장 분석(KPI 지표 분석):
 * 
 * 	- gross_revenue
 * 		- 2017-01(127,482.37) -> 2017-11(1,153,229.37)동안 약 9배 성장하여 고성장 구간을 형성
 * 		- 그러나 2017-11(1,153,229.37) -> 2017-12(843,078.29)에는 매출이 -26.8% 하락
 * 		- 2018년은 2018-04에 최고점(1,132,878.93)에 도달한 이후 하락세를 보이나 1,000,000 수준으로 안정화 (완만한 하락 이후 안정적 유지 상태)
 * 
 * 	- order_cnt
 * 		- 2017-01(750) -> 2017-11(7,288)동안 약 9.7배 증가
 * 		- 2017-11(7,288) -> 2017-12(5,513)에는 주문 수가 -24.36% 감소하며 매출 하락과 동행
 * 		- 2018년은 2018-01에 최고점(7,069)에 도달한 이후 살짝 감소하여 6,000 ~ 7,000 수준으로 안정화
 * 
 * 	- active_buyers
 * 		- 2017-01(718) -> 2017-11(7,182)동안 약 10배 증가
 * 		- 2017-11(7,182) -> 2017-12(5,450)에는 실제 사용자 수가 -24.12% 하락하며 매출, 주문 수 하락과 동행
 * 		- 2018년은 2018-01에 최고점(6,974)에 도달한 이후 살짝 감소하여 6,000 수준으로 안정화
 * 
 * 	- aov
 * 		- 2017-01(169.98) -> 2017-11(158.24)로 고성장 구간인 2017년에 상승이 아닌 하락/정체 흐름을 보임
 * 		- 2017-11(158.24) -> 2017-12(152.93)에는 -3.36% 감소함
 * 		- 2018년에는 2018-04에 최고점(166.65)에 도달하며 회복되고 이후 160 수준으로 유지
 * 
 * 	- repeat_buyer_rate
 * 		- 절대적 수준은 매우 낮지만(0.1~3.0% 수준), 2017 -> 2018로 갈수록 점진적으로 상승 (2017-01: 0.14% -> 2018-08: 2.63%)
 * 		- 즉, 고성장 구간(2017년)은 신규 유입/거래량 확대 중심이었고, 2018년은 기존 고객 비중이 점진적으로 증가하는 형태
 * 		- 그러나 현재 시계열 KPI 분석만으로는 데이터의 한계인지 재구매율의 증가인지 파악할 수 없기 때문에 이후 코호트 분석을 통해 확정지어야 함 
 * 
 * 	- 최종적으로 현재 데이터 기간(2017-01 ~ 2018-08)의 매출 변동은 AOV보다는 주문 수/구매자 수 변화로 더 잘 설명되며, 특히 매출 MoM과 주문/구매자 MoM 방향 일치율은 각각 0.8421(19개월 중 16건)로 동행 경향이 확인됨
 */


-- 월별 KPI 지표
SELECT  `year_month`
		,gross_revenue
		,order_cnt
		,active_buyers
		,aov
		,repeat_buyer_rate * 100 AS repeat_buyer_pct
  FROM  olist_am.vw_kpi_monthly_core
 ORDER
    BY  `year_month`;


-- 월별 KPI 지표 + MOM
WITH m AS (
	SELECT  `year_month`
			,gross_revenue
			,order_cnt
			,active_buyers
			,aov
			,repeat_buyers
			,repeat_buyer_rate
			,repeat_buyer_rate * 100 AS repeat_buyer_pct
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
		,repeat_buyer_pct
		,(gross_revenue - LAG(gross_revenue) OVER (ORDER BY `year_month`)) AS gross_revenue_diff
		,(gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev_rate
		,(order_cnt - LAG(order_cnt) OVER (ORDER BY `year_month`)) AS mom_order_cnt_diff
		,(order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order_cnt_rate
		,(active_buyers - LAG(active_buyers) OVER (ORDER BY `year_month`)) AS mom_active_buyers_diff
		,(active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) -1) AS mom_active_buyers_rate
		,(aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov_rate
  FROM  m
 ORDER
    BY  `year_month`;


-- 성장 상태 탐지(급등, 급락)
-- 매출 MoM 급등 상위 5개의 year_month

-- year_month		gross_revenue		mom_rev_rate
-- ======================================================
-- 2017-02			  271239.32			  1.127661
-- 2017-11			  1153229.37		  0.535352
-- 2017-03			  414330.95			  0.527548
-- 2017-05			  566657.40			  0.449947
-- 2018-01			  1077887.46		  0.278514

WITH k AS (
	SELECT  `year_month`
			,gross_revenue
			,(gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_rev_rate IS NOT NULL
 ORDER
    BY  mom_rev_rate DESC
 LIMIT  5;

-- 매출 MoM 급락 상위 5개의 year_month

-- year_month		gross_revenue		mom_rev_rate
-- ======================================================
-- 2017-12			  843078.29			  -0.268941
-- 2017-06			  490050.37			  -0.135191
-- 2018-06			  1011448.96		  -0.103941
-- 2018-02			  966168.41			  -0.103646
-- 2017-04			  390812.40			  -0.056763

WITH k AS (
	SELECT  `year_month`
			,gross_revenue
			,(gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_rev_rate IS NOT NULL
 ORDER
    BY  mom_rev_rate ASC
 LIMIT  5;

-- 주문 수 급등 상위 5개의 year_month

-- year_month		order_cnt		mom_order_cnt_rate
-- ===========================================================
-- 2017-02			  1653				1.2040
-- 2017-11			  7288				0.6275
-- 2017-03			  2546				0.5402
-- 2017-05			  3545				0.5393
-- 2018-01			  7069				0.2822

WITH k AS (
	SELECT  `year_month`
			,order_cnt
			,(order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order_cnt_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_order_cnt_rate IS NOT NULL
 ORDER
    BY  mom_order_cnt_rate DESC
 LIMIT  5;

-- 주문 수 급락 상위 5개의 year_month

-- year_month		order_cnt		mom_order_cnt_rate
-- ===========================================================
-- 2017-12			  5513				-0.2436
-- 2017-06			  3135				-0.1157
-- 2018-06			  6096				-0.0968
-- 2017-04			  2303				-0.0954
-- 2018-02			  6555				-0.0727

WITH k AS (
	SELECT  `year_month`
			,order_cnt
			,(order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order_cnt_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_order_cnt_rate IS NOT NULL
 ORDER
    BY  mom_order_cnt_rate ASC
 LIMIT  5;

-- 사용자 수 급등 상위 5개의 year_month

-- year_month		active_buyers		mom_active_buyers_rate
-- =================================================================
-- 2017-02				1630					1.2702
-- 2017-11				7182					0.6260
-- 2017-03				2508					0.5387
-- 2017-05				3478					0.5295
-- 2018-01				6974					0.2796

WITH k AS (
	SELECT  `year_month`
			,active_buyers
			,(active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) - 1) AS mom_active_buyers_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_active_buyers_rate IS NOT NULL
 ORDER
    BY  mom_active_buyers_rate DESC
 LIMIT  5;

-- 사용자 수 급락 상위 5개의 year_month

-- year_month		active_buyers		mom_active_buyers_rate
-- =================================================================
-- 2017-12				5450					-0.2412
-- 2017-06				3076					-0.1156
-- 2018-06				6058					-0.0949
-- 2017-04				2274					-0.0933
-- 2018-02				6400					-0.0823

WITH k AS (
	SELECT  `year_month`
			,active_buyers
			,(active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) - 1) AS mom_active_buyers_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_active_buyers_rate IS NOT NULL
 ORDER
    BY  mom_active_buyers_rate ASC
 LIMIT  5;

-- 평균 주문 금액 급등 상위 5개의 year_month

-- year_month		aov			mom_aov_rate
-- ===============================================
-- 2017-09		   168.93		  0.096734
-- 2018-03		   160.02		  0.085691
-- 2017-08		   154.03		  0.053197
-- 2017-04		   169.7		  0.042768
-- 2018-04		   166.65		  0.041432

WITH k AS (
	SELECT  `year_month`
			,aov
			,(aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_aov_rate IS NOT NULL
 ORDER
    BY  mom_aov_rate DESC
 LIMIT  5;

-- 평균 주문 금액 급락 상위 5개의 year_month

-- year_month		aov			mom_aov_rate
-- ===============================================
-- 2018-08		   155.17		 -0.070170
-- 2017-07		   146.25		 -0.064419
-- 2017-05		   159.85		 -0.058044
-- 2017-11		   158.24		 -0.056579
-- 2017-02		   164.09		 -0.034651

WITH k AS (
	SELECT  `year_month`
			,aov
			,(aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov_rate
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  k
 WHERE  mom_aov_rate IS NOT NULL
 ORDER
    BY  mom_aov_rate ASC
 LIMIT  5;


-- 성장 구조 분해

-- year_month		mom_rev_rate		mom_active_buyers_rate		mom_order_cnt_rate		mom_aov_rate
-- ===============================================================================================================
-- 2017-01				NULL					NULL						NULL				NULL			
-- 2017-02			  1.127661				   1.2702					   1.2040			  -0.034651
-- 2017-03			  0.527548				   0.5387					   0.5402			  -0.008227
-- 2017-04			  -0.056763				   -0.0933					   -0.0954			  0.042768
-- 2017-05			  0.449947				   0.5295					   0.5393			  -0.058044
-- 2017-06			  -0.135191				   -0.1156					   -0.1157			  -0.022083
-- 2017-07			  0.155594				   0.2360					   0.2351			  -0.064419
-- 2017-08			  0.140444				   0.0821					   0.0829			  0.053197
-- 2017-09			  0.085541				   -0.0075					   -0.0103			  0.096734
-- 2017-10			  0.071375				   0.0818					   0.0790			  -0.007104
-- 2017-11			  0.535352				   0.6260					   0.6275			  -0.056579
-- 2017-12			  -0.268941				   -0.2412					   -0.2436			  -0.033557
-- 2018-01			  0.278514				   0.2796					   0.2822			  -0.002943
-- 2018-02			  -0.103646				   -0.0823					   -0.0727			  -0.033381
-- 2018-03			  0.159837				   0.0803					   0.0683			  0.085691
-- 2018-04			  0.010959				   -0.0246					   -0.0293			  0.041432
-- 2018-05			  -0.003623				   -0.0076					   -0.0072			  0.003600
-- 2018-06			  -0.103941				   -0.0949					   -0.0968			  -0.007952
-- 2018-07			  0.015658				   0.0064					   0.0098			  0.005786
-- 2018-08			  -0.040685				   0.0349					   0.0317			  -0.070170

WITH m AS (
	SELECT  `year_month`
			,gross_revenue
			,active_buyers
			,order_cnt
			,aov
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  `year_month`
		,(gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev_rate
		,(active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) -1) AS mom_active_buyers_rate
		,(order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order_cnt_rate
		,(aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov_rate
  FROM  m
 ORDER
    BY  `year_month`;


-- KPI 성장 방향 일치 여부

-- n_months		rev_order_same_direction_cnt		rev_order_same_direction_ratio		rev_buyers_same_direction_cnt		rev_buyers_same_directions_ratio		rev_aov_same_direction_cnt		rev_aov_same_directions_ratio
-- =================================================================================================================================================================================================================================
-- 19							16									0.8421								16									0.8421								10								0.5263
WITH t AS (
	SELECT  `year_month`
			,(gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev
			,(active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) - 1) AS mom_buyers
			,(order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order
			,(aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov
	  FROM  olist_am.vw_kpi_monthly_core
	 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  COUNT(*) AS n_months
		,SUM(CASE WHEN mom_rev IS NOT NULL AND mom_order IS NOT NULL AND (mom_rev > 0 AND mom_order > 0 OR mom_rev < 0 AND mom_order < 0) THEN 1 ELSE 0 END) AS rev_order_same_direction_cnt
		,ROUND(SUM(CASE WHEN mom_rev IS NOT NULL AND mom_order IS NOT NULL AND (mom_rev > 0 AND mom_order > 0 OR mom_rev < 0 AND mom_order < 0) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) AS rev_order_same_direction_ratio
		,SUM(CASE WHEN 	mom_rev IS NOT NULL AND mom_buyers IS NOT NULL AND (mom_rev > 0 AND mom_buyers > 0 OR mom_rev < 0 AND mom_buyers < 0) THEN 1 ELSE 0 END) AS rev_buyers_same_direction_cnt
		,ROUND(SUM(CASE WHEN mom_rev IS NOT NULL AND mom_buyers IS NOT NULL AND (mom_rev > 0 AND mom_buyers > 0 OR mom_rev < 0 AND mom_buyers < 0) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) AS rev_buyers_same_directions_ratio
		,SUM(CASE WHEN mom_rev IS NOT NULL AND mom_aov IS NOT NULL AND (mom_rev > 0 AND mom_aov > 0 OR mom_rev < 0 AND mom_aov < 0) THEN 1 ELSE 0 END) AS rev_aov_same_direction_cnt
		,ROUND(SUM(CASE WHEN mom_rev IS NOT NULL AND mom_aov IS NOT NULL AND (mom_rev > 0 AND mom_aov > 0 OR mom_rev < 0 AND mom_aov < 0) THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) AS rev_aov_same_directions_ratio
  FROM  t
 WHERE  mom_rev IS NOT NULL;


-- 매출 MoM vs 주문/구매자 MoM 방향 불일치 year_month snapshot(불일치 year_month는 aov의 방향과 일치)

-- year_month		mom_rev		mom_order		mom_buyer		mom_aov
-- ==========================================================================
-- 2017-09		    0.085541	-0.0103			-0.0075		    0.096734
-- 2018-04		    0.010959	-0.0293			-0.0246		    0.041432
-- 2018-08		   -0.040685	 0.0317			 0.0349		   -0.070170

WITH t AS (
    SELECT  `year_month`,
            (gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev,
            (order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order,
            (active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) - 1) AS mom_buyers,
            (aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov
      FROM  olist_am.vw_kpi_monthly_core
     WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
)
SELECT  *
  FROM  t
 WHERE  mom_rev IS NOT NULL
   AND  ((mom_rev > 0 AND (mom_order <= 0 OR mom_buyers <= 0)) OR (mom_rev < 0 AND (mom_order >= 0 OR mom_buyers >= 0)))
 ORDER 
	BY  `year_month`;








