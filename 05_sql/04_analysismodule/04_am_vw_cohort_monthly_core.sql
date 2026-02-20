/******************************************************************************************************************************************************/


/*
 * File: 04_am_vw_cohort_monthly_core.sql
 * Description:
 * 	- 코호트 기반 월별 핵심 KPI를 Full Matrix 형태로 표준화하여 제공하는 View
 * 	- 코호트 리텐션/재구매/매출 기여를 시계열 관점에서 분석하기 위한 기준 데이터
 * 
 * 포함 KPI (cohort_year_month x year_month x month_n 기준):
 * 	- Cohort Size (m0 구매자 수)
 * 	- Cohort Active Buyers (해당 월 구매자 수)
 * 	- Cohort Order Count (해당 월 주문 수)
 * 	- Cohort Gross Revenue (해당 월 매출)
 * 	- Cohort Retention Rate (Active Buyers / Cohort Size)
 * 	- Cohort AOV (Gross Revenue / Order Count)
 * 	- Cumulative Gross Revenue (코호트 내 누적 매출)
 * 	- Cumulative Order Count (코호트 내 누적 주문 수)
 * 
 * 코호트 기준:
 * 	- 코호트는 고객별 첫 구매 월(cohort_year_month) 기준으로 정의
 * 
 * Note:
 * 	- 해당 View는 Sparse View가 아닌 월 단위 Full 집계 View입니다.
 * 	- dim_date를 기준으로 year_month를 작성하였으며, 구매 데이터가 없는 월은 0 또는 NULL로 표시됩니다.
 * 	- cohort_size는 month_n=0(첫 구매 월) 구매자 수로 정의됩니다.
 * 	- retention_rate와 aov는 분모가 0인 경우 NULL로 처리하였습니다.
 * 	- 원본 데이터 초기와 말미(2016-08~09/2018-09~12)는 표본 수가 매우 작을 수 있어 비율 KPI가 급격히 변동될 수 있으므로 해석 시 주의가 필요합니다.
 */


/******************************************************************************************************************************************************/


USE olist_am;


-- ===========================================================================================================================================


-- View 생성
CREATE OR REPLACE VIEW olist_am.vw_cohort_monthly_core AS
WITH months AS (
	SELECT  DISTINCT dd.`year_month`
			,STR_TO_DATE(CONCAT(dd.`year_month`, '-01'), '%Y-%m-%d') AS ym_dt
	  FROM  olist_dm.dim_date AS dd
),
cohorts AS (
	SELECT  DISTINCT b.cohort_year_month
			,STR_TO_DATE(CONCAT(b.cohort_year_month, '-01'), '%Y-%m-%d') AS cohort_dt
	  FROM  olist_am.vw_base_customer_monthly_purchase AS b
),
matrix AS (
	SELECT  c.cohort_year_month
			,m.`year_month`
			,TIMESTAMPDIFF(MONTH, c.cohort_dt, m.ym_dt) AS month_n
	  FROM  cohorts AS c
	  JOIN  months AS m
	    ON  m.ym_dt >= c.cohort_dt
),
base_agg AS (
	SELECT  b.cohort_year_month
			,b.`year_month`
			,b.month_n
			,COUNT(DISTINCT b.customer_unique_id) AS cohort_active_buyers
			,SUM(b.order_cnt) AS cohort_order_cnt
			,CAST(SUM(b.gross_revenue) AS DECIMAL(18,2)) AS cohort_gross_revenue
	  FROM  olist_am.vw_base_customer_monthly_purchase AS b
	 GROUP
	    BY  b.cohort_year_month
	    	,b.`year_month`
	    	,b.month_n
),
cohort_size AS (
	SELECT  b.cohort_year_month
			,COUNT(DISTINCT b.customer_unique_id) AS cohort_size
	  FROM  olist_am.vw_base_customer_monthly_purchase AS b
	 WHERE  b.month_n = 0
	 GROUP
	    BY  b.cohort_year_month
)
SELECT  mx.cohort_year_month
		,mx.`year_month`
		,mx.month_n
		,cs.cohort_size
		,COALESCE(ba.cohort_active_buyers, 0) AS cohort_active_buyers
		,COALESCE(ba.cohort_order_cnt, 0) AS cohort_order_cnt
		,COALESCE(ba.cohort_gross_revenue, 0) AS cohort_gross_revenue
		,CAST(CASE WHEN cs.cohort_size = 0 THEN NULL ELSE COALESCE(ba.cohort_active_buyers, 0) / cs.cohort_size END AS DECIMAL(10,6)) AS cohort_retention_rate
		,CAST(CASE WHEN COALESCE(ba.cohort_order_cnt, 0) = 0 THEN NULL ELSE COALESCE(ba.cohort_gross_revenue, 0) / ba.cohort_order_cnt END AS DECIMAL(18,2)) AS cohort_aov
		,CAST(SUM(COALESCE(ba.cohort_gross_revenue, 0)) OVER (PARTITION BY mx.cohort_year_month ORDER BY mx.month_n) AS DECIMAL(18,2)) AS cum_gross_revenue
		,SUM(COALESCE(ba.cohort_order_cnt, 0)) OVER (PARTITION BY mx.cohort_year_month ORDER BY mx.month_n) AS cum_order_cnt
  FROM  matrix AS mx
  JOIN  cohort_size AS cs
    ON  cs.cohort_year_month = mx.cohort_year_month
  LEFT
  JOIN  base_agg AS ba
    ON  ba.cohort_year_month = mx.cohort_year_month
   AND  ba.`year_month` = mx.`year_month`
   AND  ba.month_n = mx.month_n
 ORDER
    BY  mx.cohort_year_month
    	,mx.month_n;


-- ===========================================================================================================================================


-- QC

-- 샘플

SELECT  *
  FROM  olist_am.vw_cohort_monthly_core
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_am.vw_cohort_monthly_core;


-- Snapshot(코호트별 active_users: m0 ~ m12)

-- cohort_year_month	cohort_size	 m0_buyers	m1_buyers	m2_buyers	m3_buyers	m4_buyers	m5_buyers	m6_buyers	m7_buyers	m8_buyers	m9_buyers	m10_buyers	m11_buyers	m12_buyers
-- ==================================================================================================================================================================================================
-- 2016-09					 1			1			0			0			0			0			0			0			0			0			0			0			0			0	
-- 2016-10					262		   262			0			0			0			0			0			1			0			0			1			0			1			0	
-- 2016-12					 1			1			1			0			0			0			0			0			0			0			0			0			0			0	
-- 2017-01					717		   717			2			2			1			3			1			3			1			1			0			3			1			5	
-- 2017-02					1628	  1628			3			5			2			7			2			4			3			2			3			2			5			2	
-- 2017-03					2503	  2503			11			9			10			9			4			4			8			8			2			9			3			5	
-- 2017-04					2256	  2256			14			5			4			6			6			8			7			7			4			6			2			1	
-- 2017-05					3450	  3450			16			16			10			10			11			14			5			9			9			9			12			8	
-- 2017-06					3037	  3037			15			12			13			9			12			11			7			4			6			9			11			5	
-- 2017-07					3752	  3752			20			13			9			11			8			12			4			7			10			8			11			5	
-- 2017-08					4057	  4057			28			14			11			14			21			12			11			6			6			10			8			5	
-- 2017-09					4004	  4004			28			22			11			18			9			9			10			11			7			10			3			0	
-- 2017-10					4328	  4328			31			11			4			10			9			9			16			12			8			9			0			0	
-- 2017-11					7059	  7059			40			26			12			12			13			8			13			9			4			0			0			0	
-- 2017-12					5338	  5338			11			15			18			14			11			9			1			10			0			0			0			0	
-- 2018-01					6842	  6842			23			25			20			20			11			12			16			0			0			0			0			0
-- 2018-02					6288	  6288			22			25			19			16			14			13			0			0			0			0			0			0
-- 2018-03					6774	  6774			27			20			20			8			8			0			0			0			0			0			0			0
-- 2018-04					6582	  6582			39			20			16			9			0			0			0			0			0			0			0			0
-- 2018-05					6506	  6506			34			17			12			0			0			0			0			0			0			0			0			0
-- 2018-06					5875	  5875			25			16			0			0			0			0			0			0			0			0			0			0			
-- 2018-07					5946	  5946			31			0			0			0			0			0			0			0			0			0			0			0
-- 2018-08					6144	  6144			0			0			0			0			0			0			0			0			0			0			0			0
	
SELECT  cohort_year_month
		,MAX(cohort_size) AS cohort_size
		,SUM(CASE WHEN month_n = 0 THEN cohort_active_buyers END) AS m0_buyers
		,SUM(CASE WHEN month_n = 1 THEN cohort_active_buyers END) AS m1_buyers
		,SUM(CASE WHEN month_n = 2 THEN cohort_active_buyers END) AS m2_buyers
		,SUM(CASE WHEN month_n = 3 THEN cohort_active_buyers END) AS m3_buyers
		,SUM(CASE WHEN month_n = 4 THEN cohort_active_buyers END) AS m4_buyers
		,SUM(CASE WHEN month_n = 5 THEN cohort_active_buyers END) AS m5_buyers
		,SUM(CASE WHEN month_n = 6 THEN cohort_active_buyers END) AS m6_buyers
		,SUM(CASE WHEN month_n = 7 THEN cohort_active_buyers END) AS m7_buyers
		,SUM(CASE WHEN month_n = 8 THEN cohort_active_buyers END) AS m8_buyers
		,SUM(CASE WHEN month_n = 9 THEN cohort_active_buyers END) AS m9_buyers
		,SUM(CASE WHEN month_n = 10 THEN cohort_active_buyers END) AS m10_buyers
		,SUM(CASE WHEN month_n = 11 THEN cohort_active_buyers END) AS m11_buyers
		,SUM(CASE WHEN month_n = 12 THEN cohort_active_buyers END) AS m12_buyers
  FROM  olist_am.vw_cohort_monthly_core
 GROUP
    BY  cohort_year_month
 ORDER
    BY  cohort_year_month;


-- Snapshot (코호트별 order_cnt: m0 ~ m12)

-- cohort_year_month	cohort_size	   m0_orders	m1_orders	m2_orders	m3_orders	m4_orders	m5_orders	m6_orders	m7_orders	m8_orders	m9_orders	m10_orders	m11_orders	m12_orders
-- ====================================================================================================================================================================================================
-- 2016-09					1				1			0			0			0			0			0			0			0			0			0			0			0			0
-- 2016-10				   262			   265			0			0			0			0			0			1			0			0			1			0			1			0
-- 2016-12					1				1			1			0			0			0			0			0			0			0			0			0			0			0
-- 2017-01				   717			   749			2			2			1			3			1			3			1			1			0			3			1			5
-- 2017-02				   1628			  1651			3			5			2			7			2			4			3			2			3			2			5			2
-- 2017-03				   2503			  2541		    12			9			12			9			4			4			8			8			2			9			3			5
-- 2017-04				   2256			  2284			15			6			5			6			6			9			7			7			4			6			2			1
-- 2017-05				   3450			  3516			17			16			10			10			12			16			5			9			11			9			12			8
-- 2017-06				   3037			  3092			16			13			13			10			12			11			7			4			6			9			12			5
-- 2017-07				   3752			  3820			22			13			9			13			8			12			4			8			11			8			12			5
-- 2017-08				   4057			  4133			29			16			11			14			23			12			11			6			6			10			8			5
-- 2017-09				   4004			  4070			28			22			15			18			13			9			10			11			7			10			3			0
-- 2017-10				   4328			  4384			33			11			4			11			9			9			17			13			8			9			0			0
-- 2017-11				   7059			  7159			42			27			12			12			14			9			13			9			4			0			0			0
-- 2017-12				   5338			  5395			11			16			20			14			11			9			1			10			0			0			0			0
-- 2018-01				   6842			  6934			23			25			20			20			12			12			16			0			0			0			0			0
-- 2018-02				   6288			  6435			22			26			19			16			14			13			0			0			0			0			0			0
-- 2018-03				   6774			  6860			27			23			21			8			8			0			0			0			0			0			0			0
-- 2018-04				   6582			  6633			39			20			16			10			0			0			0			0			0			0			0			0
-- 2018-05				   6506			  6556			35			17			12			0			0			0			0			0			0			0			0			0
-- 2018-06				   5875			  5908			25			16			0			0			0			0			0			0			0			0			0			0
-- 2018-07				   5946			  6004			31			0			0			0			0			0			0			0			0			0			0			0
-- 2018-08				   6144			  6180			0			0			0			0			0			0			0			0			0			0			0			0

SELECT  cohort_year_month
        ,MAX(cohort_size) AS cohort_size
        ,SUM(CASE WHEN month_n = 0  THEN cohort_order_cnt ELSE 0 END) AS m0_orders
        ,SUM(CASE WHEN month_n = 1  THEN cohort_order_cnt ELSE 0 END) AS m1_orders
        ,SUM(CASE WHEN month_n = 2  THEN cohort_order_cnt ELSE 0 END) AS m2_orders
        ,SUM(CASE WHEN month_n = 3  THEN cohort_order_cnt ELSE 0 END) AS m3_orders
        ,SUM(CASE WHEN month_n = 4  THEN cohort_order_cnt ELSE 0 END) AS m4_orders
        ,SUM(CASE WHEN month_n = 5  THEN cohort_order_cnt ELSE 0 END) AS m5_orders
        ,SUM(CASE WHEN month_n = 6  THEN cohort_order_cnt ELSE 0 END) AS m6_orders
        ,SUM(CASE WHEN month_n = 7  THEN cohort_order_cnt ELSE 0 END) AS m7_orders
        ,SUM(CASE WHEN month_n = 8  THEN cohort_order_cnt ELSE 0 END) AS m8_orders
        ,SUM(CASE WHEN month_n = 9  THEN cohort_order_cnt ELSE 0 END) AS m9_orders
        ,SUM(CASE WHEN month_n = 10 THEN cohort_order_cnt ELSE 0 END) AS m10_orders
        ,SUM(CASE WHEN month_n = 11 THEN cohort_order_cnt ELSE 0 END) AS m11_orders
        ,SUM(CASE WHEN month_n = 12 THEN cohort_order_cnt ELSE 0 END) AS m12_orders
  FROM  olist_am.vw_cohort_monthly_core
 GROUP
    BY  cohort_year_month
 ORDER
    BY  cohort_year_month;


-- Snapshot (코호트별 gross_revenue: m0 ~ m12)

-- cohort_year_month	cohort_size		m0_revenue		m1_revenue		m2_revenue		m3_revenue		m4_revenue		m5_revenue		m6_revenue		m7_revenue		m8_revenue		m9_revenue		m10_revenue		m11_revenue		m12_revenue
-- =========================================================================================================================================================================================================================================================
-- 2016-09					1			  143.46		   0.00			   0.00				0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2016-10					262			 46490.66		   0.00			   0.00				0.00			0.00			0.00		   111.30			0.00			0.00		   356.13			0.00		    56.78			0.00
-- 2016-12					1			   19.62		   19.62		   0.00				0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2017-01					717			 127462.75		   111.07		   114.70			89.70		   246.25		   69.33		   448.43		   105.17			47.05			0.00		   427.05			66.60		   552.45
-- 2017-02				   1628			 271128.25		   481.68		   570.63			118.73		  1151.96		   74.21		   688.92		   274.30		   464.61		   385.35		   343.22		   454.39		   279.24
-- 2017-03				   2503			 413734.57		  1623.63		  1737.12		   1836.10		  1213.97		  1433.39		   689.18		   887.42		  1385.61		   237.96		   725.93		   380.30		   907.29
-- 2017-04				   2256			 388417.14		  2937.00		   940.40			758.21		  1158.30		  1208.34		  2083.80		  1096.55		   963.99		   499.82		   692.56		   343.17		   175.13
-- 2017-05				   3450			 561618.30		  1930.02		  2857.55		   1723.85		  1219.04		  1638.47		  3002.26		   608.02		  1623.52		  1353.84		  1459.06		  1438.36		  1056.81
-- 2017-06				   3037			 484122.56		  2161.99		  2142.03		   1484.87		  1434.43		  1595.53		  1601.41		   822.86		   346.26		   497.30		  2232.08		  2063.01		   513.44
-- 2017-07				   3752			 558428.59		  3070.50		  1307.59		   1156.32		  1220.36		  1865.65		  2553.19		   250.36		   413.96		  2071.11		  1163.02		  1264.85		  1409.38
-- 2017-08				   4057			 635510.20		  5056.77		  2229.03		   1014.62		  1755.13		  2883.84		  1864.71		  1949.83		  1800.03		  1204.91		  1460.16		  1825.60		   595.78
-- 2017-09				   4004			 689733.57		  3659.33		  3182.57		   1661.37		  3226.19		  2055.93		  1067.34		   965.33		  1168.15		   695.80		  1195.16		   326.30			0.00
-- 2017-10				   4328			 737563.60		  4443.59		  1643.41			578.42		  1486.94		  1408.03		  1102.84		  2448.18		  2349.37		  1977.36		   960.39			0.00			0.00
-- 2017-11				   7059			 1135312.35		  8078.27		  3969.24		   1588.94		  2124.07		  1895.06		  1133.36		  1811.92		  1323.64		   678.31			0.00			0.00			0.00
-- 2017-12				   5338			 824253.26		  2369.92		  1922.78		   2168.62		  1959.48		  2055.20		  1258.70		   284.96		  1543.82			0.00			0.00			0.00			0.00
-- 2018-01				   6842			 1057246.69		  3650.85		  3685.98		   3418.22		  2663.14		  1586.64		  1393.54		  1643.58			0.00			0.00			0.00			0.00			0.00
-- 2018-02				   6288			 949944.05		  3024.45		  4230.12		   3518.60		  2168.38		  3050.56		  1730.85			0.00			0.00			0.00			0.00			0.00			0.00
-- 2018-03				   6774			 1100941.76		  3058.08		  3526.76		   3264.78		  1360.77		   519.71			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2018-04				   6582			 1108008.24		  5090.03		  3581.14		   1886.83		   849.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2018-05				   6506			 1099984.15		  8277.65		  2623.35		   1699.22			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2018-06				   5875			 981306.94		  3123.97		  1993.21			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2018-07				   5946			 1003272.25		  4521.74			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00
-- 2018-08				   6144			 963030.33		   0.00				0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00			0.00

SELECT  cohort_year_month
        ,MAX(cohort_size) AS cohort_size
        ,SUM(CASE WHEN month_n = 0  THEN cohort_gross_revenue ELSE 0 END) AS m0_revenue
        ,SUM(CASE WHEN month_n = 1  THEN cohort_gross_revenue ELSE 0 END) AS m1_revenue
        ,SUM(CASE WHEN month_n = 2  THEN cohort_gross_revenue ELSE 0 END) AS m2_revenue
        ,SUM(CASE WHEN month_n = 3  THEN cohort_gross_revenue ELSE 0 END) AS m3_revenue
        ,SUM(CASE WHEN month_n = 4  THEN cohort_gross_revenue ELSE 0 END) AS m4_revenue
        ,SUM(CASE WHEN month_n = 5  THEN cohort_gross_revenue ELSE 0 END) AS m5_revenue
        ,SUM(CASE WHEN month_n = 6  THEN cohort_gross_revenue ELSE 0 END) AS m6_revenue
        ,SUM(CASE WHEN month_n = 7  THEN cohort_gross_revenue ELSE 0 END) AS m7_revenue
        ,SUM(CASE WHEN month_n = 8  THEN cohort_gross_revenue ELSE 0 END) AS m8_revenue
        ,SUM(CASE WHEN month_n = 9  THEN cohort_gross_revenue ELSE 0 END) AS m9_revenue
        ,SUM(CASE WHEN month_n = 10 THEN cohort_gross_revenue ELSE 0 END) AS m10_revenue
        ,SUM(CASE WHEN month_n = 11 THEN cohort_gross_revenue ELSE 0 END) AS m11_revenue
        ,SUM(CASE WHEN month_n = 12 THEN cohort_gross_revenue ELSE 0 END) AS m12_revenue
  FROM  olist_am.vw_cohort_monthly_core
 GROUP
    BY  cohort_year_month
 ORDER
    BY  cohort_year_month;


-- Snapshot (코호트별 retention_rate: m0 ~ m12)

-- cohort_year_month   cohort_size   	m0_ret   	m1_ret  	 m2_ret  	 m3_ret  	 m4_ret   	m5_ret   	m6_ret   	m7_ret   	m8_ret   	m9_ret   	m10_ret  	m11_ret  	m12_ret
-- ==================================================================================================================================================================================================
-- 2016-09                     1       1.000000 	0.000000 	0.000000 	0.000000	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2016-10                   262       1.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.003817 	0.000000 	0.000000 	0.003817 	0.000000 	0.003817 	0.000000
-- 2016-12                     1       1.000000 	1.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2017-01                   717       1.000000 	0.002789 	0.002789 	0.001395 	0.004184 	0.001395 	0.004184 	0.001395 	0.001395 	0.000000 	0.004184 	0.001395 	0.006974
-- 2017-02                  1628       1.000000 	0.001843 	0.003071 	0.001229 	0.004300 	0.001229 	0.002457 	0.001843 	0.001229 	0.001843 	0.001229 	0.003071 	0.001229
-- 2017-03                  2503       1.000000 	0.004395 	0.003596 	0.003995 	0.003596 	0.001598 	0.001598 	0.003196 	0.003196 	0.000799 	0.003596 	0.001199 	0.001998
-- 2017-04                  2256       1.000000 	0.006206 	0.002216 	0.001773 	0.002660 	0.002660 	0.003546 	0.003103 	0.003103 	0.001773 	0.002660 	0.000887 	0.000443
-- 2017-05                  3450       1.000000 	0.004638 	0.004638 	0.002899 	0.002899 	0.003188 	0.004058 	0.001449 	0.002609 	0.002609 	0.002609 	0.003478 	0.002319
-- 2017-06                  3037       1.000000 	0.004939 	0.003951 	0.004281 	0.002963 	0.003951 	0.003622 	0.002305 	0.001317 	0.001976 	0.002963 	0.003622 	0.001646
-- 2017-07                  3752       1.000000 	0.005330 	0.003465 	0.002399 	0.002932 	0.002132 	0.003198 	0.001066 	0.001866 	0.002665 	0.002132 	0.002932 	0.001333
-- 2017-08                  4057       1.000000 	0.006902 	0.003451 	0.002711 	0.003451 	0.005176 	0.002958 	0.002711 	0.001479 	0.001479 	0.002465 	0.001972 	0.001232
-- 2017-09                  4004       1.000000 	0.006993 	0.005495 	0.002747 	0.004496 	0.002248 	0.002248 	0.002498 	0.002747 	0.001748 	0.002498 	0.000749 	0.000000
-- 2017-10                  4328       1.000000 	0.007163 	0.002542 	0.000924 	0.002311 	0.002079 	0.002079 	0.003697 	0.002773 	0.001848 	0.002079 	0.000000 	0.000000
-- 2017-11                  7059       1.000000 	0.005667 	0.003683 	0.001700 	0.001700 	0.001842 	0.001133 	0.001842 	0.001275 	0.000567 	0.000000 	0.000000 	0.000000
-- 2017-12                  5338       1.000000 	0.002061 	0.002810 	0.003372 	0.002623 	0.002061 	0.001686 	0.000187 	0.001873 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-01                  6842       1.000000 	0.003362 	0.003654 	0.002923 	0.002923 	0.001608 	0.001754 	0.002338 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-02                  6288       1.000000 	0.003499 	0.003976 	0.003022 	0.002545 	0.002226 	0.002067 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-03                  6774       1.000000 	0.003986 	0.002952 	0.002952 	0.001181 	0.001181 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-04                  6582       1.000000 	0.005925 	0.003039 	0.002431 	0.001367 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-05                  6506       1.000000 	0.005226 	0.002613 	0.001844 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-06                  5875       1.000000 	0.004255 	0.002723 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-07                  5946       1.000000 	0.005214 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000
-- 2018-08                  6144       1.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000 	0.000000

SELECT  cohort_year_month
        ,MAX(cohort_size) AS cohort_size
        ,MAX(CASE WHEN month_n = 0  THEN cohort_retention_rate END) AS m0_retention
        ,MAX(CASE WHEN month_n = 1  THEN cohort_retention_rate END) AS m1_retention
        ,MAX(CASE WHEN month_n = 2  THEN cohort_retention_rate END) AS m2_retention
        ,MAX(CASE WHEN month_n = 3  THEN cohort_retention_rate END) AS m3_retention
        ,MAX(CASE WHEN month_n = 4  THEN cohort_retention_rate END) AS m4_retention
        ,MAX(CASE WHEN month_n = 5  THEN cohort_retention_rate END) AS m5_retention
        ,MAX(CASE WHEN month_n = 6  THEN cohort_retention_rate END) AS m6_retention
        ,MAX(CASE WHEN month_n = 7  THEN cohort_retention_rate END) AS m7_retention
        ,MAX(CASE WHEN month_n = 8  THEN cohort_retention_rate END) AS m8_retention
        ,MAX(CASE WHEN month_n = 9  THEN cohort_retention_rate END) AS m9_retention
        ,MAX(CASE WHEN month_n = 10 THEN cohort_retention_rate END) AS m10_retention
        ,MAX(CASE WHEN month_n = 11 THEN cohort_retention_rate END) AS m11_retention
        ,MAX(CASE WHEN month_n = 12 THEN cohort_retention_rate END) AS m12_retention
  FROM  olist_am.vw_cohort_monthly_core
 GROUP
    BY  cohort_year_month
 ORDER
    BY  cohort_year_month;


-- Snapshot (코호트별 aov: m0 ~ m12)

-- cohort_year_month  cohort_size   m0_aov    m1_aov    m2_aov    m3_aov    m4_aov    m5_aov    m6_aov    m7_aov    m8_aov    m9_aov    m10_aov   m11_aov   m12_aov
-- ================================================================================================================================================================
-- 2016-09             1             143.46
-- 2016-10             262           175.44                         111.30              356.13              56.78
-- 2016-12             1              19.62     19.62
-- 2017-01             717           170.18     55.54     57.35     89.70     82.08     69.33     149.48    105.17     47.05               142.35     66.60    110.49
-- 2017-02             1628          164.22    160.56    114.13     59.37    164.57     37.11     172.23     91.43    232.31    128.45     171.61     90.88    139.62
-- 2017-03             2503          162.82    135.30    193.01    153.01    134.89    358.35     172.30    110.93    173.20    118.98      80.66    126.77    181.46
-- 2017-04             2256          170.06    195.80    156.73    151.64    193.05    201.39     231.53    156.65    137.71    124.96     115.43    171.59    175.13
-- 2017-05             3450          159.73    113.53    178.60    172.39    121.90    136.54     187.64    121.60    180.39    123.08     162.12    119.86    132.10
-- 2017-06             3037          156.57    135.12    164.77    114.22    143.44    132.96     145.58    117.55     86.57     82.88     248.01    171.92    102.69
-- 2017-07             3752          146.19    139.57    100.58    128.48     93.87    233.21     212.77     62.59     51.75    188.28     145.38    105.40    281.88
-- 2017-08             4057          153.76    174.37    139.31     92.24    125.37    125.38     155.39    177.26    300.01    200.82     146.02    228.20    119.16
-- 2017-09             4004          169.47    130.69    144.66    110.76    179.23    158.15     118.59     96.53    106.20     99.40     119.52    108.77
-- 2017-10             4328          168.24    134.65    149.40    144.61    135.18    156.45     122.54    144.01    180.72    247.17     106.71
-- 2017-11             7059          158.59    192.34    147.01    132.41    177.01    135.36     125.93    139.38    147.07    169.58
-- 2017-12             5338          152.78    215.45    120.17    108.43    139.96    186.84     139.86    284.96    154.38
-- 2018-01             6842          152.47    158.73    147.44    170.91    133.16    132.22     116.13    102.72
-- 2018-02             6288          147.62    137.48    162.70    185.19    135.52    217.90     133.14
-- 2018-03             6774          160.49    113.26    153.34    155.47    170.10     64.96
-- 2018-04             6582          167.04    130.51    179.06    117.93     84.90
-- 2018-05             6506          167.78    236.50    154.31    141.60
-- 2018-06             5875          166.10    124.96    124.58
-- 2018-07             5946          167.10    145.86
-- 2018-08             6144          155.83

SELECT  cohort_year_month
        ,MAX(cohort_size) AS cohort_size
        ,MAX(CASE WHEN month_n = 0  THEN cohort_aov END) AS m0_aov
        ,MAX(CASE WHEN month_n = 1  THEN cohort_aov END) AS m1_aov
        ,MAX(CASE WHEN month_n = 2  THEN cohort_aov END) AS m2_aov
        ,MAX(CASE WHEN month_n = 3  THEN cohort_aov END) AS m3_aov
        ,MAX(CASE WHEN month_n = 4  THEN cohort_aov END) AS m4_aov
        ,MAX(CASE WHEN month_n = 5  THEN cohort_aov END) AS m5_aov
        ,MAX(CASE WHEN month_n = 6  THEN cohort_aov END) AS m6_aov
        ,MAX(CASE WHEN month_n = 7  THEN cohort_aov END) AS m7_aov
        ,MAX(CASE WHEN month_n = 8  THEN cohort_aov END) AS m8_aov
        ,MAX(CASE WHEN month_n = 9  THEN cohort_aov END) AS m9_aov
        ,MAX(CASE WHEN month_n = 10 THEN cohort_aov END) AS m10_aov
        ,MAX(CASE WHEN month_n = 11 THEN cohort_aov END) AS m11_aov
        ,MAX(CASE WHEN month_n = 12 THEN cohort_aov END) AS m12_aov
  FROM  olist_am.vw_cohort_monthly_core
 GROUP
    BY  cohort_year_month
 ORDER
    BY  cohort_year_month;



-- row count: 370건
SELECT  COUNT(*) AS row_cnt
  FROM  olist_am.vw_cohort_monthly_core;

-- PK 유니크 확인 -> cnt: 370 / distinct_cnt: 370 / 중복: 0건 / 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT CONCAT(cohort_year_month, '_', month_n)) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT CONCAT(cohort_year_month, '_', month_n)) AS dup_cnt
		,SUM(cohort_year_month IS NULL) AS year_month_blank_cnt
		,SUM(month_n IS NULL) AS month_n_blank_cnt
  FROM  olist_am.vw_cohort_monthly_core;

-- 필수 컬럼(year_month, cohort_size, cohort_active_buyers, cohort_order_cnt, cohort_gorss_revenue) 결측 확인: 0건
-- 	- cohort_aov와 cohort_retention_rate는 결측이 포함될 수 있음
SELECT  SUM(`year_month` IS NULL) AS null_year_month
		,SUM(cohort_size IS NULL) AS null_cohort_size
		,SUM(cohort_active_buyers IS NULL) AS null_active_buyers
		,SUM(cohort_order_cnt IS NULL) AS null_order_cnt
		,SUM(cohort_gross_revenue IS NULL) AS null_gross_revenue
  FROM  olist_am.vw_cohort_monthly_core;

-- 값 범위 확인

-- kpi					neg_kpi		min_kpi			max_kpi
-- ======================================================================
-- cohort_year_month		0	    2016-09			2018-08
-- year_month				0	    2016-09			2018-12
-- month_n					0			0			   27
-- cohort_size				0			1			  7059
-- cohort_active_buyers		0			0			  7059
-- cohort_order_cnt			0			0			  7159
-- cohort_gross_revenue		0		  0.00			1135312.35
-- cohort_retention_rate	0	    0.000000	    1.000000
-- cohort_aov				0		  19.62			 381.00
-- cum_gross_revenue		0		  19.62		    1157915.16
-- cum_order_cnt			0			1			  7301

SELECT  'cohort_year_month' AS kpi
		,0 AS neg_kpi -- CHAR 타입으로 음수 존재 불가
		,MIN(cohort_year_month) AS min_kpi
		,MAX(cohort_year_month) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'year_month' AS kpi
		,0 AS neg_kpi -- CHAR 타입으로 음수 존재 불가
		,MIN(`year_month`) AS min_kpi
		,MAX(`year_month`) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'month_n' AS kpi
		,SUM(month_n < 0) AS neg_kpi
		,MIN(month_n) AS min_kpi
		,MAX(month_n) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cohort_size' AS kpi
		,SUM(cohort_size < 0) AS neg_kpi
		,MIN(cohort_size) AS min_kpi
		,MAX(cohort_size) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cohort_active_buyers' AS kpi
		,SUM(cohort_active_buyers < 0) AS neg_kpi
		,MIN(cohort_active_buyers) AS min_kpi
		,MAX(cohort_active_buyers) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cohort_order_cnt' AS kpi
		,SUM(cohort_order_cnt < 0) AS neg_kpi
		,MIN(cohort_order_cnt) AS min_kpi
		,MAX(cohort_order_cnt) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cohort_gross_revenue' AS kpi
		,SUM(cohort_gross_revenue < 0) AS neg_kpi
		,MIN(cohort_gross_revenue) AS min_kpi
		,MAX(cohort_gross_revenue) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cohort_retention_rate' AS kpi
		,SUM(cohort_retention_rate < 0) AS neg_kpi
		,MIN(cohort_retention_rate) AS min_kpi
		,MAX(cohort_retention_rate) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cohort_aov' AS kpi
		,SUM(cohort_aov < 0) AS neg_kpi
		,MIN(cohort_aov) AS min_kpi
		,MAX(cohort_aov) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core

UNION ALL

SELECT  'cum_gross_revenue' AS kpi
		,SUM(cum_gross_revenue < 0) AS neg_kpi
		,MIN(cum_gross_revenue) AS min_kpi
		,MAX(cum_gross_revenue) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core
  
UNION ALL

SELECT  'cum_order_cnt' AS kpi
		,SUM(cum_order_cnt < 0) AS neg_kpi
		,MIN(cum_order_cnt) AS min_kpi
		,MAX(cum_order_cnt) AS max_kpi
  FROM  olist_am.vw_cohort_monthly_core;


-- 정합성 검증
-- month_n이 0부터 끊김 없이 이어지지 않는 경우: 없음
WITH r AS (
	SELECT  cohort_year_month
			,month_n
			,ROW_NUMBER() OVER (PARTITION BY cohort_year_month ORDER BY month_n) - 1 AS rnk
	  FROM  olist_am.vw_cohort_monthly_core
)
SELECT  *
  FROM  r
 WHERE  month_n <> rnk;

-- cohort_size가 코호트 내에서 상수가 아닌 row(min_size와 max_size가 다른 row): 0건
SELECT  cohort_year_month
		,MIN(cohort_size) AS min_size
		,MAX(cohort_size) AS max_size
  FROM  olist_am.vw_cohort_monthly_core
 GROUP
    BY  cohort_year_month
HAVING  MIN(cohort_size) <> MAX(cohort_size);

-- year_month가 cohort_year_month보다 앞서는 row: 0건
SELECT  *
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  STR_TO_DATE(CONCAT(`year_month`, '-01'), '%Y-%m-%d') < STR_TO_DATE(CONCAT(cohort_year_month, '-01'), '%Y-%m-%d');

-- month_n=0에서 active_buyers와 cohort_size가 다른 row: 0건
SELECT  *
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  month_n = 0
   AND  cohort_active_buyers <> cohort_size;

-- 계산 정합성
-- active_buyers가 0인데 order_cnt 또는 gross_revenue가 0이 아닌 경우: 없음
SELECT  *
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  cohort_active_buyers = 0
   AND  (cohort_order_cnt <> 0 OR cohort_gross_revenue <> 0);

-- retention 계산 검증: 이상 없음
SELECT  cohort_year_month
		,`year_month`
		,month_n
		,cohort_retention_rate AS stored_ret
		,CAST(cohort_active_buyers / NULLIF(cohort_size, 0) AS DECIMAL(10,6)) AS recal
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  cohort_retention_rate IS NOT NULL
   AND  ABS(cohort_retention_rate - CAST(cohort_active_buyers / NULLIF(cohort_size, 0) AS DECIMAL(10,6))) > 0.000001;

-- aov 계산 검증: 이상 없음
SELECT  cohort_year_month
		,`year_month`
		,month_n
		,cohort_aov AS stored_aov
		,CAST(cohort_gross_revenue / NULLIF(cohort_order_cnt, 0) AS DECIMAL(18,2)) AS recal
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  ABS(cohort_aov - CAST(cohort_gross_revenue / NULLIF(cohort_order_cnt, 0) AS DECIMAL(18,2))) > 0.01;

-- 누적 계산 검증
-- 누적 KPI(cum_*)가 감소하는 row: 0건
-- cum_gross_revenue
SELECT  *
  FROM  (
  		SELECT  cohort_year_month
  				,month_n
  				,cum_gross_revenue
  				,LAG(cum_gross_revenue) OVER (PARTITION BY cohort_year_month ORDER BY month_n) AS prev_cum
  		  FROM  olist_am.vw_cohort_monthly_core
  		) AS t
 WHERE  prev_cum IS NOT NULL
   AND  cum_gross_revenue < prev_cum;

-- cum_order_cnt
SELECT  *
  FROM  (
  		SELECT  cohort_year_month
  				,month_n
  				,cum_order_cnt
  				,LAG(cum_order_cnt) OVER (PARTITION BY cohort_year_month ORDER BY month_n) AS prev_cum
  		  FROM  olist_am.vw_cohort_monthly_core
  		) AS t
 WHERE  prev_cum IS NOT NULL
   AND  cum_order_cnt < prev_cum;

-- month_n=0일 때 누적 gross_revenue와 당월 gross_revenue가 다른 row: 0건
SELECT  *
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  month_n = 0
   AND  cum_gross_revenue <> cohort_gross_revenue;

-- month_n=0일 때 누적 order_cnt와 당월 order_cnt가 다른 row: 0건
SELECT  *
  FROM  olist_am.vw_cohort_monthly_core
 WHERE  month_n = 0
   AND  cum_order_cnt <> cohort_order_cnt;

-- 코호트 누적과 코호트 전체 합이 다른 row: 0건
WITH last_m AS (
	SELECT  cohort_year_month
			,MAX(month_n) AS max_m
	  FROM  olist_am.vw_cohort_monthly_core
	 GROUP
	    BY  cohort_year_month
)
SELECT  c.cohort_year_month
		,c.cum_gross_revenue AS last_cum
		,SUM(c2.cohort_gross_revenue) AS total_sum
  FROM  last_m AS l
  JOIN  olist_am.vw_cohort_monthly_core AS c
    ON  c.cohort_year_month = l.cohort_year_month
   AND  c.month_n = l.max_m
  JOIN  olist_am.vw_cohort_monthly_core AS c2
    ON  c2.cohort_year_month = l.cohort_year_month
 GROUP
    BY  c.cohort_year_month
    	,c.cum_gross_revenue
HAVING  ABS(last_cum - total_sum) > 0.001;

-- base view 총합과 core view 총합 일치 여부: 이상 없음(일치)

-- kpi					base_sum		core_sum		diff
-- ===============================================================
-- gross_revenue	  15418394.83	   15418394.83		0.00
-- order_cnt			96470.00		96470.00		0.00
SELECT  'gross_revenue' AS kpi
		,(SELECT SUM(gross_revenue) FROM olist_am.vw_base_customer_monthly_purchase) AS base_sum
		,(SELECT SUM(cohort_gross_revenue) FROM olist_am.vw_cohort_monthly_core) AS core_sum
		,(SELECT SUM(gross_revenue) FROM olist_am.vw_base_customer_monthly_purchase) - (SELECT SUM(cohort_gross_revenue) FROM olist_am.vw_cohort_monthly_core) AS diff

UNION ALL

SELECT  'order_cnt' AS kpi
		,(SELECT SUM(order_cnt) FROM olist_am.vw_base_customer_monthly_purchase) AS base_sum
		,(SELECT SUM(cohort_order_cnt) FROM olist_am.vw_cohort_monthly_core) AS core_sum
		,(SELECT SUM(order_cnt) FROM olist_am.vw_base_customer_monthly_purchase) - (SELECT SUM(cohort_order_cnt) FROM olist_am.vw_cohort_monthly_core) AS diff;







