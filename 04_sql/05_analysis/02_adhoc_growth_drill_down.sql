/****************************************************************************************************************************************/


/*
 * File: 02_adhoc_growth_drill_down.sql
 * Description:
 * 	- 2017-11 -> 2017-12 구간을 대상으로 매출 감소의 구조적 원인 파악
 * 	- Delivered 주문을 기준으로 카테고리별 매출 기여도, 신규 vs 재구매 구조 분해, 지역별 성장 구조 분해를 진행
 * 	- 신규 고객은 고객 첫 구매월(first_purchase_year_month)로 정의
 * 
 * Notes:
 * 	- 분석 구간은 매출 급락이 발생한 두 개월로 채택하였습니다. (2017-11, 2017-12)
 * 	- 신규 고객은 고객별 첫 구매월(first_purchase_year_month)로 정의하였습니다.
 * 	- 기여율 계산은 ABS 기반 감소 기여율로 정의하였습니다.
 */


/****************************************************************************************************************************************/


-- base 월별 KPI 지표 샘플
SELECT  *
  FROM  olist_am.vw_kpi_monthly_core
 LIMIT  10;

-- 분석 대상 월 고정
-- 	- 앞서 growth structure에서 정의한 변곡점
-- 		- 성장 피크: 2017-11
-- 		- 급락: 2017-12
SET @m_growth = '2017-11';
SET @m_drop = '2017-12';

-- 성장/급락 수치

-- year_month		gross_revenue		order_cnt		active_buyers		aov		mom_rev_rate		mom_order_cnt_rate		mom_active_buyers_rate		mom_aov_rate
-- =============================================================================================================================================================================
-- 2017-11			  1153229.37		  7288				7182		   158.24	  0.535352				  0.6275					0.6260				  -0.056579
-- 2017-12			   843078.29		  5513				5450		   152.93	  -0.268941				  -0.2436					-0.2412				  -0.033557

WITH t AS (
	SELECT  `year_month`
			,gross_revenue
			,order_cnt
			,active_buyers
			,aov
			,(gross_revenue / NULLIF(LAG(gross_revenue) OVER (ORDER BY `year_month`), 0) - 1) AS mom_rev_rate
			,(order_cnt / NULLIF(LAG(order_cnt) OVER (ORDER BY `year_month`), 0) - 1) AS mom_order_cnt_rate
			,(active_buyers / NULLIF(LAG(active_buyers) OVER (ORDER BY `year_month`), 0) - 1) AS mom_active_buyers_rate
			,(aov / NULLIF(LAG(aov) OVER (ORDER BY `year_month`), 0) - 1) AS mom_aov_rate
	  FROM  olist_am.vw_kpi_monthly_core
)
SELECT  *
  FROM  t
 WHERE  `year_month` IN (@m_growth, @m_drop)
 ORDER
    BY  `year_month`;


-- ====================================================================================================================================================================================================================================================================


/*
 * 카테고리 기여도(카테고리별 매출):
 * 
 * - 월별 카테고리 매출(2017-11 vs 2017-12)
 * 		- 2017-11과 2017-12에서 매출 상위 카테고리(Top 10)는 대부분 유지(9/10)되었음 (housewares 카테고리가 이탈하고, auto가 진입)
 * 		- 상위 카테고리 구성에는 큰 변화가 없어 상품 믹스의 구조적 이동은 관찰되지 않음
 * 		- 상위 카테고리(Top 10) 중 다수 카테고리가 동반 하락하는 패턴
 * 
 * 	- 카테고리 감소액/감소비율 & 전체 감소액 대비 기여율
 * 		- 가장 높은 전체 감소액 대비 기여율이 14.69%로 낮진 않지만, 특정 상품이 매출 감소를 대변한다고 볼 수 없는 수치
 * 		- 2017-12 매출 감소액 중 상위 10개 카테고리가 약 81%를 설명함 -> 감소가 특정 단일 카테고리에 집중되기보다는 상위 카테고리 전반에서 동반 하락한 구조
 * 		- 카테고리 매출 증가는 상위 10개의 카테고리에서 발견되지 않음
 * 		- 카테고리 매출 감소 비율은 -22.25% ~ -62.90% 정도로 폭이 넓지만, -62.90%인 office_furniture의 감소액 대비 기여율이 4%이고, -22.25%의 감소액 대비 기여율이 6%로 비율상 붕괴 카테고리는 일부 있지만 전체 급락의 주된 원인이라고 할 수는 없음
 * 
 * 
 * 	- 최종적으로 매출 급락은 특정 상품군 붕괴나 카테고리 믹스 이동에 의한 것이 아니라, 주요 카테고리 전반에서 발생한 동반 하락 구조를 보임
 * 	- 즉, 동일 기간 주문 수(약 -24%) 및 활성 구매자 수 감소(약 -24%)와 일관된 패턴을 보이며, 전반적인 거래량 감소형 하락으로 해석 가능
*/


-- 월별 카테고리 매출
WITH base AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dp.product_category_name_en, 'unknown') AS category
			,vdoi.order_id
			,vdoi.item_total_value
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_product AS dp
	    ON  dp.product_id = vdoi.product_id
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
)
SELECT  `year_month`
		,category
		,SUM(item_total_value) AS gross_revenue
		,COUNT(DISTINCT order_id) AS order_cnt
  FROM  base
 GROUP
    BY  `year_month`
    	,category
 ORDER
    BY  `year_month`
    	,gross_revenue DESC;

-- 월별 카테고리 매출 Top 10

-- year_month		category				rev
-- =================================================
-- 2017-11		  bed_bath_table		104963.58
-- 2017-11		  watches_gifts			103135.30
-- 2017-11		  health_beauty			88629.80
-- 2017-11		computers_accessories	79910.93
-- 2017-11		  furniture_decor		76687.92
-- 2017-11		  sports_leisure		73149.22
-- 2017-11			  toys				71922.79
-- 2017-11			cool_stuff			62263.06
-- 2017-11		   garden_tools			56437.34
-- 2017-11			housewares			42279.36
-- ==================================================
-- 2017-12		   watches_gifts		74898.08
-- 2017-12		   health_beauty		68907.78
-- 2017-12		   sports_leisure		68184.69
-- 2017-12				toys			63462.54
-- 2017-12		   bed_bath_table		59378.49
-- 2017-12				auto			44775.58
-- 2017-12		computers_accessories	42967.29
-- 2017-12			cool_stuff			42704.87
-- 2017-12		  furniture_decor		38228.09
-- 2017-12		   garden_tools			34143.13

WITH cat AS (
	SELECT  `year_month`
			,COALESCE(dp.product_category_name_en, 'unknown') AS category
			,SUM(vdoi.item_total_value) AS rev
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_product AS dp
	    ON  dp.product_id = vdoi.product_id
	 WHERE  `year_month` IN (@m_growth, @m_drop)
	 GROUP
	    BY  `year_month`
	    	,category
),
r AS (
	SELECT  *
			,ROW_NUMBER() OVER (PARTITION BY `year_month` ORDER BY rev DESC) AS rn
	  FROM  cat
)
SELECT  `year_month`
		,category
		,rev
  FROM  r
 WHERE  rn <= 10
 ORDER
    BY  `year_month`
    	,rev DESC;

-- 카테고리 감소액/감소비율 & 전체 감소액 대비 기여율 Top 10

-- category					rev_growth		rev_drop		drop_rate		rev_diff		contrib_to_total_drop
-- =======================================================================================================================
-- bed_bath_table			 104963.58		59378.49		-0.434294		-45585.09			0.146977
-- furniture_decor			 76687.92		38228.09		-0.501511		-38459.83			0.124004
-- computers_accessories	 79910.93		42967.29		-0.462310		-36943.64			0.119115
-- watches_gifts			 103135.30		74898.08		-0.273788		-28237.22			0.091043
-- garden_tools				 56437.34		34143.13		-0.395026		-22294.21			0.071882
-- health_beauty			 88629.80		68907.78		-0.222521		-19722.02			0.063588
-- cool_stuff				 62263.06		42704.87		-0.314122		-19558.19			0.063060
-- telephony				 31817.78		16452.82		-0.482905		-15364.96			0.049540
-- office_furniture			 22687.87		8415.70			-0.629066		-14272.17			0.046017
-- housewares				 42279.36		31165.49		-0.262868		-11113.87			0.035834

WITH cat AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dp.product_category_name_en, 'unknown') AS category
			,SUM(vdoi.item_total_value) AS rev
	  FROM  olist_dm.vw_delivered_order_items vdoi
	  JOIN  olist_dm.dim_product AS dp
	    ON  dp.product_id = vdoi.product_id
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
	 GROUP
	    BY  vdoi.`year_month`
	    	,category
),
pivot AS (
	SELECT  category
			,SUM(CASE WHEN `year_month` = @m_growth THEN rev ELSE 0 END) AS rev_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN rev ELSE 0 END) AS rev_drop
	  FROM  cat
	 GROUP
	    BY  category
),
tot AS (
	SELECT  SUM(CASE WHEN `year_month` = @m_growth THEN rev ELSE 0 END) AS total_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN rev ELSE 0 END) AS total_drop
	  FROM  cat
)
SELECT  p.category
		,p.rev_growth
		,p.rev_drop
		,(p.rev_drop - p.rev_growth) / NULLIF(p.rev_growth, 0) AS drop_rate
		,(p.rev_drop - p.rev_growth) AS rev_diff
		,CASE WHEN (p.rev_drop - p.rev_growth) < 0 THEN ABS(p.rev_drop - p.rev_growth) / NULLIF(ABS(t.total_drop - t.total_growth), 0) 
			  ELSE 0
			  END AS contrib_to_total_drop
  FROM  pivot AS p
 CROSS
  JOIN  tot AS t
 ORDER
    BY  rev_diff ASC
 LIMIT  10;


-- ====================================================================================================================================================================================================================================================================


/*
 * 신규 vs 재구매 성장 구조 분해:
 * 
 * 	- 신규/재구매 KPI 비교
 * 		- 매출 ->  신규: 1,135,312.35 -> 824,253.26 (약 -27.4%) / 재구매: 17,917.02 -> 18,825.03 (약 +5.1%)
 * 		- 주문 수 -> 신규: 7,159 -> 5,395 (약 -24.6%) / 재구매: 129 -> 118 (약 -8.5%)
 * 		- 구매자 수 -> 신규: 7,059 -> 5,338 (약 -24.4%) / 재구매: 123 -> 112 (약 -8.9%)
 * 		- aov -> 신규: 158.585326 -> 152.780956 (약 -3.66%) / 재구매: 138.891628 -> 159.534153 (약 14.9%)
 * 
 * 	- 신규/재구매 급락 기여도
 * 		- 신규 매출 감소: -27.4% / 재구매 매출 증가: 5.1%
 * 		- 신규 매출 감소가 전체 급락을 사실상 대부분 설명
 * 		- 재구매는 소폭 증가하며 급락을 일부 상쇄
 * 
 * 	- 구조 분해(변화율)
 * 		- 신규 구매자 수의 변화(-24.4%)가 매출 변화에 가장 크게 작용함 / 재구매 구매자 수의 변화는 -8.9%로 신규 구매자 수의 변화에 비해 미미
 * 		- 구매자 당 주문 수는 신규와 재구매 각각 -0.35%, 0.45%로 매출 감소에 영향을 미친다고 보기 어려움
 * 		- aov는 신규와 재구매 각각 -3.66%, 14.86%로 신규 aov는 소폭 하락하였으나, 재구매 aov는 증가하였음
 * 
 * 	- 최종적으로 2017-12 매출 급락은 재구매 붕괴가 아니라 신규 유입 감소(구매자 수 감소)에 기반한 거래량 감소로 해석됨
 */


-- 신규/재구매 KPI

-- year_month		buyer_type		buyers		revenue		orders		items		aov		items_per_order		order_per_buyer
-- ====================================================================================================================================
-- 2017-11			   new			 7059		1135312.35	7159		8319	158.585326		1.1620				1.0142
-- 2017-11			  repeat		 123		17917.02	129			155		138.891628		1.2016				1.0488
-- 2017-12			   new			 5338		824253.26	5395		6044	152.780956		1.1203				1.0107
-- 2017-12			  repeat		 112		18825.03	118			143		159.534153		1.2119				1.0536

WITH base AS (
	SELECT  `year_month`
			,CASE WHEN is_new_buyer = 1 THEN 'new'
				  WHEN is_repeat_buyer =1 THEN 'repeat'
				  ELSE 'unknown'
			END AS buyer_type
			,customer_unique_id
			,gross_revenue
			,order_cnt
			,item_cnt
	  FROM  olist_am.vw_base_customer_monthly_purchase vbcmp
	 WHERE  `year_month` IN (@m_growth, @m_drop)
)
SELECT  `year_month`
		,buyer_type
		,COUNT(DISTINCT customer_unique_id) AS buyers
		,SUM(gross_revenue) AS revenue
		,SUM(order_cnt) AS orders
		,SUM(item_cnt) AS items
		,SUM(gross_revenue) / NULLIF(SUM(order_cnt), 0) AS aov
		,SUM(item_cnt) / NULLIF(SUM(order_cnt), 0) AS items_per_order
		,SUM(order_cnt) / NULLIF(COUNT(DISTINCT customer_unique_id), 0) AS order_per_buyer
  FROM  base
 GROUP
    BY  `year_month`
    	,buyer_type
 ORDER
    BY  `year_month`
    	,buyer_type;

-- 신규/재구매 급락 기여도

-- buyer_type		rev_growth		rev_drop		rev_diff		drop_rate		contrib_to_total_drop
-- =================================================================================================================
-- new				1135312.35	    824253.26	   -311059.09		-0.273985			1.002928
-- repeat			 17917.02	    18825.03		  908.01		0.050679			0.002928

WITH agg AS (
	SELECT  `year_month`
			,CASE WHEN is_new_buyer = 1 THEN 'new'
				  WHEN is_repeat_buyer = 1 THEN 'repeat'
				  ELSE 'unknown'
			END AS buyer_type
			,SUM(gross_revenue) AS revenue
	  FROM  olist_am.vw_base_customer_monthly_purchase
	 WHERE  `year_month` IN (@m_growth, @m_drop)
	 GROUP
	    BY  `year_month`
	    	,buyer_type
),
pivot AS (
	SELECT  buyer_type
			,SUM(CASE WHEN `year_month` = @m_growth THEN revenue ELSE 0 END) AS rev_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN revenue ELSE 0 END) AS rev_drop
	  FROM  agg
	 GROUP
	    BY  buyer_type
),
tot AS (
	SELECT  SUM(CASE WHEN `year_month` = @m_growth THEN revenue ELSE 0 END) AS total_growth
	  		,SUM(CASE WHEN `year_month` = @m_drop THEN revenue ELSE 0 END) AS total_drop
	  FROM  agg
)
SELECT  p.buyer_type
		,p.rev_growth
		,p.rev_drop
		,(p.rev_drop - p.rev_growth) AS rev_diff
		,(p.rev_drop - p.rev_growth) / NULLIF(p.rev_growth, 0) AS drop_rate
		,ABS((p.rev_drop - p.rev_growth) / NULLIF((t.total_drop - t.total_growth), 0)) AS contrib_to_total_drop
  FROM  pivot AS p
 CROSS
  JOIN  tot AS t
 ORDER
    BY  rev_diff ASC;
  

-- 신규/재구매 KPI 변화율 비교

-- buyer_type		rev_growth		rev_drop		rev_change_rate		buyers_change_rate		orders_per_buyer_change_rate		aov_change_rate
-- =======================================================================================================================================================
-- new				1135312.35		824253.26			-0.273985			-0.2438						-0.00345100					 -0.0366009274
-- repeat			 17917.02		18825.03			0.050679			-0.0894						0.00457666					 0.1486232489

WITH agg AS (
	SELECT  `year_month`
			,CASE WHEN is_new_buyer = 1 THEN 'new'
				  WHEN is_repeat_buyer = 1 THEN 'repeat'
				  ELSE 'unknown'
			END AS buyer_type
			,COUNT(DISTINCT customer_unique_id) AS buyers
			,SUM(gross_revenue) AS revenue
			,SUM(order_cnt) AS orders
			,SUM(gross_revenue) / NULLIF(SUM(order_cnt), 0) AS aov
			,SUM(order_cnt) / NULLIF(COUNT(DISTINCT customer_unique_id), 0) AS orders_per_buyer
	  FROM  olist_am.vw_base_customer_monthly_purchase AS vbcmp
	 WHERE  `year_month` IN (@m_growth, @m_drop)
	 GROUP
	    BY  `year_month`
	    	,buyer_type
),
pivot AS (
	SELECT  buyer_type
			,MAX(CASE WHEN `year_month` = @m_growth THEN buyers END) AS buyers_growth
			,MAX(CASE WHEN `year_month` = @m_drop THEN buyers END) AS buyers_drop
			,MAX(CASE WHEN `year_month` = @m_growth THEN orders_per_buyer END) AS opb_growth
			,MAX(CASE WHEN `year_month` = @m_drop THEN orders_per_buyer END) AS opb_drop
			,MAX(CASE WHEN `year_month` = @m_growth THEN aov END) AS aov_growth
			,MAX(CASE WHEN `year_month` = @m_drop THEN aov END) AS aov_drop
			,MAX(CASE WHEN `year_month` = @m_growth THEN revenue END) AS rev_growth
			,MAX(CASE WHEN `year_month` = @m_drop THEN revenue END) AS rev_drop
	  FROM  agg
	 GROUP
	    BY  buyer_type
)
SELECT  buyer_type
		,rev_growth
		,rev_drop
		,(rev_drop / NULLIF(rev_growth, 0) - 1) AS rev_change_rate
		,(buyers_drop / NULLIF(buyers_growth, 0) - 1) AS buyers_change_rate
		,(opb_drop / NULLIF(opb_growth, 0) - 1) AS orders_per_buyer_change_rate
		,(aov_drop / NULLIF(aov_growth, 0) - 1) AS aov_change_rate
  FROM  pivot
 ORDER
    BY  rev_change_rate ASC;


-- ====================================================================================================================================================================================================================================================================


/*
 * 지역별 성장 구조 분해:
 * 
 * 	- 월별 city_state별 매출
 * 		- 2017-11과 2017-12에서 매출 상위 city_state(Top 10)는 대부분 유지(8/10)되었음 (fortaleza_CE, osasco_SP가 이탈하고 recife_PE, guarulhos_SP가 진입)
 * 		- 핵심 대도시권(Top 1~4)은 그대로 유지
 * 		- 상위 city_state에서 매출 감소와 주문 수 감소가 동반하는 패턴이 보임 -> 거래량 감소로 인한 매출 감소일 가능성이 있음
 * 
 * 	- city_state별 감소액 & 전체 감소액 대비 기여율
 * 		- 상위 10개 city_state의 감소 기여율 합은 약 40% 수준으로 급락이 특정 도시에 집중되었다기 보다, 주요 도시권 전반에서 동반 하락한 구조
 * 		- sao paulo_SP(-42,183.96 / -28.6%), rio de janeiro_RJ(-27,009.77/ -28.0%)와 같이 규모가 큰 도시가 평균적인 하락률로 감소
 * 		- 일부 중소 도시(botucatu_SP, divinopolis_MG)에서 -80~-90% 감소율을 보이나, 기여율은 2~3% 수준으로 전체 급락의 주된 원인은 아님
 * 
 * 	- city_state별 매출, 구매자 수 감소 & 감소 대비 기여율 (신규 구매자 기준)
 * 		- 상위 10개 city_state의 신규 매출 감소 기여율 합은 약 40% 수준으로 특정 도시 붕괴가 아닌 주요 도시권 전반에서 동반 하락하는 모습
 * 		- 특정 대도시에서의 감소액과 기여율이 큼(sao paulo_SP: -43,200.87, 13.9% / rio de janeiro_RJ: -27,452, 8.8%) -> 두 도시의 기여율 합계가 약 22~23% 수준으로 규모가 큰 도시가 전체 매출 감소율(-26.9%)과 유사한 수준의 하락률로 감소하는 모습을 보임
 * 		- 신규 구매자 수 역시 SP(-252), RJ(-167)이 크게 나타나지만, Top 10 기여도 합계가 약 40% 수준으로 특정 도시 집중 붕괴는 아님
 * 
 * 	- 최종적으로 2017-12 급락은 지역 믹스 이동이나 특정 도시 붕괴가 아니라, 전국 주요 도시권의 신규 유입 감소에 따른 거래량 하락으로 해석됨
 */


-- 월별 city_state별 매출 Top 10

-- year_month		city_state			gross_revenue		order_cnt
-- ========================================================================
-- 2017-11		   sao paulo_SP			  147240.90			  1081
-- 2017-11		 rio de janeiro_RJ		  96601.46			  556
-- 2017-11		 belo horizonte_MG		  30725.96			  212
-- 2017-11		    brasilia_DF			  27372.55			  144
-- 2017-11		  porto alegre_RS		  20920.66			  110
-- 2017-11		    curitiba_PR			  17896.52			  129
-- 2017-11		    salvador_BA			  14060.86			  88
-- 2017-11			campinas_SP			  13292.94			  89
-- 2017-11			fortaleza_CE		  11899.25			  58
-- 2017-11			 osasco_SP			  9125.62			  58
-- ========================================================================
-- 2017-12			são paulo_SP		  105056.94			  820
-- 2017-12		 rio de janeiro_RJ		  69591.69			  386
-- 2017-12		 belo horizonte_MG		  27832.91			  173
-- 2017-12			brasilia_DF			  18191.56			  117
-- 2017-12			salvador_BA			  14154.84			  91
-- 2017-12			campinas_SP			  11802.00			  79
-- 2017-12			curitiba_PR			  10604.11			  80
-- 2017-12		  porto alegre_RS		  9792.20			  69
-- 2017-12			 recife_PE			  7353.36			  38
-- 2017-12			guarulhos_SP		  7164.06			  60

WITH base AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dg.geolocation_city_state, 'unknown') AS city_state
			,vdoi.order_id
			,vdoi.item_total_value AS revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  LEFT
	  JOIN  olist_dm.dim_geolocation AS dg
	    ON  dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
),
agg AS (
	SELECT  `year_month`
			,city_state
			,SUM(revenue) AS gross_revenue
			,COUNT(DISTINCT order_id) AS order_cnt
	  FROM  base
	 GROUP
	    BY  `year_month`
	    	,city_state
),
r AS (
	SELECT  *
			,ROW_NUMBER() OVER (PARTITION BY `year_month` ORDER BY gross_revenue DESC) AS rnk
	  FROM  agg
)
SELECT  `year_month`
		,city_state
		,gross_revenue
		,order_cnt
  FROM  r
 WHERE  rnk <= 10
 ORDER
    BY  `year_month`
    	,gross_revenue DESC;


-- city_state별 감소액 & 전체 감소액 대비 기여율 Top 10

-- city_state			rev_growth		rev_drop		rev_diff		drop_rate		contrib_to_total_drop
-- =================================================================================================================
-- sao paulo_SP			147240.90		105056.94		-42183.96		-0.286496			0.136011
-- rio de janeiro_RJ	96601.46		69591.69		-27009.77		-0.279600			0.087086
-- porto alegre_RS		20920.66		9792.20			-11128.46		-0.531936			0.035881
-- brasilia_DF			27372.55		18191.56		-9180.99		-0.335409			0.029602
-- divinopolis_MG		8783.09			1043.72			-7739.37		-0.881167			0.024954
-- curitiba_PR			17896.52		10604.11		-7292.41		-0.407476			0.023512
-- fortaleza_CE			11899.25		5464.38			-6434.87		-0.540779			0.020748
-- osasco_SP			9125.62			4318.95			-4806.67		-0.526723			0.015498
-- botucatu_SP			4864.65			258.33			-4606.32		-0.946896			0.014852
-- juiz de fora_MG		6833.80			2527.86			-4305.94		-0.630095			0.013883

WITH base AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dg.geolocation_city_state, 'unknown') AS city_state
			,SUM(vdoi.item_total_value) AS revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  LEFT
	  JOIN  olist_dm.dim_geolocation AS dg
	    ON  dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
	 GROUP
	    BY  vdoi.`year_month`
	    	,city_state
),
pivot AS (
	SELECT  city_state
			,SUM(CASE WHEN `year_month` = @m_growth THEN revenue ELSE 0 END) AS rev_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN revenue ELSE 0 END) AS rev_drop
	  FROM  base
	 GROUP
	    BY  city_state
),
tot AS (
	SELECT  SUM(CASE WHEN `year_month` = @m_growth THEN revenue ELSE 0 END) AS total_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN revenue ELSE 0 END) AS total_drop
	  FROM  base
)
SELECT  p.city_state
		,p.rev_growth
		,p.rev_drop
		,(p.rev_drop - p.rev_growth) AS rev_diff
		,(p.rev_drop - p.rev_growth) / NULLIF(p.rev_growth, 0) AS drop_rate
		,ABS(p.rev_drop - p.rev_growth) / NULLIF(ABS(t.total_drop - t.total_growth), 0) AS contrib_to_total_drop
  FROM  pivot AS p
 CROSS
  JOIN  tot AS t
 ORDER
    BY  rev_diff ASC
 LIMIT  10;


-- city_state별 신규 매출 Top 10

-- year_month		city_state			new_revenue		new_orders		new_buyers
-- ======================================================================================
-- 2017-11			sao paulo_SP		145396.45			1065			1044
-- 2017-11		  rio de janeiro_RJ		95062.69			546				538
-- 2017-11		  belo horizonte_MG		29215.57			207				207
-- 2017-11			brasilia_DF			27276.67			142				140
-- 2017-11		   porto alegre_RS		20866.05			109				109
-- 2017-11			curitiba_PR			17799.72			128				124
-- 2017-11			campinas_SP			13292.94			89				87
-- 2017-11			salvador_BA			13016.51			84				81
-- 2017-11		   fortaleza_CE			11827.60			57				57
-- 2017-11			osasco_SP			9086.02				57				56
-- ======================================================================================
-- 2017-12		   sao paulo_SP			102195.58			803				792
-- 2017-12		  rio de janeiro_RJ		67610.18			374				371
-- 2017-12		  belo horizonte_MG		27204.06			167				167
-- 2017-12			brasília_DF			18095.34			116				116
-- 2017-12			salvador_BA			13873.86			90				87
-- 2017-12			campinas_SP			11746.62			78				76
-- 2017-12			curitiba_PR			10604.11			80				79
-- 2017-12		  porto alegre_RS		9445.40				67				66
-- 2017-12			recife_PE			7292.71				37				35
-- 2017-12			guarulhos_SP		7098.35				59				59

WITH base AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dg.geolocation_city_state, 'unknown') AS city_state
			,dc.customer_unique_id
			,CASE WHEN vdoi.`year_month` = vcfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_new
			,vdoi.order_id
			,vdoi.item_total_value AS revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  LEFT
	  JOIN  olist_dm.dim_geolocation AS dg
	    ON  dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
	  JOIN  olist_dm.vw_customer_first_purchase_month AS vcfpm
	    ON  vcfpm.customer_unique_id = dc.customer_unique_id
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
),
agg AS (
	SELECT  `year_month`
			,city_state
			,SUM(CASE WHEN is_new = 1 THEN revenue ELSE 0 END) AS new_revenue
			,COUNT(DISTINCT CASE WHEN is_new = 1 THEN order_id END) AS new_orders
			,COUNT(DISTINCT CASE WHEN is_new = 1 THEN customer_unique_id END) AS new_buyers
	  FROM  base
	 GROUP
	    BY  `year_month`
	    	,city_state
),
r AS (
	SELECT  *
			,ROW_NUMBER() OVER (PARTITION BY `year_month` ORDER BY new_revenue DESC) AS rnk
	  FROM  agg
)
SELECT  `year_month`
		,city_state
		,new_revenue
		,new_orders
		,new_buyers
  FROM  r
 WHERE  rnk <= 10
 ORDER
    BY  `year_month`
    	,new_revenue DESC;

-- city_state별 신규 매출 감소액 & 기여율

-- city_state			rev_growth		rev_drop		rev_diff		drop_rate		contrib_to_total_drop
-- ===================================================================================================================
-- sao paulo_SP			145396.45		102195.58		-43200.87		-0.297125			0.138883
-- rio de janeiro_RJ	95062.69		67610.18		-27452.51		-0.288783			0.088255
-- porto alegre_RS		20866.05		9445.40			-11420.65		-0.547332			0.036715
-- brasilia_DF			27276.67		18095.34		-9181.33		-0.336600			0.029516
-- divinopolis_MG		8783.09			1043.72			-7739.37		-0.881167			0.024881
-- curitiba_PR			17799.72		10604.11		-7195.61		-0.404254			0.023133
-- fortaleza_CE			11827.60		5464.38			-6363.22		-0.537998			0.020457
-- osasco_SP			9086.02			4188.69			-4897.33		-0.538996			0.015744
-- juiz de fora_MG		6833.80			2141.53			-4692.27		-0.686627			0.015085
-- botucatu_SP			4864.65			258.33			-4606.32		-0.946896			0.014809

WITH base AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dg.geolocation_city_state, 'unknown') AS city_state
			,dc.customer_unique_id
			,CASE WHEN vdoi.`year_month` = vcfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_new
			,vdoi.item_total_value AS revenue
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  LEFT
	  JOIN  olist_dm.dim_geolocation AS dg
	    ON  dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
	  JOIN  olist_dm.vw_customer_first_purchase_month AS vcfpm
	    ON  vcfpm.customer_unique_id = dc.customer_unique_id
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
),
new_month_city AS (
	SELECT  `year_month`
			,city_state
			,SUM(revenue) AS new_revenue
	  FROM  base
	 WHERE  is_new = 1
	 GROUP
	    BY  `year_month`
	    	,city_state
),
pivot AS (
	SELECT  city_state
			,SUM(CASE WHEN `year_month` = @m_growth THEN new_revenue ELSE 0 END) AS rev_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN new_revenue ELSE 0 END) AS rev_drop
	  FROM  new_month_city
	 GROUP
	    BY  city_state
),
tot AS (
	SELECT  SUM(CASE WHEN `year_month` = @m_growth THEN new_revenue ELSE 0 END) AS total_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN new_revenue ELSE 0 END) AS total_drop
	  FROM  new_month_city
)
SELECT  p.city_state
		,p.rev_growth
		,p.rev_drop
		,(p.rev_drop - p.rev_growth) AS rev_diff
		,(p.rev_drop - p.rev_growth) / NULLIF(p.rev_growth, 0) AS drop_rate
		,ABS(p.rev_drop - p.rev_growth) / NULLIF(ABS(t.total_drop - t.total_growth), 0) AS contrib_to_total_drop
  FROM  pivot AS p
 CROSS
  JOIN  tot AS t
 ORDER
    BY  rev_diff ASC
 LIMIT  10;

-- city_state별 신규 구매자 감소 & 기여율

-- city_state			buyers_growth		buyers_drop		buyers_diff		drop_rate		contrib_to_total_drop
-- ==========================================================================================================================
-- sao paulo_SP				1044				792				-252		-0.2414					0.1463
-- rio de janeiro_RJ		538					371				-167		-0.3104					0.0970
-- curitiba_PR				124					79				-45			-0.3629					0.0261
-- porto alegre_RS			109					66				-43			-0.3945					0.0250
-- belo horizonte_MG		207					167				-40			-0.1932					0.0232
-- brasilia_DF				140					116				-24			-0.1714					0.0139
-- osasco_SP				56					35				-21			-0.3750					0.0122
-- vitória_ES				39					21				-18			-0.4615					0.0105
-- fortaleza_CE				57					40				-17			-0.2982					0.0099
-- sorocaba_SP				52					35				-17			-0.3269					0.0099

WITH base AS (
	SELECT  vdoi.`year_month`
			,COALESCE(dg.geolocation_city_state, 'unknown') AS city_state
			,dc.customer_unique_id
			,CASE WHEN vdoi.`year_month` = vcfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_new
	  FROM  olist_dm.vw_delivered_order_items AS vdoi
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = vdoi.customer_id
	  LEFT
	  JOIN  olist_dm.dim_geolocation AS dg
	    ON  dg.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
	  JOIN  olist_dm.vw_customer_first_purchase_month AS vcfpm
	    ON  vcfpm.customer_unique_id = dc.customer_unique_id
	 WHERE  vdoi.`year_month` IN (@m_growth, @m_drop)
),
new_buyers_month_city AS (
	SELECT  `year_month`
			,city_state
			,COUNT(DISTINCT customer_unique_id) AS new_buyers
	  FROM  base
	 WHERE  is_new = 1
	 GROUP
	    BY  `year_month`
	    	,city_state
),
pivot AS (
	SELECT  city_state
			,SUM(CASE WHEN `year_month` = @m_growth THEN new_buyers ELSE 0 END) AS buyers_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN new_buyers ELSE 0 END) AS buyers_drop
	  FROM  new_buyers_month_city
	 GROUP
	    BY  city_state
),
tot AS (
	SELECT  SUM(CASE WHEN `year_month` = @m_growth THEN new_buyers ELSE 0 END) AS total_growth
			,SUM(CASE WHEN `year_month` = @m_drop THEN new_buyers ELSE 0 END) AS total_drop
	  FROM  new_buyers_month_city
)
SELECT  p.city_state
		,p.buyers_growth
		,p.buyers_drop
		,(p.buyers_drop - p.buyers_growth) AS buyers_diff
		,(p.buyers_drop - p.buyers_growth) / NULLIF(p.buyers_growth, 0) AS drop_rate
		,ABS(p.buyers_drop - p.buyers_growth) / NULLIF(ABS(t.total_drop - t.total_growth), 0) AS contrib_to_total_drop
  FROM  pivot AS p
 CROSS
  JOIN  tot AS t
 ORDER
    BY  buyers_diff ASC
 LIMIT  10;
