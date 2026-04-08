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


-- base 월별 KPI 지표 샘플 (날짜 고정: 2017-01 ~ 2018-08)
SELECT  *
  FROM  olist_am.vw_kpi_monthly_core
 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08';


-- ============================================================================================================================================================================================================


/*
 * 월별 거래 안정성 추이:
 * 
 * 	- 월별 거래 안정성 기본 추이
 * 		- 2017년 대비 2018년에 전반적으로 개선되는 흐름
 * 			- failed_rate는 2017년 일부 월에서 1.6% ~ 3.5% 수준까지 나타났으나, 2018년에는 1% 이하 수준으로 낮아짐
 * 		- 2017-11 -> 2017-12 매출 급락 구간에서는 취소율과 실패율이 오히려 하락되는 모습(취소율: 0.0049 -> 0.0019 / 실패율: 0.0160 -> 0.0093)
 * 			- 해당 매출 감소는 운영 불안정성 확대보다는 신규 유입 감소에 따른 거래량 축소로 해석됨
 * 		- 특정 월에서 상대적으로 높은 취소/실패율을 보임(2017-02: 0.0096 / 2018-02: 0.0109 / 2018-08: 0.0129)
 * 			- 이상 구간 후보로 볼 수 있으나 이후 주문 상태 분포 및 카테고리/지역별 분석을 통한 추가 점검 필요
 * 
 * 	- 월별 거래 안정성 추이 + MoM
 * 		- 거래 안정성은 2017년 대비 2018년에 전반적으로 개선되는 흐름을 보이지만, 일부 월에서는 단기적인 변동성이 관찰됨
 * 			- 2017년에는 failed_rate가 1.6% ~ 3.5% 수준
 * 			- 2018년에는 대부분 1% 이하 수준
 * 			- 특히 2018-03 ~ 2018-06 구간은 failed_rate가 0.29% ~ 0.60% 수준으로 매우 낮아짐 -> 전반적인 거래 안정성이 가장 높았던 구간
 * 		- 2017-11 -> 2017-12 매출 급락 구간에서는 거래 안정성 지표가 개선
 * 			- cancel_rate: 0.0049 -> 0.0019 (MoM: -0.6047)
 * 			- unavailable_rate: 0.0111 -> 0.0074 (MoM: -0.3352)
 * 			- failed_rate: 0.0160 -> 0.0093 (MoM: -0.4175)
 * 			- 따라서 해당 매출 감소는 운영 불안정성 확대보다는 신규 유입 감소에 따른 거래량 축소로 해석할 수 있음
 * 		- 상대적으로 높은 취소율 또는 실패율을 보이는 월이 일부 존재
 * 			- 2017-02: failed_rate 0.0348 / mom_failed_rate + 1.1434
 * 			- 2018-02: cancel_rate 0.0109 / mom_cancel_rate + 1.3199
 * 			- 2018-08: cancel_rate 0.0129 / mom_cancel_rate +0.9796
 * 			- 해당 월들은 이상 구간 후보로 볼 수 있으나, 구조적 문제 여부는 이후 주문 상태 분포 및 카테고리/지역별 분석을 통해 추가 확인 필요
 * 		- 월별 변동의 원인이 항상 동일한 상태(cancel/unavailable)은 아님
 * 			- 2017-02와 2017-07은 unavailable_rate 상승이 failed_rate 상승을 주도
 * 			- 2018-02와 2018-08은 unavailable보다 cancel_rate 상승이 failed_rate 상승을 주도
 * 			- 즉, 거래 안정성 악화 구간은 단일한 원인보다는 월별로 상이한 운영 이슈가 반영되었을 가능성이 있음
 * 
 * 	- 상위 위험 월 탐지
 * 		- 취소율 기준 상위 월
 * 			- 2018-08 (cancel_rate: 0.0129)
 * 			- 2017-03 (0.0123)
 * 			- 2018-02 (0.0109)
 * 			- 2017-02 (0.0096)
 * 			- 2017-05 (0.0078)
 * 			- 특히 2018년 일부 월에서 취소율 상승이 관찰됨 -> 특정 카테고리 또는 지역에서 취소가 집중되었을 가능성
 * 		- 미수(unavailable) 비율 기준 상위 월
 * 			- 2017-02 (unavailable_rate: 0.0253)
 * 			- 2017-07 (0.0129)
 * 			- 2017-10 (0.0125)
 * 			- 2017-01 (0.0125)
 * 			- 2017-03 (0.0119)
 * 			- 미수 비율이 높은 월은 대부분 2017년에 집중되어 있음 -> 초기 운영 단계에서 재고/공급 또는 주문 처리 미완료 이슈가 상대적으로 더 빈번했을 가능성
 * 		- 실패율(failed_rate) 기준 상위 월
 * 			- 2017-02 (failed_rate: 0.0348)
 * 			- 2017-03 (0.0242)
 * 			- 2017-07 (0.0199)
 * 			- 2017-10 (0.0181)
 * 			- 2017-01 (0.0163)
 * 
 * 	- 최종적으로 거래 안정성 지표는 2017년 대비 2018년에 전반적으로 개선되는 흐름을 보이며, failed_rate는 대부분 1% 이하 수준으로 유지
 * 	- 특히 2017-12 매출 급락 구간에서도 취소율과 실패율이 하락하여 매출 감소가 운영 문제보다는 신규 유입 감소에서 기인했을 가능성이 더욱 확실시 됨
 */


-- 월별 거래 안정성 기본 추이

-- year_month		total_orders		canceled_orders		unavailable_orders		failed_orders		cancel_rate		unavailable_rate		failed_rate
-- ===============================================================================================================================================================
-- 2017-01				800					   3					10					13				  0.0038			0.0125					0.0163
-- 2017-02				1780				   17					45					62				  0.0096			0.0253					0.0348
-- 2017-03				2682				   33					32					65				  0.0123			0.0119					0.0242
-- 2017-04				2404				   18					9					27				  0.0075			0.0037					0.0112
-- 2017-05				3700				   29					31					60				  0.0078			0.0084					0.0162
-- 2017-06				3245				   16					24					40				  0.0049			0.0074					0.0123
-- 2017-07				4026				   28					52					80				  0.0070			0.0129					0.0199
-- 2017-08				4331				   27					32					59				  0.0062			0.0074					0.0136
-- 2017-09				4285				   20					38					58				  0.0047			0.0089					0.0135
-- 2017-10				4631				   26					58					84				  0.0056			0.0125					0.0181
-- 2017-11				7544				   37					84					121				  0.0049			0.0111					0.0160
-- 2017-12				5673				   11					42					53				  0.0019			0.0074					0.0093
-- 2018-01				7269				   34					48					82				  0.0047			0.0066					0.0113
-- 2018-02				6728				   73					30					103				  0.0109			0.0045					0.0153
-- 2018-03				7211				   26					17					43				  0.0036			0.0024					0.0060
-- 2018-04				6939				   15					5					20				  0.0022			0.0007					0.0029
-- 2018-05				6873				   24					16					40				  0.0035			0.0023					0.0058
-- 2018-06				6167				   18					4					22				  0.0029			0.0006					0.0036
-- 2018-07				6292				   41					18					59				  0.0065			0.0029					0.0094
-- 2018-08				6512				   84					7					91				  0.0129			0.0011					0.0140

SELECT  `year_month`
		,total_orders
		,canceled_orders
		,unavailable_orders
		,failed_orders
		,ROUND(cancel_rate, 4) AS cancel_rate
		,ROUND(unavailable_rate, 4) AS unavailable_rate
		,ROUND(failed_rate, 4) AS failed_rate
  FROM  olist_am.vw_kpi_monthly_cancellation
 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
 ORDER
    BY  `year_month`;


-- 월별 거래 안정성 추이 + MoM

-- year_month		total_orders		canceled_orders		unavailable_orders		failed_orders		cancel_rate		unavailable_rate		failed_rate		mom_cancel_rate		mom_unavailable_rate		mom_failed_rate
-- ====================================================================================================================================================================================================================================
-- 2017-01				800						3					10					13				   0.0038			0.0125				   0.0163			
-- 2017-02				1780					17					45					62				   0.0096			0.0253				   0.0348			1.5469				   1.0225					1.1434
-- 2017-03				2682					33					32					65				   0.0123			0.0119				   0.0242			0.2882				   -0.5281					-0.3042
-- 2017-04				2404					18					9					27				   0.0075			0.0037				   0.0112			-0.3914				   -0.6862					-0.5366
-- 2017-05				3700					29					31					60				   0.0078			0.0084				   0.0162			0.0467				   1.2377					0.4439
-- 2017-06				3245					16					24					40				   0.0049			0.0074				   0.0123			-0.3709				   -0.1172					-0.2398
-- 2017-07				4026					28					52					80				   0.0070			0.0129				   0.0199			0.4105				   0.7463					0.6120
-- 2017-08				4331					27					32					59				   0.0062			0.0074				   0.0136			-0.1037				   -0.4279					-0.3144
-- 2017-09				4285					20					38					58				   0.0047			0.0089				   0.0135			-0.2514				   0.2002					-0.0064
-- 2017-10				4631					26					58					84				   0.0056			0.0125				   0.0181			0.2029				   0.4123					0.3401
-- 2017-11				7544					37					84					121				   0.0049			0.0111				   0.0160			-0.1263				   -0.1109					-0.1158
-- 2017-12				5673					11					42					53				   0.0019			0.0074				   0.0093			-0.6047				   -0.3352					-0.4175
-- 2018-01				7269					34					48					82				   0.0047			0.0066				   0.0113			1.4121				   -0.1081					0.2076
-- 2018-02				6728					73					30					103				   0.0109			0.0045				   0.0153			1.3199				   -0.3247					0.3571
-- 2018-03				7211					26					17					43				   0.0036			0.0024				   0.0060			-0.6676				   -0.4712					-0.6105
-- 2018-04				6939					15					5					20				   0.0022			0.0007				   0.0029			-0.4004				   -0.6942					-0.5167
-- 2018-05				6873					24					16					40				   0.0035			0.0023				   0.0058			0.6152				   2.2288					1.0194
-- 2018-06				6167					18					4					22				   0.0029			0.0006				   0.0036			-0.1641				   -0.7212					-0.3871
-- 2018-07				6292					41					18					59				   0.0065			0.0029				   0.0094			1.2323				   3.4083					1.6288
-- 2018-08				6512					84					7					91				   0.0129			0.0011				   0.0140			0.9796				   -0.6243					0.4902

WITH monthly AS (
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
)
SELECT  `year_month`
		,total_orders
		,canceled_orders
		,unavailable_orders
		,failed_orders
		,ROUND(cancel_rate, 4) AS cancel_rate
		,ROUND(unavailable_rate, 4) AS unavailable_rate
		,ROUND(failed_rate, 4) AS failed_rate
		,ROUND((cancel_rate - LAG(cancel_rate) OVER (ORDER BY `year_month`)) / NULLIF(LAG(cancel_rate) OVER (ORDER BY `year_month`), 0), 4) AS mom_cancel_rate
		,ROUND((unavailable_rate - LAG(unavailable_rate) OVER (ORDER BY `year_month`)) / NULLIF(LAG(unavailable_rate) OVER (ORDER BY `year_month`), 0), 4) AS mom_unavailable_rate
		,ROUND((failed_rate - LAG(failed_rate) OVER (ORDER BY `year_month`)) / NULLIF(LAG(failed_rate) OVER (ORDER BY `year_month`), 0), 4) AS mom_failed_rate
  FROM  monthly
 ORDER
    BY  `year_month`;


-- 상위 위험 월 탐지
-- 취소율 상위 월

-- year_month		total_orders		canceled_orders		cancel_rate
-- ===========================================================================
-- 2018-08				6512					84			   0.0129
-- 2017-03				2682					33			   0.0123
-- 2018-02				6728					73			   0.0109
-- 2017-02				1780					17			   0.0096
-- 2017-05				3700					29			   0.0078

SELECT  `year_month`
		,total_orders
		,canceled_orders
		,ROUND(cancel_rate, 4) AS cancel_rate
  FROM  olist_am.vw_kpi_monthly_cancellation
 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
 ORDER
    BY  cancel_rate DESC
    	,total_orders DESC
 LIMIT  5;

-- 미수율 상위 월

-- year_month		total_orders		unavailable_orders		unavailable_rate
-- =================================================================================
-- 2017-02				1780					45					0.0253
-- 2017-07				4026					52					0.0129
-- 2017-10				4631					58					0.0125
-- 2017-01				800						10					0.0125
-- 2017-03				2682					32					0.0119

SELECT  `year_month`
		,total_orders
		,unavailable_orders
		,ROUND(unavailable_rate, 4) AS unavailable_rate
  FROM  olist_am.vw_kpi_monthly_cancellation
 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
 ORDER
    BY  unavailable_rate DESC
    	,total_orders DESC
 LIMIT  5;

-- 실패율 상위 월

-- year_month		total_orders		failed_orders		failed_rate
-- ============================================================================
-- 2017-02				1780				 62				   0.0348
-- 2017-03				2682				 65				   0.0242
-- 2017-07				4026				 80				   0.0199
-- 2017-10				4631				 84				   0.0181
-- 2017-01				800					 13				   0.0163

SELECT  `year_month`
		,total_orders
		,failed_orders
		,ROUND(failed_rate, 4) AS failed_rate
  FROM  olist_am.vw_kpi_monthly_cancellation
 WHERE  `year_month` BETWEEN '2017-01' AND '2018-08'
 ORDER
    BY  failed_rate DESC
    	,total_orders DESC
 LIMIT  5;


-- ============================================================================================================================================================================================================


/*
 * 주문 상태 분포:
 * 
 * 	- 전체 주문 상태 분포
 * 		- delivered 주문이 전체 주문 중 약 97%를 차지함
 * 		- canceled(0.59%)와 unavailable(0.61%) 주문의 비중은 매우 낮은 수준
 * 		- 즉, 플랫폼 거래 구조에서 주문 실패가 구조적으로 높은 문제가 아니라 일부 월에서 발생한 단기적인 변동일 가능성이 높음
 * 
 * 	- 월별 주문 상태 분포(2017-02 / 2018-08)
 * 		- 2017-02(미수율 상위 월)의 경우 unavailable 주문 비중이 약 2.5%로 가장 높게 나타나며, 주문 실패율 상승이 미배송 또는 주문 미완료 상태 증가에 의해 발생한 것으로 확인됨
 * 		- 2018-08(취소율 상위 월)은 unavailable 비중은 매우 낮은 수준(0.11%)을 유지한 반면, canceled 주문 비중이 약 1.29%로 증가하며 취소 주문 확대가 주요 원인이 됨
 *
 *	- 최종적으로 플랫폼의 거래 구조 자체는 안정적(delivered 주문이 전체 중 97%)이나 일부 월에서 단기적인 변동이 발생함
 *	- 또한 거래 이상 구간은 단일한 운영 문제가 아닌 월별로 서로 다른 요인(unavailable 증가 / cancel 증가)에 의해 발생했음을 알 수 있음
 */


-- 전체 주문 상태 분포

-- order_status		order_cnt		order_share
-- ==================================================
-- delivered		  96211			   0.9709
-- shipped			  1097			   0.0111
-- unavailable		   602			   0.0061
-- canceled			   580			   0.0059
-- processing		   299			   0.0030
-- invoiced			   296			   0.0030
-- created			    5			   0.0001
-- approved			    2			   0.0000

SELECT  fo.order_status
		,COUNT(*) AS order_cnt
		,ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS order_share
  FROM  olist_dm.fact_orders AS fo
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
 GROUP
    BY  fo.order_status
 ORDER
    BY  order_cnt DESC;


-- 월별 주문 상태 분포(거래 이상 상위 월)

-- year_month		order_status		order_cnt		order_share
-- ======================================================================
-- 2017-02			 delivered			  1653			  0.9287
-- 2017-02			 unavailable		   45			  0.0253
-- 2017-02			 processing			   32			  0.0180
-- 2017-02			 shipped			   21			  0.0118
-- 2017-02			 canceled			   17			  0.0096
-- 2017-02			 invoiced			   11			  0.0062
-- 2017-02			 approved			   1			  0.0006
-- 2018-08			 delivered			  6351			  0.9753
-- 2018-08			 canceled			   84			  0.0129
-- 2018-08			 shipped			   47			  0.0072
-- 2018-08			 invoiced			   23			  0.0035
-- 2018-08			 unavailable		   7			  0.0011

SELECT  dd.`year_month`
		,fo.order_status
		,COUNT(*) AS order_cnt
		,ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY dd.`year_month`), 4) AS order_share
  FROM  olist_dm.fact_orders AS fo
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
 WHERE  dd.`year_month` IN ('2018-08', '2017-02')
 GROUP
    BY  dd.`year_month`
    	,fo.order_status
 ORDER
    BY  dd.`year_month`
		,order_cnt DESC;


-- ============================================================================================================================================================================================================


/*
 * 카테고리별 취소 위험:
 * 
 * 	- 카테고리별 취소율 기본 분석
 * 		- 일부 카테고리에서 상대적으로 높은 취소율이 관찰되었으나, 대부분 주문 규모가 작은 카테고리에서 발생한 것으로 나타남
 * 			- fixed_telephony, books_general_interest, small_appliances 등은 취소율이 약 1% 이상
 * 			- 그러나 주문 수가 200 ~ 600건 수준이며 취소 건수 또한 2 ~ 8건 수준으로 플랫폼 전체 취소 구조에 미치는 영향은 미미할 것으로 판단됨
 * 		- 거래 규모가 큰 주요 카테고리(bed_bath_table, health_beauty, sports_leisure, computers_accessories 등)는 취소율이 약 0.2% ~ 0.6% 수준으로 비교적 낮게 나타남
 * 			- 플랫폼 핵심 거래 카테고리는 전반적으로 안정적인 상태를 유지하고 있음
 * 		- 또한 대부분의 카테고리에서 unavailable 주문이 거의 발생하지 않음 -> 카테고리별 거래 실패는 공급 문제보다는 고객 취소(cancel)에 의해 발생하는 경우가 대부분
 * 
 * 	- 카테고리별 취소 기여도
 * 		- 취소 주문은 특정 소수 카테고리에 집중되기보다는 여러 주요 거래 카테고리에 분산되어 발생
 * 		- 취소 건수 기준 상위 카테고리는 sports_leisure(45건, 10.1%), housewares(37건, 8.3%), computers_accessories(35건, 7.9%) 등
 * 			- 그러나 이런 상위 카테고리는 플랫폼에서 거래 규모가 큰 주요 상품군이며, 취소율 자체는 약 0.3% ~ 0.7% 수준으로 비교적 낮음
 * 		- 취소율이 상대적으로 높은 일부 카테고리(fixed_telephony, books_general_interest, small_appliances 등)는 주문 규모가 작아 전체 취소 주문에서 차지하는 비중이 매우 미미함
 * 
 * 	- 취소율 상위 카테고리 Top 10
 * 		- 취소율 기준 상위 카테고리는 fixed_telephony(1.42%), books_general_interest(1.37%)가 상대적으로 높은 취소율을 보임(1.3% 이상)
 * 		- 취소율 상위 10개의 카테고리는 취소율은 높지만 대부분 전체 주문 수가 100 ~ 600건 수준으로 크지 않으며, 취소 건수 또한 1 ~ 8건 수준에 머물러 절대 규모 기준 영향력은 미미함
 * 		- 영문으로 번역이 되지 않은 카테고리(unknown)의 경우 취소율이 1.09%로 비교적 높게 나타남 -> 카테고리 미분류 상품군에서 상대적으로 취소 위험이 높을 가능성
 * 
 * 	- 최종적으로 취소율은 특정 카테고리에서 두드러지게 나타나기 보다는 전체 카테고리에 전반적으로 낮게 나타남
 * 	- 특정 카테고리에서 취소율이 두드러지는 경우도 있으나(1% 이상) 해당 카테고리들의 전체 주문 수가 적기 때문에 거래 이상에 큰 영향을 미친다고 보기 어려움
 */


-- 카테고리별 취소율 기본 분석
SELECT  COALESCE(dp.product_category_name_en, 'unknown') AS cateogry
		,COUNT(DISTINCT foi.order_id) AS total_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN foi.order_id END) AS canceled_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status = 'unavailable' THEN foi.order_id END) AS unavailable_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status IN ('canceled', 'unavailable') THEN foi.order_id END) AS failed_orders
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN foi.order_id END) / NULLIF(COUNT(DISTINCT foi.order_id), 0), 4) AS cancel_rate
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'unavailable' THEN foi.order_id END) / NULLIF(COUNT(DISTINCT foi.order_id), 0), 4) AS unavailable_rate
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status IN ('canceled', 'unavailable') THEN foi.order_id END) / NULLIF(COUNT(DISTINCT foi.order_id), 0), 4) AS failed_rate
  FROM  olist_dm.fact_order_items AS foi
  JOIN  olist_dm.fact_orders AS fo
    ON  fo.order_id = foi.order_id
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
  LEFT
  JOIN  olist_dm.dim_product AS dp
    ON  dp.product_id = foi.product_id
 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
 GROUP
    BY  COALESCE(dp.product_category_name_en, 'unknown')
HAVING  COUNT(DISTINCT foi.order_id) >= 100
 ORDER
    BY  cancel_rate DESC
    	,total_orders DESC;


-- 카테고리별 취소 기여도 (snapshot: Top 10)

-- category					total_orders		canceled_orders			cancel_rate		contrib_to_total_canceled
-- =====================================================================================================================
-- sports_leisure				7701					45					0.0058					0.1011
-- housewares					5875					37					0.0063					0.0831
-- computers_accessories		6671					35					0.0052					0.0787
-- health_beauty				8791					34					0.0039					0.0764
-- toys							3861					28					0.0073					0.0629
-- furniture_decor				6397					24					0.0038					0.0539
-- auto							3886					23					0.0059					0.0517
-- watches_gifts				5619					20					0.0036					0.0449
-- bed_bath_table				9412					18					0.0019					0.0404
-- unknown						1471					16					0.0109					0.0360

WITH category_cancel AS (
	SELECT  COALESCE(dp.product_category_name_en, 'unknown') AS category
			,COUNT(DISTINCT foi.order_id) AS total_orders
			,COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN foi.order_id END) AS canceled_orders
	  FROM  olist_dm.fact_order_items AS foi
	  JOIN  olist_dm.fact_orders AS fo
	    ON  fo.order_id = foi.order_id
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = fo.order_purchase_date_key
	  LEFT
	  JOIN  olist_dm.dim_product AS dp
	    ON  dp.product_id = foi.product_id
	 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
	 GROUP
	    BY  COALESCE(dp.product_category_name_en, 'unknown')
)
SELECT  category
		,total_orders
		,canceled_orders
		,ROUND(canceled_orders / NULLIF(total_orders, 0), 4) AS cancel_rate
		,ROUND(canceled_orders / NULLIF(SUM(canceled_orders) OVER (), 0), 4) AS contrib_to_total_canceled
  FROM  category_cancel
 WHERE  total_orders >= 100
 ORDER
    BY  canceled_orders DESC
    	,cancel_rate DESC;


-- 취소율 상위 카테고리 Top 10

-- category							total_orders		cancel_orders		cancel_rate
-- ===========================================================================================
-- fixed_telephony						212					  3				  0.0142
-- books_general_interest				511					  7				  0.0137
-- home_appliances_2					234					  3				  0.0128
-- small_appliances						630					  8				  0.0127
-- musical_instruments					628					  8				  0.0127
-- construction_tools_safety			167					  2				  0.0120
-- unknown								1471				  16			  0.0109
-- costruction_tools_garden				194				  	  2				  0.0103
-- fashion_male_clothing				111					  1				  0.0090
-- food_drink							227					  2				  0.0088

SELECT  COALESCE(dp.product_category_name_en, 'unknown') AS category
		,COUNT(DISTINCT foi.order_id) AS total_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN foi.order_id END) AS cancel_orders
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN foi.order_id END) / NULLIF(COUNT(DISTINCT foi.order_id), 0), 4) AS cancel_rate
  FROM  olist_dm.fact_order_items AS foi
  JOIN  olist_dm.fact_orders AS fo
    ON  fo.order_id = foi.order_id
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
  LEFT
  JOIN  olist_dm.dim_product AS dp
    ON  dp.product_id = foi.product_id
 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
 GROUP
    BY  COALESCE(dp.product_category_name_en, 'unknown')
HAVING  COUNT(DISTINCT foi.order_id) >= 100
 ORDER
    BY  cancel_rate DESC
    	,total_orders DESC
 LIMIT  10;


-- ============================================================================================================================================================================================================


/*
 * 지역별 취소 위험:
 * 
 * 	- 지역별 취소율 기본 분석
 * 		- 일부 도시에서 상대적으로 높은 취소율이 확인되었지만, 대부분 주문 규모가 100 ~ 200건 수준의 소규모 거래 지역에서 나타남
 * 			- governador valadares_MG(2.84%), itu_SP(2.22%), francisco morato_SP(2%) 등은 비교적 높은 취소율을 보이지만 전체 주문 수 00 ~ 200건, 취소 건수 2 ~ 4건 수준
 * 		- 플랫폼 주요 거래 도시인 sao paulo_SP(15,510건), rio de janeiro_RJ(6,843건), belo horizonte_MG(2,758건) 등은 취소율이 약 0.3% ~ 0.9% 수준으로 비교적 낮음 -> 대형 거래 지역에서는 전반적으로 안정적인 주문 구조 유지
 * 		- 일부 지역에서는 unavailable 주문이 cancel보다 더 많이 발생하는 사례도 관찰됨
 * 			- 지역별 거래 실패는 단순한 취소 문제뿐 아니라 배송 또는 주문 처리 과정과 관련된 운영 요인의 영향을 받을 가능성
 * 
 * 	- 지역별 취소 기여도
 * 		- 취소 주문은 특정 소수 지역에만 집중되기보다는 주요 거래 도시권에 분산되어 발생
 * 		- 취소 건수 기준 상위 지역은 sao paulo_SP(135건, 30.6%), rio de janeiro_RJ(38건, 8.6%), belo horizonte_MG(14건, 3.2%) 등이 있음
 * 			- 해당 지역들은 플랫폼에서 거래 규모가 큰 핵심 도시권
 * 			- 취소율 자체는 대체로 0.5% ~ 1.0% 수준으로 비교적 안정적인 편
 * 		- 취소율이 상대적으로 높은 일부 지역(governador valadares_MG, itu_SP 등)은 주문 규모가 작아 전체 취소 구조에 미치는 영향은 제한적
 * 
 * 	- 최종적으로 취소 및 실패 주문은 특정 지역에서 구조적으로 집중되어 발생하기보다는 전체 거래 규모가 큰 주요 도시권을 중심으로 분산되어 나타나는 것으로 확인됨
 * 	- 따라서 플랫폼 거래 구조는 지역 차원에서도 전반적으로 안정적인 상태를 유지하고 있는 것으로 판단됨
 */


-- 지역별 취소율 기본 분석
SELECT  dc.customer_city_state AS city_state
		,COUNT(DISTINCT fo.order_id) AS total_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN fo.order_id END) AS cancel_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status = 'unavailable' THEN fo.order_id END) AS unavailable_orders
		,COUNT(DISTINCT CASE WHEN fo.order_status IN ('canceled', 'unavailable') THEN fo.order_id END) AS failed_orders
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN fo.order_id END) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 4) AS cancel_rate
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status = 'unavailable' THEN fo.order_id END) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 4) AS unavailable_rate
		,ROUND(COUNT(DISTINCT CASE WHEN fo.order_status IN ('canceled', 'unavailable') THEN fo.order_id END) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 4) AS failed_rate
  FROM  olist_dm.fact_orders AS fo
  JOIN  olist_dm.dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
  JOIN  olist_dm.dim_customer AS dc
    ON  dc.customer_id = fo.customer_id
 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
 GROUP
    BY  dc.customer_city_state
HAVING  COUNT(DISTINCT fo.order_id) >= 100
 ORDER
    BY  cancel_rate DESC
    	,total_orders DESC;


-- 지역별 취소 기여도 (snapshot: Top 10)

-- city_state					total_orders		cancel_orders		cancel_rate		contrib_to_total_cancel
-- ===================================================================================================================
-- sao paulo_SP						15501				135				   0.0087				0.3061
-- rio de janeiro_RJ				6843				38				   0.0056				0.0862
-- belo horizonte_MG				2758				14				   0.0051				0.0317
-- guarulhos_SP						1187				12				   0.0101				0.0272
-- campinas_SP						1437				10				   0.0070				0.0227
-- curitiba_PR						1515				10				   0.0066				0.0227
-- osasco_SP						745					8				   0.0107				0.0181
-- sao bernardo do campo_SP			935					8				   0.0086				0.0181
-- brasilia_DF						2125				7				   0.0033				0.0159
-- goiania_GO						687					7				   0.0102				0.0159

WITH state_cancel AS (
	SELECT  dc.customer_city_state AS city_state
			,COUNT(DISTINCT fo.order_id) AS total_orders
			,COUNT(DISTINCT CASE WHEN fo.order_status = 'canceled' THEN fo.order_id END) AS cancel_orders
	  FROM  olist_dm.fact_orders AS fo
	  JOIN  olist_dm.dim_date AS dd
	    ON  dd.date_key = fo.order_purchase_date_key
	  JOIN  olist_dm.dim_customer AS dc
	    ON  dc.customer_id = fo.customer_id
	 WHERE  dd.`year_month` BETWEEN '2017-01' AND '2018-08'
	 GROUP
	    BY  dc.customer_city_state
)
SELECT  city_state
		,total_orders
		,cancel_orders
		,ROUND(cancel_orders / NULLIF(total_orders, 0), 4) AS cancel_rate
		,ROUND(cancel_orders / NULLIF(SUM(cancel_orders) OVER (), 0), 4) AS contrib_to_total_cancel
  FROM  state_cancel
 WHERE  total_orders >= 100
 ORDER
    BY  cancel_orders DESC;



















