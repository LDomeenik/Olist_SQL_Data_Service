/******************************************************************************************************************************************************/


/*
 * File: dm_dim_date.sql
 * Description:
 * 	- Data MArt 공용 날짜 차원(dim_date) 생성 및 적재
 * 	- Grain: 1 row = 1 day
 * 	- ISO 주차/요일 기준 적용
 * 
 * Note:
 * 	- 해당 dim_date 테이블은 Sales Mart와 Operations Mart에서 모두 dimension 테이블로 사용되는 공용 차원 테이블입니다.
 * 	- source 테이블 없이 캘린더를 생성한 테이블입니다. (1 row = 1 day)
 * 	- dim_date의 범위를 다음과 같이 고정하였습니다.
 * 		- 시작일: 주문 관련 주요 날짜 컬럼들의 최소값 - 1 달
 * 		- 종료일: 주문 관련 주요 날짜 컬럼들의 최대값 + 1 달
 * 	- 캘린더 날짜를 기준으로 1 row 당 날짜의 파생 컬럼들을 생성하였습니다. (year/quarter/month/week_of_year/day/day_or_week)
 * 	- 이후 분석의 용이성을 위해 플래그 컬럼을 통해 주중/주말 여부와 월의 시작/끝 여부를 파악하였습니다. (is_weekend / is_month_start / is_month_end)
*/


/******************************************************************************************************************************************************/


USE olist_dm;


-- ===========================================================================================================================================


DROP TABLE IF EXISTS olist_dm.dim_date;

-- DDL(테이블 생성)
CREATE TABLE olist_dm.dim_date (
	date_key		INT			NOT NULL,
	date			DATE		NOT NULL,
	year			SMALLINT	NOT NULL,
	quarter		TINYINT		NOT NULL,
	month			TINYINT		NOT NULL,
	day			TINYINT		NOT NULL,
	`year_month`	CHAR(7)		NOT NULL,
	year_quarter	CHAR(7)		NOT NULL,
	week_of_year	TINYINT		NOT NULL,
	day_of_week		TINYINT		NOT NULL,
	day_name		VARCHAR(9)	NOT NULL,
	is_weekend		TINYINT		NOT NULL,
	is_month_start	TINYINT		NOT NULL,
	is_month_end	TINYINT		NOT NULL,
	
	-- PK, INDEX
	PRIMARY KEY (date_key),
 	INDEX idx_dm_dim_date_year_month (`year_month`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;


-- ===========================================================================================================================================


/*
 * ETL: dim_date
 * 	- Range: MIN/MAX (purchase/approved/delivered_carrier/delivered_customer/estimated_delivery) -/+ 1 Month
 * 	- week_of_year: ISO week (WEEK(date, 3))
 * 	- day_of_week: 1 = Mon ... 7 = Sun (WEEKDAY(date) + 1)
*/

-- 날짜 범위 (주문 관련 주요 날짜 컬럼 전체)
SET @start_date := (
	SELECT  DATE_SUB(MIN(d), INTERVAL 1 MONTH)
	  FROM  (
	  		SELECT  DATE(order_purchase_dt) AS d FROM olist_stg.stg_orders WHERE order_purchase_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_approved_dt) AS d FROM olist_stg.stg_orders WHERE order_approved_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_delivered_carrier_dt) AS d FROM olist_stg.stg_orders WHERE order_delivered_carrier_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_delivered_customer_dt) AS d FROM olist_stg.stg_orders WHERE order_delivered_customer_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_estimated_delivery_dt) AS d FROM olist_stg.stg_orders WHERE order_estimated_delivery_dt IS NOT NULL
	  		) AS t
);

SET @end_date := (
	SELECT  DATE_ADD(MAX(d), INTERVAL 1 MONTH)
	  FROM  (
	  		SELECT  DATE(order_purchase_dt) AS d FROM olist_stg.stg_orders WHERE order_purchase_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_approved_dt) AS d FROM olist_stg.stg_orders WHERE order_approved_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_delivered_carrier_dt) AS d FROM olist_stg.stg_orders WHERE order_delivered_carrier_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_delivered_customer_dt) AS d FROM olist_stg.stg_orders WHERE order_delivered_customer_dt IS NOT NULL
	  		UNION ALL
	  		SELECT  DATE(order_estimated_delivery_dt) AS d FROM olist_stg.stg_orders WHERE order_estimated_delivery_dt IS NOT NULL
	  		) AS t
);

SELECT  @start_date AS start_date
		,@end_date AS end_date;

SET @days := DATEDIFF(@end_date, @start_date);

-- 캘린더 생성 및 데이터 적재
TRUNCATE TABLE olist_dm.dim_date;


INSERT INTO olist_dm.dim_date (
	date_key,
	`date`,
	`year`,
	quarter,
	`month`,
	`day`,
	`year_month`,
	year_quarter,
	week_of_year,
	day_of_week,
	day_name,
	is_weekend,
	is_month_start,
	is_month_end
)
SELECT  CAST(DATE_FORMAT(d, '%Y%m%d') AS UNSIGNED) AS date_key
		,d AS `date`
		,YEAR(d) AS `year`
		,QUARTER(d) AS `quarter`
		,MONTH(d) AS `month`
		,DAY(d) AS `day`
		,DATE_FORMAT(d, '%Y-%m') AS `year_month`
		,CONCAT(YEAR(d), '-Q', QUARTER(d)) AS year_quarter
		,WEEK(d, 3) AS week_of_year
		,WEEKDAY(d) + 1 AS day_of_week
		,DAYNAME(d) AS day_name
		,CASE WHEN WEEKDAY(d) IN (5, 6) THEN 1 ELSE 0 END AS is_weekend
		,CASE WHEN DAY(d) = 1 THEN 1 ELSE 0 END AS is_month_start
		,CASE WHEN d = LAST_DAY(d) THEN 1 ELSE 0 END AS is_month_end
  FROM  (
  		SELECT  DATE_ADD(@start_date, INTERVAL seq.n DAY) AS d
  		  FROM  (
  		  		SELECT  ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
  		  		  FROM  information_schema.COLUMNS AS c1
  		  		 CROSS
  		  		  JOIN  information_schema.COLUMNS AS c2
  		  		 LIMIT  5000
  		  		) AS seq
  		 WHERE  seq.n <= @days
  		) AS cal;


-- ===========================================================================================================================================


/*
 * QC: dim_date
 * 	- row count: 861건 (end_date - start_date + 1 = 861)
 * 	- 날짜 범위: 2016-08-04 ~ 2018-12-12
 * 		- purchase_dt가 dim_date 범위 밖에 있는 건수: 0건
 * 		- approved_dt가 dim_date 범위 밖에 있는 건수: 0건
 * 		- delivered_carrier_dt가 dim_date 범위 밖에 있는 건수: 0건
 * 		- delivered_customer_dt가 dim_date 범위 밖에 있는 건수: 0건
 * 		- estimated_delivery_dt가 dim_date 범위 밖에 있는 건수: 0건
 * 	- PK 결측 및 유니크 -> 결측: 0건 / 중복 row: 0건
 * 	- date_key와 date의 정합성 -> DATE_FORMAT 실패 건수: 0건
 * 	- 요일/주말 로직 일관성
 * 		- day_of_week 범위: 1~7
 * 	- 플래그 컬럼 일관성
 * 		- 주말 플래그 컬럼(Sat = 6 / Sun = 7) -> is_weekend 플래그 컬럼이 로직과 다른 건수: 0건
 * 		- 월 시작/끝 플래그 컬럼 -> is_month_start 플래그 컬럼이 로직과 다른 건수: 0건 / is_month_end 플래그 컬럼이 로직과 다른 건수: 0건
*/


-- 샘플
SELECT  *
  FROM  olist_dm.dim_date
 LIMIT  50;

-- 데이터 타입
DESCRIBE olist_dm.dim_date;

-- row count: 861건 (end_date - start_date + 1 = 861)
SELECT  DATEDIFF(MAX(`date`), MIN(`date`)) + 1 AS expected_cnt
		,COUNT(*) AS row_cnt
  FROM  olist_dm.dim_date;


-- 날짜 범위
-- 범위 확인: 2016-08-04 ~ 2018-12-12
SELECT  MIN(`date`) AS min_date
		,MAX(`date`) AS max_date
  FROM  olist_dm.dim_date;

-- purchase_dt가 dim_date 범위 밖에 있는 건수: 0건
SELECT  COUNT(*) AS out_of_range_purchase
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_dm.dim_date AS d
    ON  d.`date` = o.order_purchase_date
 WHERE  o.order_purchase_date IS NOT NULL
   AND  d.date_key IS NULL;

-- approved_dt가 dim_date 범위 밖에 있는 건수: 0건
SELECT  COUNT(*) AS out_of_range_approved
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_dm.dim_date AS d
    ON  d.`date` = DATE(o.order_approved_dt)
 WHERE  o.order_approved_dt IS NOT NULL
   AND  d.date_key IS NULL;

-- delivered_carrier_dt가 dim_date 범위 밖에 있는 건수: 0건
SELECT  COUNT(*) AS out_of_range_carrier
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_dm.dim_date AS d
    ON  d.`date` = DATE(o.order_delivered_carrier_dt)
 WHERE  o.order_delivered_carrier_dt IS NOT NULL
   AND  d.date_key IS NULL;

-- delivered_customer_dt가 dim_date 범위 밖에 있는 건수: 0건
SELECT  COUNT(*) AS out_of_range_customer
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_dm.dim_date AS d
    ON  d.`date` = DATE(o.order_delivered_customer_dt)
 WHERE  o.order_delivered_customer_dt IS NOT NULL
   AND  d.date_key IS NULL;

-- estimated_delivery_dt가 dim_date 범위 밖에 있는 건수: 0건
SELECT  COUNT(*) AS out_of_range_estimated
  FROM  olist_stg.stg_orders AS o
  LEFT
  JOIN  olist_dm.dim_date AS d
    ON  d.`date` = o.order_estimated_delivery_dt 
 WHERE  o.order_estimated_delivery_dt IS NOT NULL
   AND  d.date_key IS NULL;


-- PK 결측 및 유니크 -> null_cnt: 0건 / dup_cnt: 0건
SELECT  SUM(date_key IS NULL) AS null_cnt
		,COUNT(*) - COUNT(DISTINCT date_key) AS dup_cnt
  FROM  olist_dm.dim_date;


-- date_key와 date의 정합성 확인 -> DATE_FORMAT 실패 건수: 0건
SELECT  COUNT(*) AS mismatch_cnt
  FROM  olist_dm.dim_date
 WHERE  date_key <> CAST(DATE_FORMAT(`date`, '%Y%m%d') AS UNSIGNED);


-- 요일/주말 로직 일관성
-- day_of_week 범위: 1 ~ 7
SELECT  MIN(day_of_week) AS min_dow
		,MAX(day_of_week) AS max_dow
  FROM  olist_dm.dim_date;

-- 연말/연초 주차 샘플 -> 정상
SELECT  *
  FROM  olist_dm.dim_date
 WHERE  `date` BETWEEN '2016-12-25' AND '2017-01-10'
 ORDER
    BY  `date`;


-- 플래그 컬럼 일관성 체크
-- 주말 플래그 일관성 체크(Sat = 6 / Sun = 7) -> is_weekend 플래그 컬럼이 로직과 다른 건수: 0건
SELECT  COUNT(*) AS mismatch_cnt
  FROM  olist_dm.dim_date
 WHERE  is_weekend <> CASE WHEN day_of_week IN (6, 7) THEN 1 ELSE 0 END;

-- 월 시작/끝 플래그 체크
-- is_month_start 플래그 컬럼이 로직과 다른 건수: 0건
SELECT  COUNT(*) AS mismatch_start
  FROM  olist_dm.dim_date
 WHERE  is_month_start <> CASE WHEN DAY(`date`) = 1 THEN 1 ELSE 0 END;

-- is_month_end 플래그 컬럼이 로직과 다른 건수: 0건
SELECT  COUNT(*) AS mismatch_end
  FROM  olist_dm.dim_date
 WHERE  is_month_end <> CASE WHEN `date` = LAST_DAY(`date`) THEN 1 ELSE 0 END;










