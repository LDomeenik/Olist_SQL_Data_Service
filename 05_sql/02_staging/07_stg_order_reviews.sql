/****************************************************************************************************************************************/


/*
 * File: 07_stg_order_reviews.sql
 * Description:
 * 	- Source 데이터: olist_raw.order_reviews
 *  - 리뷰(order_reviews) 단위의 Staging 테이블 생성
 * 	- 주문 식별자(order_id) 기준 조인 안전성 확보  
 * 	- 문자열 컬럼(review_comment_title / review_comment_message) 표준화 (TRIM/REPLACE 적용 / 완전 공백은 NULL로 변환)
 * 	- 시간 컬럼(review_creation_date / review_answer_timestamp)은 DATETIME 파싱 및 DATE 파생 컬럼 생성
 * 	- 문자열 공백 여부는 row-level 플래그로 관리 (is_title_blank / is_message_blank)
 * Notes:
 * 	- review_id와 order_id를 결합하여 복합 PK로 사용하였습니다.
 * 	- 식별자 성격을 지닌 ID 컬럼(review_id / order_id)와 필수 컬럼인 review_score는 NOT NULL을 적용하였습니다.
 * 	- 그 외의 컬럼에 대하여는 NULL을 허용하였습니다. (추후 데이터 확장 고려)
 * 	- 테이블 자체에는 정합성 이상치가 발견되지 않아, 따로 처리(삭제/보정/플래그)하지 않았습니다.
 * 	- 조인 정합성 과정에서 시간 정합성(리뷰 생성시각이 주문 구매시각보다 빠른 경우)에 이상(74건)이 발견되었으나, 따로 처리하지 않고 추후 DM 레이어에서 관리할 예정입니다.
 */


/****************************************************************************************************************************************/


USE olist_stg;


/*
 * order_reviews 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포맷 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- order_reviews 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 order_reviews 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */


-- 샘플
SELECT  *
  FROM  olist_raw.order_reviews
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_raw.order_reviews;

-- row count: 99,224행
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.order_reviews;

-- review_id 유니크 확인(PK 가능 여부) -> cnt: 99,224 / distinct_cnt: 98,410 / 중복: 814건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT review_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT review_id) AS dup_cnt
		,SUM(review_id IS NULL OR review_id = '') AS blank_cnt
  FROM  olist_raw.order_reviews;

-- review_id가 중복인 케이스에서 다른 컬럼 값 확인 -> 완전 중복: 0건
SELECT  review_id
		,COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS order_cnt
		,COUNT(DISTINCT review_score) AS score_cnt
		,COUNT(DISTINCT REPLACE(review_creation_date, '\r', '')) AS creation_cnt
		,COUNT(DISTINCT REPLACE(review_answer_timestamp, '\r', '')) AS answer_cnt
  FROM  olist_raw.order_reviews
 GROUP
    BY  review_id
HAVING  COUNT(*) > 1
   AND  COUNT(DISTINCT order_id) = 1
 ORDER
    BY  cnt DESC;

-- order_id 유니크 확인 -> cnt: 99,224 / distinct_cnt: 98,673 / 중복: 551건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT order_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT order_id) AS dup_cnt
		,SUM(order_id IS NULL OR order_id = '') AS blank_cnt
  FROM  olist_raw.order_reviews;

-- order_id 기준 리뷰 개수 분포 -> 리뷰 개수가 1개인 order_id: 98,126건 / 2개인 order_id: 543건 / 3개인 order_id: 4건
SELECT  review_cnt
		,COUNT(*) AS order_cnt
  FROM  (
  		SELECT  order_id
  				,COUNT(*) AS review_cnt
  		  FROM  olist_raw.order_reviews
  		 GROUP
  		    BY  order_id
  		) AS t
 GROUP 
    BY  review_cnt
 ORDER
    BY  review_cnt;

-- (reveiw_id, order_id) 유니크 확인 (복합 PK 가능 여부) -> 중복값: 0건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  review_id
  				,order_id
  				,COUNT(*) AS cnt
  		  FROM  olist_raw.order_reviews
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- 기타 컬럼 중복 및 결측 확인(문자열)
-- review_comment_title 공백 및 결측치: 87,658건(NULL: 0건 / 공백: 87,658건)
SELECT  SUM(review_comment_title IS NULL OR LOWER(TRIM(REPLACE(review_comment_title, '\r', ''))) = '') AS total_unkonwn_cnt
		,SUM(review_comment_title IS NULL) AS null_cnt
		,SUM(LOWER(TRIM(REPLACE(review_comment_title, '\r', ''))) = '') AS blank_cnt
  FROM  olist_raw.order_reviews;

-- review_comment_message 공백 및 결측치: 58,256건(NULL: 0건 / 공백: 58,256건)
SELECT  SUM(review_comment_message IS NULL OR LOWER(TRIM(REPLACE(review_comment_message, '\r', ''))) = '') AS total_unknown_cnt
		,SUM(review_comment_message IS NULL) AS null_cnt
		,SUM(LOWER(TRIM(REPLACE(review_comment_message, '\r', ''))) = '') AS blank_cnt
  FROM  olist_raw.order_reviews;

-- review_comment_title과 review_comment_message가 모두 비어 있는 row: 56,527건
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.order_reviews
 WHERE  LOWER(TRIM(REPLACE(review_comment_message, '\r', ''))) = ''
   AND  LOWER(TRIM(REPLACE(review_comment_title, '\r', ''))) = '';

-- 기타 컬럼 중복 및 결측 확인(숫자 컬럼)
-- review_score 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 0건 / 최솟값: 1 / 최댓값: 5
-- 	- review_score row 분포 -> 5: 57,328건 / 4: 19,142건 / 1: 11,424건 / 3: 8,179건 / 2: 3,151건
SELECT  SUM(review_score IS NULL) AS review_null_cnt
		,SUM(review_score < 0) AS review_negative_cnt
		,SUM(review_score = 0) AS review_zero_cnt
		,MIN(review_score) AS review_min
		,MAX(review_score) AS review_max
  FROM  olist_raw.order_reviews;

SELECT  review_score
		,COUNT(*) AS cnt
  FROM  olist_raw.order_reviews
 GROUP
    BY  review_score
 ORDER
    BY  COUNT(*) DESC;

-- 시간 관련 컬럼 파싱 실패 건수
-- review_creation_date 파싱 실패 건수: 0건 / review_answer_timestamp 파싱 실패 건수: 0건
SELECT  SUM(review_creation_date <> '' AND STR_TO_DATE(REPLACE(review_creation_date, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL) AS creation_parse_fail_cnt
		,SUM(review_answer_timestamp <> '' AND STR_TO_DATE(REPLACE(review_answer_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s') IS NULL) AS answer_parse_fail_cnt
  FROM  olist_raw.order_reviews;

-- 시간 순서 이상치
-- 리뷰 답변 시간이 리뷰 생성 시간보다 빠른 경우: 0건
SELECT  COUNT(*) AS answer_before_creation_cnt
  FROM  olist_raw.order_reviews
 WHERE  review_creation_date <> ''
   AND  review_answer_timestamp <> ''
   AND  STR_TO_DATE(REPLACE(review_creation_date, '\r', ''), '%Y-%m-%d %H:%i:%s') > STR_TO_DATE(REPLACE(review_answer_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s');

-- 조인 정합성 -> orders에 없는 reviews의 order_id: 0건
SELECT  COUNT(*) AS total_reviews
		,SUM(CASE WHEN o.order_id IS NULL THEN 1 ELSE 0 END) AS reviews_without_orders
  FROM  olist_raw.order_reviews AS r
  LEFT
  JOIN  olist_raw.orders AS o
    ON  o.order_id = r.order_id;


/*
 * stg_order_reviews 테이블 ETL:
 * 	- review_id와 order_id를 결합하여 복합 PK로 지정
 * 	- review_score 컬럼의 범위가 1~5로 한정되기 때문에 데이터 타입 변경 (TINYINT)
 * 	- review_comment_title 컬럼 표준화 (TRIM / 개행 제거 및 완전 공백 row를 NULL로 변경)
 * 	- review_comment_message 컬럼 표준화 (TRIM / 개행 제거 및 완전 공백 row를 NULL로 변경)
 * 	- 시간 관련 컬럼(review_creation_date / review_answer_timestamp) 컬럼 타입 변경 (DATETIME)
 * 	- 시간 관련 컬럼(review_creation_date / review_answer_timestamp)의 파생 컬럼(일자 컬럼) 생성 (DATE 타입)
 * 	- 표준화된 review_comment_title과 review_comment_message가 NULL인 값에 대해 플래그 컬럼 생성 (is_title_blank / is_message_blank)
 */

DROP TABLE IF EXISTS olist_stg.stg_order_reviews;


-- 테이블 생성
CREATE TABLE olist_stg.stg_order_reviews (
	review_id				VARCHAR(50)  NOT NULL,
	order_id				VARCHAR(50)  NOT NULL,
	review_score			TINYINT		 NOT NULL,
	review_comment_title    TEXT		 NULL,
	review_comment_message  TEXT		 NULL,
	review_creation_dt	    DATETIME     NULL,
	review_answer_dt		DATETIME     NULL,
	
	-- 파생 컬럼 - 시간 관련
	review_creation_date    DATE		 NULL,
	review_answer_date		DATE		 NULL,
	
	-- 플래그 컬럼 - 문자열 결측치
	is_title_blank			TINYINT		 NOT NULL,
	is_message_blank		TINYINT		 NOT NULL,
	
	-- 복합 PK 및 Indexes
	PRIMARY KEY (review_id, order_id),
	INDEX idx_stg_order_reviews_order_id (order_id), -- 조인 인덱스
	INDEX idx_stg_order_reviews_creation_dt (review_creation_dt) -- 생성일 기준 분석/집계용 인덱스
);

-- 데이터 적재
TRUNCATE TABLE olist_stg.stg_order_reviews;


INSERT INTO olist_stg.stg_order_reviews (
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_dt,
	review_answer_dt,
	
	review_creation_date,
	review_answer_date,
	
	is_title_blank,
	is_message_blank
)
SELECT  review_id
		,order_id
		,review_score
		,title_norm AS review_comment_title
		,message_norm AS review_comment_message
		,review_creation_dt
		,review_answer_dt
		
		,DATE(review_creation_dt) AS review_creation_date
		,DATE(review_answer_dt) AS review_answer_date
		
		,CASE WHEN title_norm IS NULL THEN 1
			  ELSE 0
			  END AS is_title_blank
		,CASE WHEN message_norm IS NULL THEN 1
			  ELSE 0
			  END AS is_message_blank
  FROM  (
  		SELECT  review_id
  				,order_id
  				,review_score
  				,NULLIF(TRIM(REPLACE(review_comment_title, '\r', '')), '') AS title_norm
  				,NULLIF(TRIM(REPLACE(review_comment_message, '\r', '')), '') AS message_norm
  				,STR_TO_DATE(REPLACE(review_creation_date, '\r', ''), '%Y-%m-%d %H:%i:%s') AS review_creation_dt
  				,STR_TO_DATE(REPLACE(review_answer_timestamp, '\r', ''), '%Y-%m-%d %H:%i:%s') AS review_answer_dt
  		  FROM  olist_raw.order_reviews
  		) AS t
 WHERE  review_id IS NOT NULL
   AND  review_id <> ''
   AND  order_id IS NOT NULL
   AND  order_id <> ''
   AND  review_score IS NOT NULL; -- PK 및 NOT NULL 항목 재명시


-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_order_reviews
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_order_reviews;

-- row count: 99,224행
SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_order_reviews;

-- (review_id, order_id) 유니크 확인 -> 중복 개수: 0건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  review_id
  				,order_id
  				,COUNT(*) AS cnt
  		  FROM  olist_stg.stg_order_reviews
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- order_id 기준 리뷰 개수 분포 -> 리뷰 개수가 1개인 order_id: 98,126건 / 2개인 order_id: 543건 / 3개인 order_id: 4건
SELECT  review_cnt
		,COUNT(*) AS order_cnt
  FROM  (
  		SELECT  order_id
  				,COUNT(*) AS review_cnt
  		  FROM  olist_stg.stg_order_reviews
  		 GROUP
  		    BY  order_id
  		) AS t
 GROUP 
    BY  review_cnt
 ORDER
    BY  review_cnt;

-- 기타 컬럼 중복 및 결측 확인(문자열) + 플래그 컬럼과의 일치
-- review_comment_title 공백 및 결측치: 87,658건(NULL: 87,658건 / 공백: 0건 / 플래그 컬럼(1): 87,658)
SELECT  SUM(review_comment_title IS NULL OR review_comment_title = '') AS total_unkonwn_cnt
		,SUM(review_comment_title IS NULL) AS null_cnt
		,SUM(review_comment_title = '') AS blank_cnt
		,SUM(is_title_blank = 1)
  FROM  olist_stg.stg_order_reviews;

-- review_comment_message 공백 및 결측치: 58,256건(NULL: 58,256건 / 공백: 0건 / 플래그 컬럼(1): 58,256)
SELECT  SUM(review_comment_message IS NULL OR review_comment_message = '') AS total_unknown_cnt
		,SUM(review_comment_message IS NULL) AS null_cnt
		,SUM(review_comment_message = '') AS blank_cnt
		,SUM(is_message_blank = 1)
  FROM  olist_stg.stg_order_reviews;

-- review_comment_title과 review_comment_message가 모두 비어 있는 row: 56,527건 (플래그 컬럼(1): 56,527)
SELECT  SUM(review_comment_title IS NULL AND review_comment_message IS NULL) AS total_null_cnt
		,SUM(is_title_blank = 1 AND is_message_blank = 1) AS flag_cnt
  FROM  olist_stg.stg_order_reviews;

-- 기타 컬럼 중복 및 결측 확인(숫자 컬럼)
-- review_score 분포 -> 결측치: 0건 / 음수: 0건 / 0값: 0건 / 최솟값: 1 / 최댓값: 5
-- 	- review_score row 분포 -> 5: 57,328건 / 4: 19,142건 / 1: 11,424건 / 3: 8,179건 / 2: 3,151건
SELECT  SUM(review_score IS NULL) AS review_null_cnt
		,SUM(review_score < 0) AS review_negative_cnt
		,SUM(review_score = 0) AS review_zero_cnt
		,MIN(review_score) AS review_min
		,MAX(review_score) AS review_max
  FROM  olist_stg.stg_order_reviews;

SELECT  review_score
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_order_reviews
 GROUP
    BY  review_score
 ORDER
    BY  COUNT(*) DESC;

-- 시간 관련 컬럼 파싱 실패 건수
-- review_creation_dt NULL 건수: 0건 / review_answer_dt NULL 건수: 0건
SELECT  SUM(review_creation_dt IS NULL) AS creation_dt_null_cnt
		,SUM(review_answer_dt IS NULL) AS answer_dt_null_cnt
  FROM  olist_stg.stg_order_reviews;

-- 시간 순서 이상치
-- 리뷰 답변 시간이 리뷰 생성 시간보다 빠른 경우: 0건
SELECT  COUNT(*) AS answer_before_creation_cnt
  FROM  olist_stg.stg_order_reviews
 WHERE  review_creation_dt IS NOT NULL
   AND  review_answer_dt IS NOT NULL
   AND  review_creation_dt > review_answer_dt;

-- 조인 정합성(1) -> orders에 없는 reviews의 order_id: 0건
SELECT  COUNT(*) AS total_reviews
		,SUM(CASE WHEN o.order_id IS NULL THEN 1 ELSE 0 END) AS reviews_without_orders
  FROM  olist_stg.stg_order_reviews AS r
  LEFT
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = r.order_id;

-- 조인 정합성(2) -> orders의 주문 시간보다 리뷰 생성 시간이 빠른 경우: 74건
-- 	- review_creation_dt가 order_purchase_dt보다 빠른 시간 중 최솟값(diff_min(분) 최대): -489
-- 	- review_creation_dt가 order_purchase_dt보다 빠른 시간 중 최댓값(diff min(분) 최소): -161,056
-- 	- 시간 이상치 분포는 넓게 퍼져 있음
-- 	- 가설: order_id가 중복되기 때문에 발생하는 것이다. -> 가설 기각 (중복 order_id에서의 이상치 건수는 3건 / 단일 order_id에서의 이상치 건수는 71건)
-- 	- 조인에서 나타나는 시간 정합성 이슈는 분석 시 필터링 가능하도록 DM 레이어에서 플래그로 관리할 예정
SELECT  COUNT(*) AS mismatch_review_time_cnt
  FROM  olist_stg.stg_order_reviews AS r
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = r.order_id
 WHERE  r.review_creation_dt IS NOT NULL
   AND  o.order_purchase_dt IS NOT NULL
   AND  r.review_creation_dt < o.order_purchase_dt;

SELECT  COUNT(*) AS mismatch_cnt
		,MIN(TIMESTAMPDIFF(MINUTE, o.order_purchase_dt, r.review_creation_dt)) AS min_diff_min
		,MAX(TIMESTAMPDIFF(MINUTE, o.order_purchase_dt, r.review_creation_dt)) AS max_diff_min
  FROM  olist_stg.stg_order_reviews AS r
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = r.order_id
 WHERE  r.review_creation_dt IS NOT NULL
   AND  o.order_purchase_dt IS NOT NULL
   AND  r.review_creation_dt < o.order_purchase_dt;

SELECT  TIMESTAMPDIFF(MINUTE, o.order_purchase_dt, r.review_creation_dt) AS diff_min
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_order_reviews AS r
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = r.order_id
 WHERE  r.review_creation_dt IS NOT NULL
   AND  o.order_purchase_dt IS NOT NULL
   AND  r.review_creation_dt < o.order_purchase_dt
 GROUP
    BY  1
 ORDER
    BY  cnt DESC, diff_min;

-- dup_orders: 3건 / single_orders: 71건
WITH mismatch AS (
	SELECT  r.order_id
	  FROM  olist_stg.stg_order_reviews AS r
	  JOIN  olist_stg.stg_orders AS o
	    ON  o.order_id = r.order_id
	 WHERE  r.review_creation_dt IS NOT NULL
	   AND  o.order_purchase_dt IS NOT NULL
	   AND  r.review_creation_dt < o.order_purchase_dt 
),
dup_orders AS (
	SELECT  order_id
	  FROM  olist_stg.stg_order_reviews
	 GROUP
	    BY  order_id
	HAVING  COUNT(*) > 1
)
SELECT  COUNT(*) AS mismatch_rows
		,SUM(CASE WHEN d.order_id IS NOT NULL THEN 1
				  ELSE 0
				  END) AS mismatch_in_dup_orders
		,SUM(CASE WHEN d.order_id IS NULL THEN 1
				  ELSE 0
				  END) AS mismatch_in_single_orders
  FROM  mismatch AS m
  LEFT
  JOIN  dup_orders AS d
    ON  d.order_id = m.order_id;

-- 1: 71건 / 2: 3건
WITH mismatch_orders AS (
	SELECT  DISTINCT r.order_id
	  FROM  olist_stg.stg_order_reviews AS r
	  JOIN  olist_stg.stg_orders AS o
	    ON  o.order_id = r.order_id
	 WHERE  r.review_creation_dt IS NOT NULL
	   AND  o.order_purchase_dt IS NOT NULL
	   AND  r.review_creation_dt < o.order_purchase_dt
)
SELECT  cnt.review_cnt
		,COUNT(*) AS order_cnt
  FROM  (
  		SELECT  r.order_id
  				,COUNT(*) AS review_cnt
  		  FROM  olist_stg.stg_order_reviews AS r
  		  JOIN  mismatch_orders AS m
  		    ON  m.order_id = r.order_id
  		 GROUP
  		    BY  r.order_id
  		) AS cnt
 GROUP
    BY  cnt.review_cnt
 ORDER
    BY  cnt.review_cnt;

-- 74건
SELECT  COUNT(DISTINCT r.order_id) AS mismatch_order_cnt
  FROM  olist_stg.stg_order_reviews AS r
  JOIN  olist_stg.stg_orders AS o
    ON  o.order_id = r.order_id
 WHERE  r.review_creation_dt < o.order_purchase_dt;

