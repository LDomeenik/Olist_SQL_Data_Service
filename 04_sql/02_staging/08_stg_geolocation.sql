/****************************************************************************************************************************************/


/*
 * File: 08_stg_geolocation.sql
 * Description:
 * 	- Source 데이터: olist_raw.geolocation
 *  - 우편번호 prefix(geolocation_zip_code_prefix) 단위의 Staging 테이블 생성
 * 	- 우편번호 기준 조인 안전성 확보  
 * 	- 문자열 컬럼(geolocation_city / geolocation_state) 표준화 (TRIM, REPLACE 적용 / 타입 변환)
 * 	- 지도/지역 분석을 위한 대표 좌표(lat/lng) 및 대표 도시/주(city/state) 산출
 * 	- 품질 지표/플래그 컬럼 생성 (invalid 좌표 존재 여부 / 복수 state 매핑 여부)
 * Notes:
 * 	- geolocation_zip_code_prefix를 PK로 사용하였습니다.
 * 	- PK 컬럼인 geolocation_zip_code_prefix와 집계/플래그 컬럼은 NOT NULL을 지정하였습니다.
 * 	- 이외 컬럼은 추후 데이터 확장을 고려하여 NULL을 허용하였습니다.
 * 	- 대표 좌표는 브라질 유효 범위 내에서만 산출하며, 유효 좌표가 없는 경우 NULL로 처리하였습니다.
 * 	- 대표값 선정은 zip_prefix 그룹 내 최빈값 기준이며, 동률일 경우 정렬 기준으로 1건을 결정하였습니다.
 * 	- mode_ratio_pct(대표 좌표 비중)는 대표값 신뢰도 판단을 위한 지표로, 추후 DM/대시보드에서 필터링 기준으로 활용할 예정입니다.
 * 
 */


/****************************************************************************************************************************************/

USE olist_stg;

/*
 * geolocation 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포맷 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- geolocation 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 geolocation 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */

-- 샘플
SELECT  *
  FROM  olist_raw.geolocation
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_raw.geolocation;

-- row count: 1,000,163행
SELECT  COUNT(*) AS cnt
  FROM  olist_raw.geolocation;

-- geolocation_zip_code_prefix 유니크 확인 -> cnt: 1,000,163 / distinct_cnt: 19,015 / 중복: 981,148건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix) AS dup_cnt
		,SUM(geolocation_zip_code_prefix IS NULL OR geolocation_zip_code_prefix = '') AS blank_cnt
  FROM  olist_raw.geolocation;

-- geolocation_lat 유니크 확인 -> cnt: 1,000,163 / distinct_cnt: 658,242 / 중복: 341,921건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT geolocation_lat) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT geolocation_lat) AS dup_cnt
		,SUM(geolocation_lat IS NULL) AS blank_cnt
  FROM  olist_raw.geolocation;

-- geolocation_lng 유니크 확인 -> cnt: 1,000,163 / distinct_cnt: 674,800 / 중복: 325,364건 / 결측 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT geolocation_lng) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT geolocation_lng) AS dup_cnt
		,SUM(geolocation_lng IS NULL) AS blank_cnt
  FROM  olist_raw.geolocation;

-- (geolocation_zip_code_prefix, geolocation_state) 중복 그룹 수: 17,972건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
  		SELECT  geolocation_zip_code_prefix
  				,geolocation_state
  				,COUNT(*) AS cnt
  		  FROM  olist_raw.geolocation
  		 GROUP
  		    BY  1, 2
  		HAVING  COUNT(*) > 1
  		) AS t;

-- (geolocation_lat, geolocation_lng) 중복 그룹 수: 132,219건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
		SELECT  geolocation_lat
				,geolocation_lng
				,COUNT(*) AS cnt
		  FROM  olist_raw.geolocation
		 GROUP
		    BY  1, 2
		HAVING  COUNT(*) > 1
  		) AS t;

-- (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) 중복 그룹 수: 131,720건
SELECT  COUNT(*) AS dup_cnt
  FROM  (
		SELECT  geolocation_zip_code_prefix
				,geolocation_lat
				,geolocation_lng
				,COUNT(*) AS cnt
		  FROM  olist_raw.geolocation
		 GROUP
		    BY  1, 2, 3
		HAVING  COUNT(*) > 1
  		)AS t;

-- zip_prefix별 row 수 분포 -> 24220: 1,146 / 24230: 1,102 / 38400: 965 ...
SELECT  geolocation_zip_code_prefix
		,COUNT(*) AS cnt
  FROM  olist_raw.geolocation
 GROUP
    BY  1
 ORDER
    BY  cnt DESC
 LIMIT  20;

-- zip_prefix별 좌표 분산: 가장 많은 (lat, lng)를 가진 zip_code_prefix는 745건을 가지고 있음 (최소는 1건)
SELECT  geolocation_zip_code_prefix
		,COUNT(DISTINCT CONCAT(geolocation_lat, ',', geolocation_lng)) AS distinct_latlng_cnt
  FROM  olist_raw.geolocation
 GROUP
    BY  1
 ORDER
    BY  distinct_latlng_cnt DESC
 LIMIT  20;

-- zip_prefix별 좌표 퍼짐 정도
SELECT  geolocation_zip_code_prefix
		,COUNT(*) AS row_cnt
		,COUNT(DISTINCT CONCAT(geolocation_lat, ',', geolocation_lng)) AS distinct_latlng_cnt
		,(MAX(geolocation_lat) - MIN(geolocation_lat)) AS lat_range
		,(MAX(geolocation_lng) - MIN(geolocation_lng)) AS lng_range
  FROM  olist_raw.geolocation
 GROUP
    BY  1
 ORDER
    BY  (MAX(geolocation_lat) - MIN(geolocation_lat)) DESC
    	,(MAX(geolocation_lng) - MIN(geolocation_lng)) DESC
 LIMIT  20;

-- (lat, lng) 범위 이상치(브라질): 29건
SELECT  COUNT(*) AS invalid_cnt
  FROM  olist_raw.geolocation
 WHERE  geolocation_lat NOT BETWEEN -35 AND 6
    OR  geolocation_lng NOT BETWEEN -75 AND -30;

SELECT  *
  FROM  olist_raw.geolocation
 WHERE  geolocation_lat NOT BETWEEN -35 AND 6
    OR  geolocation_lng NOT BETWEEN -75 AND -30;

-- 서로 다른 state에 동일한 zip_prefix: 2개의 state에서 동일한 zip_code_prefix를 사용하는 경우가 8건 존재
SELECT  geolocation_zip_code_prefix
		,COUNT(DISTINCT geolocation_state) AS state_cnt
  FROM  olist_raw.geolocation
 GROUP
    BY  1
HAVING  COUNT(DISTINCT geolocation_state) > 1;

-- 서로 다른 city에 동일한 zip_prefix: 2개 이상의 city에서 동일한 zip_code_prefix를 사용하는 경우가 550건 존재
SELECT  geolocation_zip_code_prefix
		,COUNT(DISTINCT geolocation_city) AS city_cnt
  FROM  olist_raw.geolocation
 GROUP
    BY  1
HAVING  COUNT(DISTINCT geolocation_city) > 1;

SELECT  city_cnt
		,COUNT(*) AS zip_cnt
  FROM  (
  		SELECT  geolocation_zip_code_prefix
  				,COUNT(DISTINCT TRIM(UPPER(geolocation_city))) AS city_cnt
  		  FROM  olist_raw.geolocation
  		 GROUP
  		    BY  1
  		) AS t
 GROUP
    BY  1
 ORDER
    BY  1;

-- 복수 state인 zip_prefix에 대한 state별 빈도
SELECT  geolocation_zip_code_prefix
		,geolocation_state
		,COUNT(*) AS cnt
  FROM  olist_raw.geolocation
 WHERE  geolocation_zip_code_prefix IN (
 										SELECT  geolocation_zip_code_prefix
 										  FROM  olist_raw.geolocation
 										 GROUP
 										    BY  1
 										HAVING  COUNT(DISTINCT geolocation_state) > 1
 										)
 GROUP
    BY  1, 2
 ORDER
    BY  geolocation_zip_code_prefix
    	,cnt DESC;

-- zip_prefix별 대표 좌표(mode) 비중
WITH freq AS (
	SELECT  geolocation_zip_code_prefix
			,geolocation_lat
			,geolocation_lng
			,COUNT(*) AS cnt
	  FROM  olist_raw.geolocation
	 WHERE  geolocation_lat BETWEEN -35 AND 6
	   AND  geolocation_lng BETWEEN -75 AND -30
	 GROUP
	    BY  1, 2, 3
),
ranked AS (
	SELECT  geolocation_zip_code_prefix
			,geolocation_lat
			,geolocation_lng
			,cnt
			,ROW_NUMBER() OVER(PARTITION BY geolocation_zip_code_prefix ORDER BY cnt DESC, geolocation_lat, geolocation_lng) AS rnk
	  FROM  freq
),
total AS (
	SELECT  geolocation_zip_code_prefix
			,COUNT(*) AS row_cnt
	  FROM  olist_raw.geolocation
	 GROUP
	    BY  1
)
SELECT  r.geolocation_zip_code_prefix
		,t.row_cnt
		,r.cnt AS mode_cnt
		,ROUND(r.cnt / t.row_cnt * 100, 2) AS mode_ratio_pct
  FROM  ranked AS r
  JOIN  total AS t
    ON  t.geolocation_zip_code_prefix = r.geolocation_zip_code_prefix
 WHERE  r.rnk = 1
 ORDER
    BY  mode_ratio_pct ;

-- 기타 컬럼 결측 및 공백 -> geolocation_city 결측 및 공백: 0건 / geolocation_state 결측 및 공백: 0건
SELECT  SUM(geolocation_city IS NULL OR TRIM(REPLACE(geolocation_city, '\r', '')) = '') AS city_blank_cnt
		,SUM(geolocation_state IS NULL OR TRIM(REPLACE(geolocation_state, '\r', '')) = '') AS state_blank_cnt
  FROM  olist_raw.geolocation;

-- city 정규화 여부 확인 -> 정규화 전: 5,969건 / 정규화 후: 5,968건
SELECT  COUNT(DISTINCT geolocation_city) AS raw_city_distinct
		,COUNT(DISTINCT LOWER(TRIM(REPLACE(geolocation_city, '\r', '')))) AS norm_city_distinct
  FROM  olist_raw.geolocation;

-- state 분포 확인 -> SP: 40.42%(404,268건) / MG: 12.63%(126,336건) / RJ: 12.11%(121,169건) ...
WITH CTE AS (
	SELECT  UPPER(TRIM(REPLACE(geolocation_state, '\r', ''))) AS state_norm
			,COUNT(*) AS cnt
	  FROM  olist_raw.geolocation
	 GROUP
	    BY  1
)
SELECT  state_norm
		,cnt
		,ROUND(cnt / SUM(cnt) OVER() * 100.0, 2) AS cnt_per
  FROM  CTE
 ORDER
    BY  cnt DESC;

-- geolocation_state 길이 이상치 확인 -> 길이가 2 이상인 state 개수: 0건
SELECT  SUM(geolocation_state IS NOT NULL 
			AND TRIM(REPLACE(geolocation_state, '\r', '')) <> '' 
			AND LENGTH(TRIM(REPLACE(geolocation_state, '\r', ''))) <> 2) AS state_len_invalid_cnt
  FROM  olist_raw.geolocation;

-- (city, state) 정규화 여부 확인 -> 정규화 전: 6,350건 / 정규화 후: 6,349건
SELECT  COUNT(DISTINCT CONCAT(geolocation_city, '_', geolocation_state)) AS raw_city_state_distinct
		,COUNT(DISTINCT CONCAT(LOWER(TRIM(REPLACE(geolocation_city, '\r', ''))), '_', UPPER(TRIM(REPLACE(geolocation_state, '\r', ''))))) AS norm_city_state_distinct
  FROM  olist_raw.geolocation;

-- geolocation_zip_code_prefix의 데이터 타입 변경 여부 확인 -> 최소 길이: 5 / 최대 길이: 5 / 문자열: 0
SELECT  MIN(LENGTH(TRIM(REPLACE(geolocation_zip_code_prefix, '\r', '')))) AS min_len
		,MAX(LENGTH(TRIM(REPLACE(geolocation_zip_code_prefix, '\r', '')))) AS max_len
		,SUM(TRIM(REPLACE(geolocation_zip_code_prefix, '\r', '')) REGEXP '^[0-9]+$' = 0) AS non_numeric_cnt
  FROM  olist_raw.geolocation
 WHERE  geolocation_zip_code_prefix IS NOT NULL
   AND  TRIM(REPLACE(geolocation_zip_code_prefix, '\r', '')) <> '';


/*
 * stg_geolocation 테이블 ETL:
 * 	- olist_raw.geolocation 데이터의 타입을 변환 
 * 		-> geolocation_zip_code_prefix: CHAR(5) (길이가 5로 고정 / 조인 안전성을 위해 문자형으로 지정)
 * 		-> geolocation_state: CHAR(2) (길이가 2로 고정)
 *	- zip_prefix 단위의 raw row(row_cnt) 집계
 *	- 브라질 유효 좌표 범위(lat: -35 ~ 6 / lng: -75 ~ -30)를 기준으로 좌표 유효성 판단(invalid_latlng_cnt) 
 * 	- zip_prefix 단위로 대표 좌표(lat/lng)를 산출 (브라질 유효 범위 내 최빈값으로 지정)
 * 	- zip_prefix 단위로 대표 city/state를 산출 (zip_prefix 그룹 내 (city, state) 최빈값으로 선정)
 * 	- zip_prefix 단위로 state 분포 집계(state_cnt)
 * 	- zip_prefix 단위로 city 분포 집계(city_cnt)
 * 	- city와 state를 결합한 geolocation_city_state 파생 컬럼 생성
 * 	- 집계 결과를 기반으로 품질 관리용 플래그 컬럼 생성
 * 		-> is_invalid_latlng_exists: zip_prefix 그룹 내 유효 범위를 벗어난 좌표가 1건 이상 존재할 경우 1
 * 		-> is_multi_state: 동일 zip_prefix가 2개 이상의 state에 매핑될 경우 1
 * 
 * Note:
 * 	- zip_prefix 단위로 집계 기준을 설정하여 Staging 테이블을 구성하였습니다.
 * 	- 추후 분석의 편의를 위해 city와 state를 결합한 파생 컬럼을 생성하였습니다.
 * 	- geolocation_zip_code_prefix의 경우 모두 숫자 / 길이 5로 고정이나, 안전성을 고려해 문자열로 CHAR(5)로 지정하였습니다.
 * 	- 대표 좌표는 브라질 유효 범위 내에서만 산출하였습니다.
 * 	- 유효 좌표가 없는 zip_prefix는 대표 좌표를 NULL로 유지하였습니다. 
 */

DROP TABLE IF EXISTS olist_stg.stg_geolocation;

-- 테이블 생성
CREATE TABLE olist_stg.stg_geolocation (
	geolocation_zip_code_prefix  CHAR(5)         NOT NULL,
	geolocation_lat				 DECIMAL(10, 6)  NULL,
	geolocation_lng				 DECIMAL(10, 6)  NULL,
	geolocation_city			 VARCHAR(100)    NULL,
	geolocation_state			 CHAR(2)         NULL,
	
	-- 파생 컬럼
	geolocation_city_state		 VARCHAR(200)    NULL,
	row_cnt						 INT			 NOT NULL,
	mode_cnt					 INT			 NOT NULL,
	mode_ratio_pct				 DECIMAL(6, 2)   NOT NULL,
	invalid_latlng_cnt			 INT			 NOT NULL,
	city_cnt					 TINYINT		 NOT NULL,
	state_cnt					 TINYINT		 NOT NULL,
	
	-- 플래그 컬럼
	is_invalid_latlng_exists     TINYINT         NOT NULL,
	is_multi_city				 TINYINT		 NOT NULL,
	is_multi_state				 TINYINT         NOT NULL,
	
	-- PK, Indexes
	PRIMARY KEY (geolocation_zip_code_prefix),
	INDEX idx_stg_geolocation_state (geolocation_state),
	INDEX idx_stg_geolocation_city_state (geolocation_city_state)
);

-- 데이터 적재
TRUNCATE TABLE olist_stg.stg_geolocation;

INSERT INTO olist_stg.stg_geolocation (
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state,
	
	geolocation_city_state,
	row_cnt,
	mode_cnt,
	mode_ratio_pct,
	invalid_latlng_cnt,
	city_cnt,
	state_cnt,
	
	is_invalid_latlng_exists,
	is_multi_city,
	is_multi_state
)
WITH base AS (
	SELECT  TRIM(REPLACE(geolocation_zip_code_prefix, '\r', '')) AS zip_prefix
			,geolocation_lat AS lat
			,geolocation_lng AS lng
			,LOWER(TRIM(REPLACE(geolocation_city, '\r', ''))) AS city_norm
			,UPPER(TRIM(REPLACE(geolocation_state, '\r', ''))) AS state_norm
			,CASE WHEN geolocation_lat BETWEEN -35 AND 6 AND geolocation_lng BETWEEN -75 AND -30 THEN 1
			      ELSE 0
			      END AS is_valid_latlng
	  FROM  olist_raw.geolocation
	 WHERE  geolocation_zip_code_prefix IS NOT NULL
	   AND  TRIM(REPLACE(geolocation_zip_code_prefix, '\r', '')) <> ''
),
agg AS (
	SELECT  zip_prefix
			,COUNT(*) AS row_cnt
			,SUM(CASE WHEN is_valid_latlng = 0 THEN 1 ELSE 0 END) AS invalid_latlng_cnt
			,COUNT(DISTINCT city_norm) AS city_cnt
			,COUNT(DISTINCT state_norm) AS state_cnt
	  FROM  base
	 GROUP
	    BY  zip_prefix
),
latlng_freq AS (
	SELECT  zip_prefix
			,lat
			,lng
			,COUNT(*) AS cnt
	  FROM  base
	 WHERE  is_valid_latlng = 1
	 GROUP
	    BY  zip_prefix
	    	,lat
	    	,lng
),
latlng_mode AS (
	SELECT  zip_prefix
			,lat
			,lng
			,cnt
			,ROW_NUMBER() OVER (PARTITION BY zip_prefix ORDER BY cnt DESC, lat ASC, lng ASC) AS rnk
	  FROM  latlng_freq
),
city_state_freq AS (
	SELECT  zip_prefix
			,city_norm
			,state_norm
			,COUNT(*) AS cnt
	  FROM  base
	 GROUP
	    BY  zip_prefix
	    	,city_norm
	    	,state_norm
),
city_state_mode AS (
	SELECT  zip_prefix
			,city_norm
			,state_norm
			,cnt
			,ROW_NUMBER() OVER(PARTITION BY zip_prefix ORDER BY cnt DESC, city_norm ASC, state_norm ASC) AS rnk
	  FROM  city_state_freq
)
SELECT  a.zip_prefix AS geolocation_zip_code_prefix
		,lm.lat AS geolocation_lat
		,lm.lng AS geolocation_lng
		,csm.city_norm AS geolocation_city
		,csm.state_norm AS geolocation_state
		
		,CASE WHEN csm.city_norm IS NULL OR csm.state_norm IS NULL THEN NULL
			  ELSE  CONCAT(csm.city_norm, '_', csm.state_norm)
			  END AS geolocation_city_state
		,a.row_cnt
		,COALESCE(lm.cnt, 0) AS mode_cnt
		,ROUND(COALESCE(lm.cnt, 0) / a.row_cnt * 100.0, 2) AS mode_ratio_pct
		,a.invalid_latlng_cnt
		,a.city_cnt
		,a.state_cnt
		
		,CASE WHEN a.invalid_latlng_cnt > 0 THEN 1 ELSE 0 END AS is_invalid_latlng_exists
		,CASE WHEN a.city_cnt > 1 THEN 1 ELSE 0 END AS is_multi_city
		,CASE WHEN a.state_cnt > 1 THEN 1 ELSE 0 END AS is_multi_state
  FROM  agg AS a
  LEFT
  JOIN  latlng_mode AS lm
    ON  lm.zip_prefix = a.zip_prefix
   AND  lm.rnk = 1
  LEFT
  JOIN  city_state_mode AS csm
    ON  csm.zip_prefix = a.zip_prefix
   AND  csm.rnk = 1;


-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_geolocation
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_geolocation;

-- row count: 19,015행
SELECT  COUNT(*) AS cnt
  FROM  olist_stg.stg_geolocation;

-- geolocation_zip_code_prefix의 유니크 확인 -> cnt: 19,015 / distinct_cnt: 19,015 / 중복: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix) AS dup_cnt
  FROM  olist_stg.stg_geolocation;

-- geolocation_lat, geolocation_lng 공백 및 결측: 4건 (is_invalid_latlng_exists가 모두 1 / mode_cnt가 모두 0)
SELECT  SUM(geolocation_lat IS NULL OR geolocation_lng IS NULL) AS null_latlng_cnt
		,COUNT(*) AS total_cnt
  FROM  olist_stg.stg_geolocation;

SELECT  geolocation_zip_code_prefix
		,row_cnt
		,mode_cnt
		,invalid_latlng_cnt
  FROM  olist_stg.stg_geolocation
 WHERE  geolocation_lat IS NULL
    OR  geolocation_lng IS NULL;

SELECT  *
  FROM  olist_stg.stg_geolocation
 WHERE  geolocation_lat IS NULL
    OR  geolocation_lng IS NULL;

-- city_state 결측치: 0건
SELECT  SUM(geolocation_city IS NULL OR geolocation_city = '') AS null_city_cnt
		,SUM(geolocation_state IS NULL OR geolocation_state = '') AS null_state_cnt
		,SUM(geolocation_city_state IS NULL OR geolocation_city_state = '') AS null_city_state_cnt
  FROM  olist_stg.stg_geolocation;

-- 대표 좌표가 유효 범위 밖인 케이스: 0건
SELECT  COUNT(*) AS invalid_rep_latlng_cnt
  FROM  olist_stg.stg_geolocation
 WHERE  (geolocation_lat IS NOT NULL AND geolocation_lat NOT BETWEEN -35 AND 6)
    OR  (geolocation_lng IS NOT NULL AND geolocation_lng NOT BETWEEN -75 AND -30);

-- state 길이 이상치: 0건
SELECT  COUNT(*) AS state_len_invalid_cnt
  FROM  olist_stg.stg_geolocation
 WHERE  geolocation_state IS NOT NULL
   AND  geolocation_state <> ''
   AND  LENGTH(geolocation_state) <> 2;

-- city_cnt와 대표 city가 일치하지 않는 경우: 0건
SELECT  COUNT(*) AS inconsistent_city_rep
  FROM  olist_stg.stg_geolocation
 WHERE  city_cnt = 1
   AND  (geolocation_city IS NULL OR geolocation_state = '');

-- state_cnt와 대표 state가 일치하지 않는 경우: 0건
SELECT  COUNT(*) AS inconsistent_state_rep
  FROM  olist_stg.stg_geolocation
 WHERE  state_cnt = 1
   AND  (geolocation_state IS NULL OR geolocation_state = '');

-- mode_cnt가 row_cnt보다 큰 경우: 0건
SELECT  COUNT(*) AS mode_cnt_gt_row_cnt
  FROM  olist_stg.stg_geolocation
 WHERE  mode_cnt > row_cnt;

-- mode_ratio_pct 범위 이상치: 0건
SELECT  COUNT(*) AS invalid_mode_ratio_cnt
  FROM  olist_stg.stg_geolocation
 WHERE  mode_ratio_pct < 0
    OR  mode_ratio_pct > 100;

-- is_invalid_latlng_exists 집계값 이상치: 0건
SELECT  COUNT(*) AS flag_mismatch_invalid_latlng
  FROM  olist_stg.stg_geolocation
 WHERE  (invalid_latlng_cnt > 0 AND is_invalid_latlng_exists <> 1)
    OR  (invalid_latlng_cnt = 0 AND is_invalid_latlng_exists <> 0);

-- is_multi_city 집계값 이상치: 0건
SELECT  COUNT(*) AS flag_mismatch_multi_city
  FROM  olist_stg.stg_geolocation
 WHERE  (city_cnt > 1 AND is_multi_city <> 1)
    OR  (city_cnt <= 1 AND is_multi_city <> 0);

-- is_multi_state 집계값 이상치: 0건
SELECT  COUNT(*) AS flag_mismatch_multi_state
  FROM  olist_stg.stg_geolocation
 WHERE  (state_cnt > 1 AND is_multi_state <> 1)
    OR  (state_cnt <= 1 AND is_multi_state <> 0);

-- 유효좌표와 mode_cnt, 대표좌표가 일치하지 않는 경우: 0건
SELECT  COUNT(*) AS inconsistent_no_valid_latlng
  FROM  olist_stg.stg_geolocation
 WHERE  (mode_cnt = 0 AND (geolocation_lat IS NOT NULL OR geolocation_lng IS NOT NULL))
    OR  (mode_cnt > 0 AND (geolocation_lat IS NULL OR geolocation_lng IS NULL));

-- mode_ratio_pct가 낮은 zip_prefix -> 83252, 18243, 95130, 78131은 0%
-- 	- mode_ratio_pct가 5%보다 낮은 건수: 19,015,건 중 2,888건 (15.19%)
SELECT  geolocation_zip_code_prefix
		,row_cnt
		,mode_cnt
		,mode_ratio_pct
  FROM  olist_stg.stg_geolocation
 WHERE  mode_ratio_pct < 5
 ORDER
    BY  mode_ratio_pct ASC
    	,row_cnt DESC
 LIMIT  50;

SELECT  SUM(mode_ratio_pct < 5) AS low_mode_cnt
		,COUNT(*) AS total_cnt
		,ROUND(SUM(mode_ratio_pct < 5) / COUNT(*) * 100, 2) AS low_mode_pct
  FROM  olist_stg.stg_geolocation;

-- invalid_latlng_cnt가 많은 zip_prefix -> 68275: 5 / 98780: 3 / 35179: 2 / 29654: 2 / 83252: 2 / 그 외: 1
SELECT  geolocation_zip_code_prefix
		,row_cnt
		,invalid_latlng_cnt
  FROM  olist_stg.stg_geolocation
 WHERE  invalid_latlng_cnt > 0
 ORDER
    BY  invalid_latlng_cnt DESC
    	,row_cnt DESC
 LIMIT  50;

-- 조인 정합성(1): 고객 zip_prefix가 stg_geolocation에 매칭되지 않는 비율 -> 총 99,441건 중 278건 (0.28%)
SELECT  COUNT(*) AS customer_cnt
		,SUM(g.geolocation_zip_code_prefix IS NULL) AS unmatched_cnt
		,ROUND(SUM(g.geolocation_zip_code_prefix IS NULL) / COUNT(*) * 100, 2) AS unmatched_pct
  FROM  olist_stg.stg_customers AS c
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = c.customer_zip_code_prefix;

SELECT  DISTINCT c.customer_zip_code_prefix
  FROM  olist_stg.stg_customers AS c
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
 WHERE  g.geolocation_zip_code_prefix IS NULL
 LIMIT  50;
  
-- 조인 정합성(2): 판매자 zip_prefix 매칭 누락률 -> 총 3,095건 중 7건 (0.23%)
SELECT  COUNT(*) AS seller_cnt
		,SUM(g.geolocation_zip_code_prefix IS NULL) AS unmatched_cnt
		,ROUND(SUM(g.geolocation_zip_code_prefix IS NULL) / COUNT(*) * 100, 2) AS unmatched_pct
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = s.seller_zip_code_prefix;

SELECT  DISTINCT s.seller_zip_code_prefix
  FROM  olist_stg.stg_sellers AS s
  LEFT
  JOIN  olist_stg.stg_geolocation AS g
    ON  g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
 WHERE  g.geolocation_zip_code_prefix IS NULL
 LIMIT  50;



