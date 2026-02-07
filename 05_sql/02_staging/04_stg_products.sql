/****************************************************************************************************************************************/


/*
 * File: 04_stg_products.sql
 * Description:
 * 	- Source 데이터: olist_raw.products
 *  - 상품(product) 단위의 Staging 테이블 생성
 * 	- 상품 번호(product_id) 기준 조인 안전성 확보 및 분석용 타입 표준화 수행
 * 	- product_category_name_translation과 조인하여 product_category_name_en 컬럼을 포함
 * 	- 정합성 위반 row에 대하여 플래그 컬럼 생성
 * 	- 
 * Notes:
 * 	- product_id를 PK로 사용하였습니다.
 * 	- 식별자 성격을 지닌 ID 컬럼(product_id)은 NOT NULL을 적용하였습니다.
 * 	- 그 외의 컬럼에 대하여는 NULL을 허용하였습니다. (추후 데이터 확장 고려)
 * 	- product_category_name에 결측(공백)이 발견되어 플래그 컬럼을 생성하였습니다. (is_category_blank)
 * 	- product_category_name_en 조인 결과가 결측인 경우 그대로 결측으로 
 * 	- product_weight_g = 0인 row 중 이상치가 확인되어 플래그 컬럼을 생성하였습니다. (is_weight_zero)
 * 	- 치수 결측은 삭제 및 보정하지 않고 product_volume_cm3을 조건부 계산하여 NULL로 유지합니다.
 */


/****************************************************************************************************************************************/


USE olist_stg;


/*
 * products 테이블 사전 DQ:
 *  - Raw 데이터의 유일성/결측/포맷 상태를 확인
 *  - Staging ETL에서의 허용 기준 설정
 * Notes:
 * 	- products 테이블을 stg 레이어에 ETL 하기 전 ETL 기준을 설정하기 위한 스크립트입니다.
 * 	- 해당 스크립트를 통해 products 테이블의 stg 레이어 ETL 기준을 정립합니다.
 */


-- 샘플
SELECT  *
  FROM  olist_raw.products
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_raw.products;

-- row count: 32,951행
SELECT  COUNT(*)
  FROM  olist_raw.products;

-- product_id의 유니크 확인 (PK 가능 여부) -> cnt: 32,951 / distinct_cnt: 32,951 / 중복: 0건 / NULL 및 공백: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT product_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT product_id) AS dup_cnt
		,SUM(product_id IS NULL OR TRIM(product_id) = '') AS blank_cnt
  FROM  olist_raw.products;

-- product_category 결측치 및 분포
-- 	- cnt: 32,951 / distinct_cnt: 74 / 중복: 32,877건 / NULL 및 공백: 610건
-- 	- 74개의 카테고리가 존재함 (상위 3개 -> cama_mesa_banho: 3,029 / esporte_lazer: 2,867 / moveis_decoracao: 2,657)
--	- NULL 및 공백인 row들을 확인 결과, product_category_name이 NULL인 값은 없음(전부 공백)
-- 	- 또한 해당 row들은 product_name_length, product_description_length, product_photos_qty가 모두 NULL값을 가짐
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT product_category_name) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT product_category_name) AS dup_cnt
		,SUM(product_category_name IS NULL OR TRIM(product_category_name) = '') AS blank_cnt
  FROM  olist_raw.products;

SELECT  product_category_name
		,COUNT(*) AS cnt
  FROM  olist_raw.products
 GROUP
    BY  product_category_name
 ORDER
    BY  COUNT(*) DESC;

SELECT  *
  FROM  olist_raw.products
 WHERE  product_category_name IS NULL
    OR  TRIM(product_category_name) = '';

-- 숫자 컬럼 분포
-- products_name_length -> null_cnt: 610건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 5 / 최댓값: 76
SELECT  SUM(product_name_length IS NULL) AS name_null_cnt
		,SUM(product_name_length < 0) AS name_negative_cnt
		,SUM(product_name_length = 0) AS name_zero_cnt
		,MIN(product_name_length) AS name_min
		,MAX(product_name_length) AS name_max
  FROM  olist_raw.products;

-- product_description_length -> null_cnt: 610건 / negative_cnt: 0건 / zero_Cnt: 0건 / 최솟값: 4 / 최댓값: 3,992
SELECT  SUM(product_description_length IS NULL) AS description_null_cnt
		,SUM(product_description_length < 0) AS description_negative_cnt
		,SUM(product_description_length = 0) AS description_zero_cnt
		,MIN(product_description_length) AS description_min
		,MAX(product_description_length) AS description_max
  FROM  olist_raw.products;

-- product_photos_qty -> null_cnt: 610건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 1 / 최댓값: 20
SELECT  SUM(product_photos_qty IS NULL) AS photos_null_cnt
		,SUM(product_photos_qty < 0) AS photos_negative_cnt
		,SUM(product_photos_qty = 0) AS photos_zero_cnt
		,MIN(product_photos_qty) AS photos_min
		,MAX(product_photos_qty) AS photos_max
  FROM  olist_raw.products;
		
-- product_weight_g -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 4건 / 최솟값: 0 / 최댓값: 40,425
SELECT  SUM(product_weight_g IS NULL) AS weight_null_cnt
		,SUM(product_weight_g < 0) AS weight_negative_cnt
		,SUM(product_weight_g = 0) AS weight_zero_cnt
		,MIN(product_weight_g) AS weight_min
		,MAX(product_weight_g) AS weight_max
  FROM  olist_raw.products;
		
-- product_length_cm -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 7 / 최댓값: 105
SELECT  SUM(product_length_cm IS NULL) AS length_null_cnt
		,SUM(product_length_cm < 0) AS length_negative_cnt
		,SUM(product_length_cm = 0) AS length_zero_cnt
		,MIN(product_length_cm) AS length_min
		,MAX(product_length_cm) AS length_max
  FROM  olist_raw.products;

-- product_height_cm -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 2 / 최댓값: 105
SELECT  SUM(product_height_cm IS NULL) AS height_null_cnt
		,SUM(product_height_cm < 0) AS height_negative_cnt
		,SUM(product_height_cm = 0) AS height_zero_cnt
		,MIN(product_height_cm) AS height_min
		,MAX(product_height_cm) AS height_max
  FROM  olist_raw.products;

-- product_width_cm -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 6 / 최댓값: 118
SELECT  SUM(product_width_cm IS NULL) AS width_null_cnt
		,SUM(product_width_cm < 0) AS width_negative_cnt
		,SUM(product_width_cm = 0) AS width_zero_cnt
		,MIN(product_width_cm) AS width_min
		,MAX(product_width_cm) AS width_max
  FROM  olist_raw.products;

-- product_weight_g의 NULL row: 09ff539a621711667c43eba6a3bd8466 / 5eb564652db742ff8f28759cd8d2652a
-- 	- 해당 row들은 product_name_length, product_description_length, product_photos_qty를 제외한 모든 숫자 컬럼이 NULL인 경우와 앞의 컬럼들을 포함한 모든 숫자 컬럼이 NULL인 경우
SELECT  *
  FROM  olist_raw.products
 WHERE  product_weight_g IS NULL;

-- product_weight_g의 zero row: product_weight_g가 0인 row들의 제품 측정값이 정수로 존재함 -> product_weight_g = 0인 row는 이상치
SELECT  *
  FROM  olist_raw.products
 WHERE  product_weight_g = 0;

SELECT  SUM(product_weight_g = 0) AS weight_zero_cnt
		,SUM(product_weight_g = 0 AND (product_length_cm IS NULL OR product_height_cm IS NULL OR product_width_cm IS NULL)) AS weight_zero_but_missing_dims_cnt
  FROM  olist_raw.products;

-- 조인 안전성 확인 -> order_items 테이블에 존재하는 product_id의 고유 개수: 32,951 / 조인 실패 row: 0건
SELECT  COUNT(DISTINCT oi.product_id) AS distinct_product_in_order_items
		,SUM(p.product_id IS NULL) AS missing_in_products_cnt
  FROM  olist_stg.stg_order_items AS oi
  LEFT
  JOIN  olist_raw.products AS p
    ON  p.product_id = oi.product_id;

-- product_category_name_translation 데이터 확인
SELECT  *
  FROM  olist_raw.product_category_name_translation
 LIMIT  10;

-- 조인 안정성 확인
-- translation 키 유일성 확인 -> 동일 product_category_name이 존재하는 경우: 0건
SELECT  product_category_name
		,COUNT(*) AS cnt
  FROM  olist_raw.product_category_name_translation
 GROUP
    BY  1
HAVING  COUNT(*) > 1;

-- 라벨 누락 -> 조인 시 null이 아닌 건수: 32,951건 / null인 건수: 623건
SELECT  COUNT(*) AS category_not_null_cnt
		,SUM(t.product_category_name IS NULL) AS unmapped_cnt
  FROM  olist_raw.products AS p
  LEFT
  JOIN  olist_raw.product_category_name_translation AS t
    ON  NULLIF(LOWER(TRIM(REPLACE(t.product_category_name, '\r', ''))), '') = NULLIF(LOWER(TRIM(REPLACE(p.product_category_name, '\r', ''))), '')
 WHERE  p.product_category_name IS NOT NULL;

/*
 * stg_products 테이블 ETL:
 * 	- 상품 분류명(product_category_name) 표준화(소문자/공백 제거)
 * 	- 상품 치수 기준 파생 컬럼 생성(product_volume_cm3)
 * 	- 원천 미기재 row(product_category_name이 공백) 기준 플래그 컬럼 생성(is_category_blank)
 * 	- 조인 시 발생하는 결측 row(product_category_name_en이 공백) 기준 플래그 컬럼 생성(is_category_en_unmapped)
 * 	- 정합성 위반 row(product_weight_g가 0) 기준 플래그 컬럼 생성(is_weight_zero)
 * 
 * Note:
 * 	- 상품 분류명(product_category_name)의 NULL 및 공백(610건)은 NULL로 표준화하였습니다. (추후 DM 단계에서 그룹화할 예정)
 * 	- 상품 분류명 영문 번역(product_category_name_en)의 NULL 및 공백은 NULL로 표준화하였습니다.
 * 	- 정합성 위반 row를 삭제하지 않고 플래그로 관리함으로써 데이터 손실을 방지하고 추후 분석 단계에서 필터링이 가능하도록 설계하였습니다.
 * 	- 상품 부피(product_volume_cm3)는 정합성이 보장되는 경우에만 조건부 계산이 이루어져야 합니다.
 * 	- 이외 0 값 혹은 NULL 값은 정상적인 값으로 판단하였습니다.
 */

DROP TABLE IF EXISTS olist_stg.stg_products;

-- 테이블 생성
CREATE TABLE olist_stg.stg_products (
	product_id				    VARCHAR(50)   NOT NULL,
	product_category_name		VARCHAR(100)  NULL,
	product_category_name_en	VARCHAR(100)  NULL,
	product_name_length			INT			  NULL,
	product_description_length  INT			  NULL,
	product_photos_qty			INT			  NULL,
	product_weight_g			INT			  NULL,
	product_length_cm			INT			  NULL,
	product_height_cm			INT			  NULL,
	product_width_cm			INT			  NULL,
	
	-- 파생 컬럼: 제품의 부피
	product_volume_cm3			BIGINT		  NULL,
	
	-- 플래그 컬럼
	is_category_blank			TINYINT       NOT NULL,
	is_category_en_unmapped		TINYINT		  NOT NULL,
	is_weight_zero				TINYINT		  NOT NULL,
	
	-- PK, INDEX
	PRIMARY KEY (product_id),
	INDEX idx_stg_products_category_name (product_category_name)	
);

-- 데이터 적재
TRUNCATE TABLE olist_stg.stg_products;


INSERT INTO olist_stg.stg_products (
	product_id,
	product_category_name,
	product_category_name_en,
	product_name_length,
	product_description_length,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm,
	
	product_volume_cm3,
	
	is_category_blank,
	is_category_en_unmapped,
	is_weight_zero
)
SELECT  c.product_id
		,c.product_category_name_norm AS product_category_name
		,t.product_category_name_english_norm AS product_category_name_en
		,c.product_name_length
		,c.product_description_length
		,c.product_photos_qty
		,c.product_weight_g
		,c.product_length_cm
		,c.product_height_cm
		,c.product_width_cm
		
		,CASE WHEN c.product_length_cm IS NULL OR c.product_height_cm IS NULL OR c.product_width_cm IS NULL THEN NULL
			  ELSE c.product_length_cm * c.product_height_cm * c.product_width_cm
			  END AS product_volume_cm3
		
		,CASE WHEN c.product_category_name_norm IS NULL THEN 1
			  ELSE 0
			  END AS is_category_blank
		,CASE WHEN c.product_category_name_norm IS NOT NULL AND t.product_category_name_norm IS NULL THEN 1
			  ELSE 0
			  END AS is_category_en_unmapped
		,CASE WHEN c.product_weight_g = 0 THEN 1
		      ELSE 0 END AS is_weight_zero
  FROM  (
  		SELECT  product_id
		,NULLIF(LOWER(TRIM(REPLACE(product_category_name, '\r', ''))), '') AS product_category_name_norm
		,product_name_length
		,product_description_length
		,product_photos_qty
		,product_weight_g
		,product_length_cm
		,product_height_cm
		,product_width_cm
		FROM  olist_raw.products
		) c
  LEFT
  JOIN  (
  		SELECT  NULLIF(LOWER(TRIM(REPLACE(product_category_name, '\r', ''))), '') AS product_category_name_norm
  				,NULLIF(LOWER(TRIM(REPLACE(product_category_name_english, '\r', ''))), '') AS product_category_name_english_norm
  		  FROM  olist_raw.product_category_name_translation
  		) AS t
    ON  t.product_category_name_norm = c.product_category_name_norm
 WHERE  c.product_id IS NOT NULL
   AND  c.product_id <> ''; -- PK 명시


-- 테이블 검증
-- 샘플
SELECT  *
  FROM  olist_stg.stg_products
 LIMIT  10;

-- 데이터 타입
DESCRIBE olist_stg.stg_products;

-- PK 중복 재확인 -> cnt: 32,951 / distinct_cnt: 32,951 / 중복: 0건 / 공백 및 결측: 0건
SELECT  COUNT(*) AS cnt
		,COUNT(DISTINCT product_id) AS distinct_cnt
		,COUNT(*) - COUNT(DISTINCT product_id) AS dup_cnt
		,SUM(product_id IS NULL OR TRIM(product_id) = '') AS blank_cnt
  FROM  olist_stg.stg_products;

-- 정합성 확인(1): product_category_name 공백 및 결측 -> 610건 (기존 테이블과 차이 없음)
SELECT  SUM(product_category_name IS NULL OR product_category_name = '') AS category_name_blank_cnt
  FROM  olist_stg.stg_products;

-- 정합성 확인(2): 파생 컬럼(product_volume_cm3) 결측 -> 2건 (치수 결측 2건으로 인한 NULL값 -> 이상 없음)
SELECT  SUM(product_volume_cm3 IS NULL) AS volume_blank_cnt
  FROM  olist_stg.stg_products;

-- 정합성 확인(3): 파생 컬럼(product_volume_cm3) 조건 충족 여부 -> 조건 미충족: 0건
SELECT  SUM(product_length_cm IS NOT NULL AND product_height_cm IS NOT NULL AND product_width_cm IS NOT NULL AND product_volume_cm3 IS NULL) AS volume_mismatch_cnt
  FROM  olist_stg.stg_products;

-- 값 범위 이상치 확인
-- products_name_length -> null_cnt: 610건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 5 / 최댓값: 76
SELECT  SUM(product_name_length IS NULL) AS name_null_cnt
		,SUM(product_name_length < 0) AS name_negative_cnt
		,SUM(product_name_length = 0) AS name_zero_cnt
		,MIN(product_name_length) AS name_min
		,MAX(product_name_length) AS name_max
  FROM  olist_stg.stg_products;

-- product_description_length -> null_cnt: 610건 / negative_cnt: 0건 / zero_Cnt: 0건 / 최솟값: 4 / 최댓값: 3,992
SELECT  SUM(product_description_length IS NULL) AS description_null_cnt
		,SUM(product_description_length < 0) AS description_negative_cnt
		,SUM(product_description_length = 0) AS description_zero_cnt
		,MIN(product_description_length) AS description_min
		,MAX(product_description_length) AS description_max
  FROM  olist_stg.stg_products;

-- product_photos_qty -> null_cnt: 610건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 1 / 최댓값: 20
SELECT  SUM(product_photos_qty IS NULL) AS photos_null_cnt
		,SUM(product_photos_qty < 0) AS photos_negative_cnt
		,SUM(product_photos_qty = 0) AS photos_zero_cnt
		,MIN(product_photos_qty) AS photos_min
		,MAX(product_photos_qty) AS photos_max
  FROM  olist_stg.stg_products;
		
-- product_weight_g -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 4건 / 최솟값: 0 / 최댓값: 40,425
SELECT  SUM(product_weight_g IS NULL) AS weight_null_cnt
		,SUM(product_weight_g < 0) AS weight_negative_cnt
		,SUM(product_weight_g = 0) AS weight_zero_cnt
		,MIN(product_weight_g) AS weight_min
		,MAX(product_weight_g) AS weight_max
  FROM  olist_stg.stg_products;
		
-- product_length_cm -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 7 / 최댓값: 105
SELECT  SUM(product_length_cm IS NULL) AS length_null_cnt
		,SUM(product_length_cm < 0) AS length_negative_cnt
		,SUM(product_length_cm = 0) AS length_zero_cnt
		,MIN(product_length_cm) AS length_min
		,MAX(product_length_cm) AS length_max
   FROM  olist_stg.stg_products;

-- product_height_cm -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 2 / 최댓값: 105
SELECT  SUM(product_height_cm IS NULL) AS height_null_cnt
		,SUM(product_height_cm < 0) AS height_negative_cnt
		,SUM(product_height_cm = 0) AS height_zero_cnt
		,MIN(product_height_cm) AS height_min
		,MAX(product_height_cm) AS height_max
  FROM  olist_stg.stg_products;

-- product_width_cm -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 6 / 최댓값: 118
SELECT  SUM(product_width_cm IS NULL) AS width_null_cnt
		,SUM(product_width_cm < 0) AS width_negative_cnt
		,SUM(product_width_cm = 0) AS width_zero_cnt
		,MIN(product_width_cm) AS width_min
		,MAX(product_width_cm) AS width_max
  FROM  olist_stg.stg_products;

-- product_volume_cm3 -> null_cnt: 2건 / negative_cnt: 0건 / zero_cnt: 0건 / 최솟값: 168 / 최댓값: 296,208
SELECT  SUM(product_volume_cm3 IS NULL) AS volume_null_cnt
		,SUM(product_volume_cm3 < 0) AS volume_negative_cnt
		,SUM(product_volume_cm3 = 0) AS volume_zero_cnt
		,MIN(product_volume_cm3) AS volume_min
		,MAX(product_volume_cm3) AS volume_max
  FROM  olist_stg.stg_products;

-- 정합성 플래그 수 확인 -> is_category_blank: 610건 / is_category_en_unmapped: 13건 / is_weight_zero: 4건 (원본 이상치와 차이 없음 is_category_blank + is_category_en_unmapped = 623건 = 두 테이블 간 조인시 결측치 건수)
SELECT  SUM(is_category_blank = 1) AS category_blank_cnt
		,SUM(is_category_en_unmapped = 1) AS category_en_blank_cnt
		,SUM(is_weight_zero = 1) AS weight_zero_cnt
  FROM  olist_stg.stg_products;

-- 조인 정합성(1): 카테고리는 존재하지만 번역이 없는 상품 -> 13건(portateis_cozinha_e_preparadores_de_alimentos: 10건 / pc_gamer: 3건)
SELECT  COUNT(*) AS translation_missing_cnt
  FROM  olist_stg.stg_products AS p
  LEFT
  JOIN  olist_raw.product_category_name_translation AS pcnt
    ON  pcnt.product_category_name = p.product_category_name
 WHERE  p.product_category_name IS NOT NULL
   AND  pcnt.product_category_name IS NULL;

SELECT  p.product_category_name
		,COUNT(*) AS cnt
  FROM  olist_stg.stg_products AS p
  LEFT
  JOIN  olist_raw.product_category_name_translation AS pcnt
    ON  pcnt.product_category_name = p.product_category_name
 WHERE  p.product_category_name IS NOT NULL
   AND  pcnt.product_category_name IS NULL
 GROUP
    BY  p.product_category_name
 ORDER
    BY  cnt DESC;

-- 조인 정합성(2): products에는 존재하지만 order_items에는 존재하지 않는 상품 -> 0건
SELECT  COUNT(*) AS products_not_sold_cnt
  FROM  olist_stg.stg_products AS p
  LEFT
  JOIN  olist_stg.stg_order_items AS oi
    ON  oi.product_id = p.product_id
 WHERE  oi.product_id IS NULL;




