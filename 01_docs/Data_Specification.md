 
# 데이터 명세서

```
본 프로젝트는 데이터의 신뢰성과 재현성을 확보하기 위해
Raw / Staging / Data Mart의 3-레이어 구조로 설계하였습니다.

- Raw: 외부 원본 데이터를 가공 없이 보존하기 위한 레이어
- Staging: 데이터 타입 변환, 결측 처리, 컬럼 정리를 수행하는 정제 레이어
- Data Mart: 분석 및 KPI 산출에 최적화된 집계/차원 레이어
```


---

## 1. Raw 레이어

```
Raw 레이어는 외부 데이터 소스(csv 파일)로부터 수집된 원본 데이터를 
가공 없이 그대로 적재·보존한 레이어입니다.

이 레이어의 주요 목적은
- 원본 데이터의 형태를 그대로 유지하여 재현성을 확보하고
- 이후 Staging 및 Data Mart 레이어에서 수행되는 전처리 및 변환의 기준점 역할을 하는 것
입니다.

Raw 레이어는 다음과 같은 원칙으로 구성하였습니다.

1. 원본 데이터 구조 유지
   - 컬럼명은 원본 csv의 헤더를 그대로 사용
   - 컬럼의 의미를 변경하거나 파생 컬럼을 생성하지 않음

2. 데이터 가공 및 전처리 미수행
   - 데이터 타입 변환, 결측치 처리, 중복 제거 수행하지 않음
   - 모든 정제 및 비즈니스 로직을 수행하지 않음

3. 원본 데이터 손실 방지
   - 결측치, 이상치, 중복 데이터 그대로 적재
     
Raw 레이어의 모든 테이블은 MySQL의 LOAD DATA INFILE 방식을 사용하여 적재하였습니다.
csv 파일은 MySQL 서버에서 접근 가능한 경로에 위치하였으며, 
헤더 행은 IGNORE 1 ROWS 옵션을 통해 제외하였습니다.
숫자형 컬럼(INT, DECIMAL)의 경우,
csv 상의 빈 값('')을 NULL로 변환하여 적재하였습니다.
```


### Raw 레이어 테이블 목록


| 테이블명                              | 설명                   |
| :-------------------------------- | -------------------- |
| orders                            | 주문 단위의 기본 정보         |
| order_items                       | 주문 내 상품 단위 정보        |
| order_payments                    | 주문별 결제 정보            |
| order_reviews                     | 주문에 대한 고객 리뷰 정보      |
| customers                         | 고객 기본 정보             |
| products                          | 상품 메타 정보             |
| sellers                           | 판매자 정보               |
| geolocation                       | 우편번호 단위 위치 정보        |
| product_category_name_translation | 상품 카테고리 번역 정보(영문 번역) |


### Raw 레이어 테이블 요약 명세


#### raw.orders

```
주문 단위의 기본 정보가 저장된 테이블로,
주문 상태 및 주문/배송 관련 시각 정보를 포함합니다.
```

| 컬럼명                           | 데이터 타입  |
| :---------------------------- | ------- |
| order_id                      | VARCHAR |
| customer_id                   | VARCHAR |
| order_status                  | VARCHAR |
| order_purchase_timestamp      | VARCHAR |
| order_approved_at             | VARCHAR |
| order_delivered_carrier_date  | VARCHAR |
| order_delivered_customer_date | VARCHAR |
| order_estimated_delivery_date | VARCHAR |

#### raw.order_items

```
각 주문에 포함된 상품 단위의 상세 정보를 저장한 테이블입니다.
```

| 컬럼명                 | 데이터 타입  |
| :------------------ | ------- |
| order_id            | VARCHAR |
| order_item_id       | VARCHAR |
| product_id          | VARCHAR |
| seller_id           | VARCHAR |
| shipping_limit_date | VARCHAR |
| price               | DECIMAL |
| feight_value        | DECIMAL |


#### raw.order_payments

```
주문별 결제 수단 및 결제 금액 정보를 저장한 테이블입니다.
```

| 컬럼명                 | 데이터 타입  |
| :------------------ | ------- |
| order_id            | VARCHAR |
| payment_sequential  | INT     |
| payment_type        | VARCHAR |
| payment_intallments | INT     |
| payment_value       | DECIMAL |


#### raw.order_reviews

```
주문에 대한 고객 리뷰 및 평점 정보를 저장한 테이블입니다.
텍스트 컬럼 특성상 ESCAPE 옵션을 적용하여 적재하였습니다.
```

| 컬럼명                     | 데이터 타입  |
| :---------------------- | ------- |
| review_id               | VARCHAR |
| order_id                | VARCHAR |
| review_score            | INT     |
| review_comment_title    | TEXT    |
| review_comment_message  | TEXT    |
| review_creation_date    | VARCHAR |
| review_answer_timestamp | VARCHAR |


#### raw.customers

```
고객 식별자 및 지역 정보를 저장한 테이블입니다.
```

| 컬럼명                      | 데이터 타입  |
| :----------------------- | ------- |
| customer_id              | VARCHAR |
| customer_unique_id       | VARCHAR |
| customer_zip_code_prefix | VARCHAR |
| customer_city            | VARCHAR |
| customer_state           | VARCHAR |


#### raw.products

```
상품의 카테고리 및 물리적 특성 정보를 저장한 테이블입니다.
```

| 컬럼명                        | 데이터 타입  |
| :------------------------- | ------- |
| product_id                 | VARCHAR |
| product_category_name      | VARCHAR |
| product_name_length        | INT     |
| product_description_length | INT     |
| product_photos_qty         | INT     |
| product_weight_g           | INT     |
| product_length_cm          | INT     |
| product_height_cm          | INT     |
| product_width_cm           | INT     |


#### raw.sellers

```
판매자의 위치 및 식별 정보를 저장한 테이블입니다.
```

| 컬럼명                    | 데이터 타입  |
| :--------------------- | ------- |
| seller_id              | VARCHAR |
| seller_zip_code_prefix | VARCHAR |
| seller_city            | VARCHAR |
| seller_state           | VARCHAR |


#### raw.geolocation

```
우편번호 단위의 위도·경도 정보를 저장한 테이블입니다.
```

| 컬럼명                         | 데이터 타입  |
| :-------------------------- | ------- |
| geolocation_zip_code_prefix | VARCHAR |
| geolocation_lat             | DECIMAL |
| geolocation_lng             | DECIMAL |
| geolocation_city            | VARCHAR |
| geolocation_state           | VARCHAR |


#### raw.product_category_name_translation

```
상품 카테고리의 포르투갈어-영어 번역 정보를 저장한 테이블입니다.
```

| 컬럼명                           | 데이터 타입  |
| :---------------------------- | ------- |
| product_category_name         | VARCHAR |
| product_category_name_english | VARCHAR |


---

## 2. Staging 레이어


```
Staging 레이어는 Raw 레이어에 적재된 원본 데이터를 기반으로,
분석 및 Data Mart 설계를 위한 중간 정제 계층입니다.

이 레이어의 주요 목적은 다음과 같습니다.

1. Raw 데이터의 구조적·의미적 정합성 검증
    - Primary Key 유니크 여부 확인
    - 필수 컬럼 NULL 여부 검증
    - 테이블 간 조인 정합성 검증

2. 데이터 타입 및 포맷 표준화
    - 문자열 기반 날짜/시간 컬럼을 DATETIME / DATE 타입으로 변환
    - 문자열 컬럼의 공백 제거 및 대소문자 표준화
    - 분석에 필요한 파생 날짜 컬럼 생성 (purchase_dt, delivered_dt 등)

3. 분석에 필요한 파생 컬럼 생성
	- 주문 단위 기준 시점 컬럼 (purchase_date 등)
	- 금액 관련 파생 컬럼 (item_total_value 등)
	- 지역 분석을 위한 파생 컬럼 (city_state)

4. 데이터 품질 관리 적용
	- 정합성 위반 데이터는 삭제하지 않고 플래그 컬럼(is_*)으로 관리
	- 비즈니스 해석에 따라 분석 단계에서 유연하게 필터링 가능하도록 설계

5. 조인 안전성 확보
	- 주문, 고객, 상품, 판매자, 결제, 리뷰 등 주요 엔티티 간
	  조인 키를 기준으로 정합성 검증 수행
	- geolocation 테이블과 같이 비즈니스 특성상 예외가 존재하는 경우,
	  주석 및 추가 검증 로직을 통해 관리

Staging 레이어에서는 데이터의 해석이나 KPI 계산은 수행하지 않으며,
데이터의 신뢰성과 사용 가능성 확인을 범위로 합니다.
비즈니스 지표 계산 및 집계는 이후 Data Mart 레이어에서 수행합니다.
```


### Staging 레이어 테이블 목록


| 테이블명                              | 설명                   |
| :-------------------------------- | -------------------- |
| orders                            | 주문 단위의 기본 정보         |
| order_items                       | 주문 내 상품 단위 정보        |
| order_payments                    | 주문별 결제 정보            |
| order_reviews                     | 주문에 대한 고객 리뷰 정보      |
| customers                         | 고객 기본 정보             |
| products                          | 상품 메타 정보             |
| sellers                           | 판매자 정보               |
| geolocation                       | 우편번호 단위 위치 정보        |
| product_category_name_translation | 상품 카테고리 번역 정보(영문 번역) |


### Staging 레이어 테이블 명세


#### stg.orders

```
테이블명: olist_stg.stg_orders

테이블 source: olist_raw.orders

Primary Key: order_id

Indexes: 
	- idx_stg_orders_customer_id (customer_id): 고객 조인용 인덱스
	- idx_stg_orders_purchase_dt (order_purchase_dt): 기간 필터/집계용 인덱스

설계 목적:
	- Raw 레이어의 timestamp 문자열 컬럼을 DATETIME/DATE로 표준화
	- 주문 기준 시점(purchase) 중심 파생 컬럼 생성 (기간 집계/리드타임 분석용)
	- 데이터 삭제 없이 정합성 위반을 플래그로 정리 (추후 분석시 유연한 필터링)

적재 규칙:
	- order_purchase_dt는 분석 기준 시점으로 NOT NULL을 보장
	- 사전 DQ 상 파싱 실패는 0건이었으나, 안전을 위해 purchase_dt IS NOT NULL 조건 제약
	- 정합성 위반 row는 삭제하지 않고 플래그로 관리 (데이터 손실 방지)
	- 리드타임 파생 컬럼은 조건부 계산 적용 
	  (시간 정합성이 깨진 케이스 발견 -> 컬럼 계산 시 NULL 처리)
	- order_estimated_delivery_dt 컬럼은 DATE로 표준화 (일자 중심 컬럼으로 판단됨)
	- 조인 정합성에 이상이 발견되었으나,
	  (orders에는 있는데 order_items에는 없는 주문(775건)/
	  orders에는 있는데 order_payments에는 없는 주문(1건))
	  stg 레이어에서는 따로 파생 컬럼으로 저장하지 않았습니다.
	- 해당 정합성은 추후 DM 레이어에서 통합 정합성 관리 뷰로 관리할 예정입니다.

특이 사항(DQ 결과):
	- 주문 시간 기준 시간 순서 정합성 위반 row 존재: 166건
	- 주문 상태와 배송 시간 간 정합성 위반 row 존재: 14건
	- shipped/invoiced 상태이나 배송 인계 시간이 존재하지 않는 row 존재: 314건
	  (비즈니스상 오류 여부를 단정하기 어려움 -> 추적용 플래그로 관리)
	- orders에는 있는데 order_items에는 없는 주문: 775건
	- orders에는 있는데 order_payments에는 없는 주문: 1건
```


- **컬럼 명세 (1): 원본 컬럼 - 타입 표준화**

| 컬럼명                             | 타입          | NULL | 설명          | 비고                       |
| :------------------------------ | ----------- | ---- | ----------- | ------------------------ |
| **order_id**                    | VARCHAR(50) | N    | 주문 고유 식별자   | PK                       |
| **customer_id**                 | VARCHAR(50) | N    | 고객 식별자      | customers 조인 키           |
| **order_status**                | VARCHAR(20) | N    | 주문 상태       | LOWER + TRIM 적용          |
| **order_purchase_dt**           | DATETIME    | N    | 주문 구매 시각    | 분석 기준 시점                 |
| **order_approved_dt**           | DATETIME    | Y    | 결제 승인 시각    | 승인 전 주문은 NULL            |
| **order_delivered_carrier_dt**  | DATETIME    | Y    | 택배사 인계 시각   | 빈 문자열은 NULL 처리 후 파싱      |
| **order_delivered_customer_dt** | DATETIME    | Y    | 고객 배송 완료 시각 | 배송 미완료 시 NULL            |
| **order_estimated_delivery_dt** | DATE        | Y    | 예상 배송 일자    | 일자 단위 의미가 핵심 -> DATE로 처리 |

- **컬럼 명세 (2): 시간 관련 파생 컬럼**

| 컬럼명                     | 타입       | NULL | 설명     | 파생 기준                    |
| :---------------------- | -------- | ---- | ------ | ------------------------ |
| **order_purchase_date** | DATE     | N    | 구매 일자  | DATE(order_purchase_dt)  |
| **order_year**          | SMALLINT | N    | 구매 연도  | YEAR(order_purchase_dt)  |
| **order_month**         | TINYINT  | N    | 구매 월   | MONTH(order_purchase_dt) |
| **order_year_month**    | CHAR(7)  | N    | 구매 연-월 | YYYY-MM                  |

- **컬럼 명세 (3): 배송/승인 관련 파생 지표**

| 컬럼명                     | 타입         | NULL | 설명              | 파생 기준                                                                                                             |
| :---------------------- | ---------- | ---- | --------------- | ----------------------------------------------------------------------------------------------------------------- |
| **approve_lead_days**   | INT        | Y    | 주문 -> 승인 소요일    | approved - purchase<br>(approved_dt >= purchase_dt일 때만)                                                           |
| **delivery_lead_days**  | INT        | Y    | 주문 -> 배송 완료 소요일 | delivered_customer - purchase<br>(delivered_dt >= purchase_dt 일 때만)                                               |
| **delivery_delay_days** | INT        | Y    | 배송 지연 일수        | deliverd_customer - estimated<br>(delivered_dt & estimated_dt가 존재하고 delivered_dt >= purchase_dt 일 때만 / 음수는 조기 배송) |
| **is_delivered**        | TINYINT(1) | N    | 배송 완료 여부        | delivery_customer_dt <br>IS NOT NULL이면 1 else 0                                                                   |
| **is_canceled**         | TINYINT(1) | N    | 주문 취소 여부        | status IN<br>('canceled', 'unavailable')이면 1 else 0                                                               |

- **컬럼 명세 (4): 플래그 지표**

| 컬럼명                    | 타입         | NULL | 설명                       | 파생 기준                                                                                 |
| :--------------------- | ---------- | ---- | ------------------------ | ------------------------------------------------------------------------------------- |
| is_time_inconsistent   | TINYINT(1) | N    | 시간 순서 정합성 위반             | approved/carrier/delivered 중 <br>하나라도 < purchase_dt이면 1                               |
| is_status_inconsistent | TINYINT(1) | N    | 상태-시간 강한 위반              | delivered인데 delivered_dt가 NULL OR canceled/unavailable인데 delivered_dt가 존재             |
| is_carrier_dt_missing  | TINYINT(1) | N    | shipped/invoiced 인계시각 누락 | order_status IN <br>('shipped', 'invoiced') AND order_delivered_carrier_dt<br>IS NULL |


#### stg.customers

```
테이블명: olist_stg.stg_customers

테이블 source: olist_raw.customers

Primary Key: customer_id

Indexes: 
	- idx_stg_customers_unique_id (customer_unique_id): 사람 단위 분석/조회용 인덱스
	- idx_stg_customers_zip_prefix (customer_zip_code_prefix): 지역 기반 집계/필터용
	- idx_stg_customers_state (customer_state): 지역(주) 기반 집계/필터용

설계 목적:
	- 고객 키 기준 조인 안전성 확보 (customer_id / customer_unique_id)
	- 각 컬럼들의 타입 변경(문자열 고정 / 길이 제한) 및 표준화(공백 제거 / 대,소문자 통일)
	- city와 state를 결합한 customer_city_state 컬럼 생성 (단위 집계/필터 편의성)

적재 규칙:
	- customer_id와 customer_unique_id는 조인 및 엔티티 식별의 핵심 키이므로 
	  NOT NULL을 보장
	- 사전 DQ 상 결측 및 공백 row는 0건이었으나, 
	  안전을 위해 customer_id(PK) IS NOT NULL 조건 적용
	- 이외 컬럼은 사전 DQ상 결측 및 공백이 0건이었으나, 운영 확장 가능성을 고려해
	  NULL 허용 
	- customer_zip_code_prefix를 길이 5로 고정 (CHAR(5)) / 
	  customer_state를 길이 2로 고정 (CHAR(2)) 
	  -> 사전 DQ상 길이/타입 이상치는 없었으나, 조인 및 타입 일관성 확보를 위해 문자열로 고정

특이 사항(DQ 결과):
	- customer_unique_id:
		  전체 개수: 99,441(전체 row와 동일)
		  중복 개수: 3,345(고유 개수: 96,096)
		  공백 개수: 0건
	- customer_zip_code_prefix / customer_city / customer_state의 결측 및 공백 건수:
	  0 건
	- customer_state 길이 이상치(길이가 2가 아닌 state): 0건
	- customer_zip_code_prefix의 길이 이상치:
		  최소 길이: 5
		  최대 길이: 5
		  문자열 건수: 0건
```


- **컬럼 명세(1): 원본 컬럼 - 타입 표준화

| 컬럼명                      | 타입           | NULL | 설명                | 비고                          |
| :----------------------- | ------------ | ---- | ----------------- | --------------------------- |
| **customer_id**              | VARCHAR(50)  | N    | 고객 / 주문 식별자       | PK                          |
| **customer_unique_id**       | VARCHAR(50)  | N    | 고객 고유 식별자         |                             |
| **customer_zip_code_prefix** | CHAR(5)      | Y    | 고객 우편 번호 기반 위치 정보 | 오직 숫자만 존재 -> 안전성을 위해 문자열 처리 |
| **customer_city**            | VARCHAR(100) | Y    | 고객 도시 정보          | LOWER, TRIM 적용              |
| **customer_state**           | CHAR(2)      | Y    | 고객 주 정보           | UPPER, TRIM 적용              |

- **컬럼 명세(2): 파생 컬럼 - 도시-주 결합

| 컬럼명                     | 타입           | NULL | 설명           | 파생 기준                                       |
| :---------------------- | ------------ | ---- | ------------ | ------------------------------------------- |
| **customer_city_state** | VARCHAR(200) | Y    | 고객 도시 - 주 정보 | CONCAT(customer_city, '__', customer_state) |


#### stg.order_items

```
테이블명: olist_stg.stg_order_items

테이블 source: olist_raw.order_items

Primary Key: (order_id, order_item_id)

Indexes: 
	- idx_stg_order_items_product_id (product_id): 제품 단위 분석/조회용
	- idx_stg_order_items_seller_id (seller_id): 판매자 단위 분석/조회용
	- idx_stg_order_items_shipping_limit_dt (shipping_limit_dt): 기간 집계/필터용

설계 목적:
	- 주문 키 기준 조인 안전성 확보 (order_id)
	- 제품 키 기준 조인 안전성 확보 (product_id)
	- 판매자 키 기준 조인 안전성 확보 (seller_id)
	- shipping_limit_date를 통한 날짜 컬럼 생성(shipping_limit_dt/shipping_limit_date)
	- price와 freight_value를 합한 총 가격 컬럼 생성(item_total_value)
	- order_item_id의 순번 컬럼 생성 (order_item_seq)

적재 규칙:
	- order_id, order_item_id, product_id, seller_id는 
	  조인 및 엔티티 식별의 핵심 키이므로 NOT NULL을 보장
	- 사전 DQ 상 결측 및 공백 row는 0건이었으나, 
	  안전을 위해 핵심 키에는 NOT NULL 조건 적용
	- 이외 컬럼은 사전 DQ상 결측 및 공백이 0건이었으나, 운영 확장 가능성을 고려해
	  NULL 허용 
	- order_item_id는 주문 순번으로 보이나, 원본 데이터를 최대한 건드리지 않기 위해 
	  순번 컬럼(order_item_seq)를 생성해 INT로 지정
	- 총 가격의 경우(item_total_value) 가격 컬럼의 사전 DQ 상 NULL 값이 없었으나, 
	  price와 freight_value 중 하나라도 NULL 값이 있을 경우 NULL 값을 출력하도록 지정

특이 사항(DQ 결과):
	- (order_id, order_item_id) 중복: 0건
	- order_item_id 분포:
		  1~21까지의 주문 순번으로 이루어짐(1: 98,666 / 2: 9,803 / 3: 2,287 ...)
	- price 값 분포:
		  NULL: 0건
		  음수: 0건
		  0원: 0건
		  최솟값: 0.85
		  최댓값: 6,735
	- freight_value 값 분포:
		  NULL: 0건
		  음수: 0건
		  0원: 383건 (배송비의 경우 0원일 수 있기 때문에 정상치로 판단)
		  최솟값: 0
		  최댓값: 409.68
	- 배송 마감시각이 주문 구매시각보다 이른 경우: 0건
```


- **컬럼 명세(1): 원본 컬럼 - 타입 표준화

| 컬럼명                   | 타입             | NULL | 설명        | 비고                                                 |
| :-------------------- | -------------- | ---- | --------- | -------------------------------------------------- |
| **order_id**          | VARCHAR(50)    | N    | 주문 식별자    | 단독 PK 불가능 / PK(복합)의 구성 요소                          |
| **order_item_id**     | VARCHAR(50)    | N    | 주문 상품 식별자 | 단독 PK 불가능 / PK(복합)의 구성 요소                          |
| **product_id**        | VARCHAR(50)    | N    | 제품 식별자    | 주문상품 단위 조인 키                                       |
| **seller_id**         | VARCHAR(50)    | N    | 판매자 식별자   | 주문상품 단위 조인 키                                       |
| **shipping_limit_dt** | DATETIME       | Y    | 배송 마감 일시  | 날짜와 시간 모두 의미가 있으므로 DATETIME으로 지정                   |
| **price**             | DECIMAL(10, 2) | Y    | 가격        | 사전 DQ 기준 소수 2자리 존재,<br>금액 연산 안정성 위해 DECIMAL 사용     |
| **freight_value**     | DECIMAL(10, 2) | Y    | 배송 가격     | 사전 DQ 기준 0원 존재(정상 케이스),<br>금액 연산 안정성 위해 DECIMAL 사용 |


- **컬럼 명세(2): 시간 관련 파생 컬럼**

| 컬럼명                     | 타입   | NULL | 설명       | 파생 기준                   |
| :---------------------- | ---- | ---- | -------- | ----------------------- |
| **shipping_limit_date** | DATE | Y    | 배송 마감 일자 | DATE(shipping_limit_dt) |

- **컬럼 명세(3): 기타 파생 컬럼**

| 컬럼명                  | 타입             | NULL | 설명             | 파생 기준                                                                                                     |
| :------------------- | -------------- | ---- | -------------- | --------------------------------------------------------------------------------------------------------- |
| **order_item_seq**   | INT            | Y    | 주문 내 아이템 순번    | CAST(TRIM(order_item_id) AS UNSIGNED)                                                                     |
| **item_total_value** | DECIMAL(10, 2) | Y    | 주문 상품 단위 금액의 합 | CASE WHEN price IS NULL OR freight_value IS NULL THEN NULL<br>ELSE ROUND(price + freight_value, 2)<br>END |


#### stg_products

```
테이블명: olist_stg.stg_products

테이블 source: olist_raw.products

Primary Key: product_id

Indexes: 
	- idx_stg_products_product_category_name (product_category_name): 
	  상품 분류명 기준 집계/조인 인덱스

설계 목적:
	- 상품 키(product_id) 기준 조인 안전성 확보
	- 상품 분류명(product_category_name) 표준화(소문자/공백 제거, 공백->NULL)로
	  카테고리 분석 및 번역 테이블 조인 안전성 확보
	- 길이, 높이, 너비 컬럼을 통해 파생 컬럼 생성(product_volume_cm3)
	- 데이터 삭제 없이 정합성 위반을 플래그로 관리(is_category_blank/is_weight_zero)

적재 규칙:
	- product_id는 엔티티 식별의 핵심 키이므로 NOT NULL을 보장하며 PK로 설정
	- product_category_name은 원천 데이터에 공백(610건)이 존재하여 
	  TRIM을 적용한 후 공백은 NULL로 표준화하여 적재 (추후 DM 단계에서 그룹화 예정)
	- 그 외 수치 컬럼(길이/설명 길이/사진 수/무게/치수)은 원본을 보존하기 위해 
	  값 보정 없이 적재하며 NULL을 허용
	- 치수 컬럼(길이/높이/너비)이 모두 NULL인 항목(2건)은 
	  원천 데이터에서 아예 측정/제공되지 않은 데이터로 판단
	- product_volume_cm3는 product_length_cm, product_height_cm, 
	  product_width_cm 모두 존재할 때만 계산하며, 하나라도 NULL이면 NULL로 유지
	- 플래그 컬럼은 row 삭제 없이 DQ 케이스를 식별하기 위해 생성하며 NOT NULL을 보장

특이 사항(DQ 결과):
	- product_id 중복: 0건
	- product_category_name NULL: 610건
	- product_name_length 분포:
		  null_cnt: 610건
		  음수: 0건
		  0값: 0건
		  최솟값: 5
		  최댓값: 76
	- product_description_length 분포:
		  null_cnt: 610건
		  음수: 0건
		  0값: 0건
		  최솟값: 4
		  최댓값: 3,992
	- product_photos_qty 분포:
		  null_cnt: 610건
		  음수: 0건
		  0값: 0건
		  최솟값: 1
		  최댓값: 20
	- product_weight_g 분포:
		  null_cnt: 2건
		  음수: 0건
		  0값: 4건
		  최솟값: 0
		  최댓값: 40,425
	- product_length_cm 분포:
		  null_cnt: 2건
		  음수: 0건
		  0값: 0건
		  최솟값: 7
		  최댓값: 105
	- product_height_cm 분포:
		  null_cnt: 2건
		  음수: 0건
		  0값: 0건
		  최솟값: 2
		  최댓값: 105
	- product_width_cm 분포:
		  null_cnt: 2건
		  음수: 0건
		  0값: 0건
		  최솟값: 6
		  최댓값: 118
	- product_volume_cm3(파생 컬럼):
		  null_cnt: 2건
		  음수: 0건
		  0값: 0건
		  최솟값: 168
		  최댓값: 296,208
	- 플래그 분포
		  is_category_blank: 610건
		  is_weight_zero: 4건	  
	
```


- **컬럼 명세(1): 원본 컬럼**

| 컬럼명                        | 타입           | NULL | 설명           | 비고            |
| :------------------------- | ------------ | ---- | ------------ | ------------- |
| **product_id**                 | VARCHAR(50)  | N    | 상품 식별자       | PK            |
| **product_category_name**      | VARCHAR(100) | Y    | 상품 분류명       | 공백은 NULL로 표준화 |
| **product_name_length**        | INT          | Y    | 상품명 문자열 길이   | 원본 값 유지       |
| **product_description_length** | INT          | Y    | 상품 설명 문자열 길이 | 원본 값 유지       |
| **product_photos_qty**         | INT          | Y    | 상품 사진 개수     | 원본 값 유지       |
| **product_weight_g**           | INT          | Y    | 상품 무게(g)     | 0값 존재(이상치)    |
| **product_length_cm**          | INT          | Y    | 상품 길이(cm)    | 원본 값 유지       |
| **product_height_cm**          | INT          | Y    | 상품 높이(cm)    | 원본 값 유지       |
| **product_width_cm**           | INT          | Y    | 상품 너비(cm)    | 원본 값 유지       |

- **컬럼 명세(2): 파생 컬럼 - 상품 부피**

| 컬럼명                    | 타입     | NULL | 설명                            | 파생 기준                                          |
| :--------------------- | ------ | ---- | ----------------------------- | ---------------------------------------------- |
| **product_volume_cm3** | BIGINT | Y    | 상품 부피<br>(3개 치수 모두 존재할 때만 계산) | length x height x width (세 컬럼이 모두 NOT NULL일 때) |

- **컬럼 명세(3): 플래그 컬럼**

| 컬럼명                   | 타입         | NULL | 설명             | 파생 기준                              |
| :-------------------- | ---------- | ---- | -------------- | ---------------------------------- |
| **is_category_blank** | TINYINT(1) | N    | 상품 카테고리 미기재 여부 | product_category_name IS NULL 이면 1 |
| **is_weight_zero**    | TINYINT(1) | N    | 상품 무게 이상치 여부   | product_weight_g = 0 이면 1          |


#### stg.sellers

```
테이블명: olist_stg.stg_sellers

테이블 source: olist_raw.sellers

Primary Key: seller_id

Indexes: 
	- idx_stg_sellers_zip_prefix (seller_zip_code_prefix): 지역 기반 집계/필터용
	- idx_stg_sellers_state (seller_state): 지역(주) 기반 집계/필터용

설계 목적:
	- 판매자 키 기준 조인 안전성 확보 (seller_id)
	- 각 컬럼들의 타입 변경(문자열 고정 / 길이 제한) 및 표준화(공백 제거 / 대,소문자 통일)
	- city와 state를 결합한 seller_city_state 컬럼 생성 (단위 집계/필터 편의성)

적재 규칙:
	- seller_id는 조인 및 엔티티 식별의 핵심 키이므로 NOT NULL을 보장
	- 사전 DQ 상 결측 및 공백 row는 0건이었으나, 
	  안전을 위해 seller_id(PK) IS NOT NULL 조건 적용
	- 이외 컬럼은 사전 DQ상 결측 및 공백이 0건이었으나, 운영 확장 가능성을 고려해
	  NULL 허용 
	- seller_zip_code_prefix를 길이 5로 고정 (CHAR(5)) / 
	  seller_state를 길이 2로 고정 (CHAR(2)) 
	  -> 사전 DQ상 길이/타입 이상치는 없었으나, 조인 및 타입 일관성 확보를 위해 문자열로 고정

특이 사항(DQ 결과):
	- seller_zip_code_prefix / seller_city / seller_state의 결측 및 공백 건수:
	  0 건
	- seller_state 길이 이상치(길이가 2가 아닌 state): 0건
	- seller_zip_code_prefix의 길이 이상치:
		  최소 길이: 5
		  최대 길이: 5
		  문자열 건수: 0건
	- city 분포:
		  sao paulo: 22.46%(695건) / curitiba: 4.1%(127건) / 
		  rio de janeiro: 3.1%(96건) ... 총 610개
	- state 분포:
		  SP: 59.74%(1,849건) / PR: 11.28%(349건) / 
		  MG: 7.88%(244건) ... 총 23개의 state
	- city_state 분포:
		  sao paulo_SP: 22.46%(695건) / curitiba_PR: 4.01%(124건) / 
		  rio de janeiro_RJ: 3%(93건) ... 총 635개
		  (city는 610개로 집계되며, 동일 city가 서로 다른 state에 존재하여 
		  city_state의 개수가 증가함)
	- order_items에는 있지만 sellers에는 없는 seller_id 0건
	- sellers에는 있지만 order_items에는 없는 seller_id 0건
```


-  **컬럼 명세(1): 원본 컬럼 - 타입 표준화

| 컬럼명                        | 타입           | NULL | 설명                 | 비고                          |
| :------------------------- | ------------ | ---- | ------------------ | --------------------------- |
| **seller_id**              | VARCHAR(50)  | N    | 판매자 식별자            | PK                          |
| **seller_zip_code_prefix** | CHAR(5)      | Y    | 판매자 우편 번호 기반 위치 정보 | 오직 숫자만 존재 -> 안전성을 위해 문자열 처리 |
| **seller_city**            | VARCHAR(100) | Y    | 판매자 도시 정보          | LOWER, TRIM 적용              |
| **seller_state**           | CHAR(2)      | Y    | 판매자 주 정보           | UPPER, TRIM 적용              |

- **컬럼 명세(2): 파생 컬럼 - 도시-주 결합

| 컬럼명                   | 타입           | NULL | 설명            | 파생 기준                                     |
| :-------------------- | ------------ | ---- | ------------- | ----------------------------------------- |
| **seller_city_state** | VARCHAR(200) | Y    | 판매자 도시 - 주 정보 | CONCAT(sellers_city, '__', sellers_state) |


#### stg.order_payments

```
테이블명: olist_stg.stg_order_payments

테이블 source: olist_raw.order_payments

Primary Key: (order_id, payment_sequential)

Indexes:
	- idx_stg_order_payments_order_id (order_id): 주문 식별자 기준 조인 키
	- idx_stg_order_payments_type (payment_type): 결제 수단 기반 집계/필터용

설계 목적:
	- 주문 식별자(order_id) 기준 조인 안전성 확보
	- 문자열 컬럼(payment_type) 표준화 (공백 제거/대,소문자 통일)
	- 정합성 위반 row는 삭제하지 않고 플래그로 관리 
	  (is_installments_zero / is_payment_value_zero)

적재 규칙:
	- order_id와 payment_sequential은 복합 PK 키이므로 NOT NULL을 보장
	- 그 외 컬럼은 원본을 보존과 데이터 확장성을 고려하여 NULL을 허용
	- payment_type은 LOWER와 TRIM을 적용하여 표준화
	- payment_installments의 0값은 이상치로 보이나, 데이터를 원본 그대로 보존하기 위해
	  해당 데이터를 삭제/보정하지 않고 플래그 컬럼을 생성 (is_installments_zero)
	- payment_value의 0값은 이상치로 단정할 수 없으나(바우처/쿠폰으로 인한 0원),
	  분석 시 별도 세그먼트로 분리해 보기 위해 
	  특이 케이스 식별용 플래그 컬럼을 생성 (is_payment_value_zero)
	- 조인 정합성 과정에서 orders에는 있으나, 
	  order_payments에는 없는 주문 내역(1건)이 발견되었으나, 
	  따로 처리하지 않고 추후 DM 레이어에서 관리 계획 
	  (order_payments에는 없는 row이기 때문)

특이 사항(DQ 결과):
	- payment_type 분포:
		  credit_card: 73.92% (76,795)건 / boleto: 19.04% (19,784건) / 
		  voucher: 5.56% (5,775건) / debit_card: 1.47% (1,529건) / 
		  not_defined: 0.00% (3건)
	- payment_sequential 분포:
		  결측치: 0건
		  음수: 0건
		  0값: 0건
		  최솟값: 1
		  최댓값: 29
	- payment_installments 분포:
		  결측치: 0건
		  음수: 0건
		  0값: 2건 (이상치일 것이라 판단되어 플래그 컬럼 생성)
		  최솟값: 0
		  최댓값: 24
	- payment_value 분포:
		  결측치: 0건
		  음수: 0건
		  0값: 9건 (바우처/쿠폰 등으로 인한 0원일 가능성이 있어 이상치가 아닌 것으로 판단)
		  최솟값: 0
		  최댓값: 13,664.08
	- order_id당 결제 row 수 분포:
		  1: 96,479 / 2: 2,382 / 3: 310 / 4: 108 / 
		  이외 5~29는 10 이하의 결제 row를 가짐
	- orders에는 있지만 payments에는 없는 주문: 1건
		  해당 row는 주문 상태는 delivered이며, order_items 및 금액 합계가 존재하나,
		  order_payments row가 존재하지 않는 원천 데이터 불일치 케이스로 확인됨
		  (이후 DM 레이어에서 조인 정합성 플래그로 관리 예정)
	- payments에는 있지만 orders에는 없는 주문: 0건
```


- **컬럼 명세(1): 원본 컬럼 - 타입 표준화**

| 컬럼명                  | 타입             | NULL | 설명            | 비고                                                                             |
| :------------------- | -------------- | ---- | ------------- | ------------------------------------------------------------------------------ |
| **order_id**             | VARCHAR(50)    | N    | 주문 식별자        | 복합 PK 구성요소 / 조인 키                                                              |
| **payment_sequential**   | INT            | N    | 동일 주문 내 결제 순번 | 복합 PK 구성요소                                                                     |
| **payment_type**         | VARCHAR(20)    | Y    | 결제 수단         | credit_card / boleto / voucher / debit_card / not_defined<br>(LOWER / TRIM 적용) |
| **payment_installments** | INT            | Y    | 할부 개월 수       | 0 값이 존재 (플래그로 관리)                                                              |
| **payment_value**        | DECIMAL(10, 2) | Y    | 결제 금액         | 0 값이 존재 (플래그로 관리)                                                              |

- **컬럼 명세(2): 플래그 컬럼**

| 컬럼명                   | 타입      | NULL | 설명              | 생성 기준                        |
| :-------------------- | ------- | ---- | --------------- | ---------------------------- |
| **is_installments_zero**  | TINYINT | N    | 할부 개월 수가 0인 결제건 | payment_installments = 0이면 1 |
| **is_payment_value_zero** | TINYINT | N    | 결제 금액이 0인 결제    | payment_value = 0이면 1        |


#### stg.order_reviews

```
테이블명: olist_stg.stg_order_reviews

테이블 source: olist_raw.order_reviews

Primary Key: (review_id, order_id)

Indexes:
	idx_stg_order_reviews_order_id (order_id): 조인용
	idx_stg_order_reviews_creation_dt (review_creation_dt): 생성일 기준 분석/집계용

설계 목적:
	- 주문 식별자(order_id) 기준 조인 안전성 확보
	- 문자열 컬럼(review_comment_title / review_comment_message) 표준화 
	  (TRIM, REPLACE 적용 / 완전 공백은 NULL로 변환)
	- 시간 컬럼(review_creation_date / review_answer_timestamp) 
	  DATETIME 파싱 및 파생 컬럼 생성
	- 문자열 공백 여부 플래그 컬럼 생성 (is_title_blank / is_message_blank)

적재 규칙:
	- review_id와 order_id를 결합하여 복합 PK로 지정
	- review_score 컬럼의 범위가 1~5까지로 한정되어 있기 때문에 TINYINT로 타입 변경
	- review_id와 order_id는 식별자 성격을 가지기 때문에 NOT NULL을 지정
	- review_score는 테이블 특성 상 중요 컬럼이기 때문에 NOT NULL을 지정
	- 그 외 컬럼은 현재는 결측치가 없으나, 데이터 확장성을 고려해 NULL을 허용
	- review_comment_title과 review_comment_message 컬럼에 
	  TRIM과 REPLACE를 적용한 후, NULLIF를 통해 완전 공백 row를 NULL로 변경
	- 시간 관련 컬럼(review_creation_date / review_answer_timestamp)의 
	  컬럼 타입을 DATETIME으로 변경 후, 컬럼명을 dt로 변경
	- 시간 관련 컬럼(review_creation_dt / review_answer_dt)의 파생 컬럼(일자 컬럼) 생성
	- 표준화된 review_comment_title과 review_comment_message가 NULL인 값에 대해 
	  각각 플래그 컬럼 생성 (is_title_blank / is_message_blank)
	- orders 테이블과의 조인 정합성을 확인한 결과 orders의 주문 시간보다 
	  reviews의 리뷰 생성 시간이 빠른 경우가 74건 발견되었으나, 
	  이후 분석 시 필터링이 가능하도록 DM 레이어에서 플래그로 관리할 예정

특이 사항 (DQ 결과):
	- order_id 기준 리뷰 개수 분포
		  리뷰 개수가 1개인 order_id: 98,126건
		  리뷰 개수가 2개인 order_id: 543건
		  리뷰 개수가 3개인 order_id: 4건
	- 플래그 컬럼 row 수 확인
		  is_blank_title이 1인 row: 87,658건
		  is_blank_message가 1인 row: 58,256건
		  둘 모두 1인 row: 56,527
	- review_score 분포
		  5점: 57,328건
		  4점: 19,142건
		  3점: 8,179건
		  2점: 3,151건
		  1점: 11,424건
	- orders의 주문 시간보다 reviews의 리뷰 생성 시간이 빠른 경우
		  건수: 74건
		  차이가 가장 큰 row의 시간 차이: -161,056분
		  차이가 가장 작은 row의 시간 차이: -489분
```


- **컬럼 명세(1): 원본 컬럼 - 표준화**

| 컬럼명                    | 타입          | NULL | 설명         | 비고                                                    |
| :--------------------- | ----------- | ---- | ---------- | ----------------------------------------------------- |
| review_id              | VARCHAR(50) | N    | 리뷰 식별자     | 복합 PK 구성 요소                                           |
| order_id               | VARCHAR(50) | N    | 주문 식별자     | 복합 PK 구성 요소 / 조인 키                                    |
| review_score           | TINYINT     | N    | 리뷰 점수(1~5) | DQ 상 NULL, 이상치가 0건이고,<br>리뷰 점수 기반 이벤트이므로 NOT NULL을 보장 |
| review_comment_title   | TEXT        | Y    | 리뷰 제목      | 공백이 다수 존재 -> NULL로 표준화                                |
| review_comment_message | TEXT        | Y    | 리뷰 내용      | 공백이 다수 존재 -> NULL로 표준화                                |
| review_creation_dt     | DATETIME    | Y    | 리뷰 생성 시각   | review_creation_date에 대해 STR_TO_DATE 적용               |
| review_answer_dt       | DATETIME    | Y    | 리뷰 답변 시각   | review_answer_timestamp에 대해 STR_TO_DATE 적용            |


- **컬럼 명세(2): 파생 컬럼 - 시간 관련**

| 컬럼명                  | 타입   | NULL | 설명       | 생성 기준                    |
| :------------------- | ---- | ---- | -------- | ------------------------ |
| review_creation_date | DATE | Y    | 리뷰 생성 일자 | DATE(review_creation_dt) |
| review_answer_date   | DATE | Y    | 리뷰 답변 일자 | DATE(review_answer_dt)   |


- **컬럼 명세(3): 플래그 컬럼 - 문자열 결측치**

| 컬럼명              | 타입         | NULL | 설명              | 생성 기준                        |
| :--------------- | ---------- | ---- | --------------- | ---------------------------- |
| is_title_blank   | TINYINT(1) | N    | 리뷰 제목 <br>공백 여부 | title 표준화 결과가 <br>NULL이면 1   |
| is_message_blank | TINYINT(1) | N    | 리뷰 내용 <br>공백 여부 | message 표준화 결과가 <br>NULL이면 1 |


#### stg.geolocation

```
테이블명: olist_stg.stg_geolocation

테이블 source: olist_raw.geolocation

Primary Key: geolocation_zip_code_prefix

Indexes:
	idx_stg_geolocation_state (geolocation_state): 주 기준 분석/집계용
	idx_stg_geolocatino_city_state (geolocation_city_state): 시_주 단위 분석/집계용

설계 목적:
	- 우편번호(geolocation_zip_code_prefix) 기준 조인 안전성 확보
	- 문자열 컬럼(geolocation_city / geolocation_state) 표준화 
	  (TRIM, REPLACE 적용 / 타입 변환)
	- 지도/지역 분석을 위한 대표 좌표(lat/lng) 및 대표 도시/주(city/state) 산출
	- 품질 지표/플래그 컬럼 생성 (invalid 좌표 존재 여부 / 복수 state 매핑 여부)

적재 규칙:
	- geolocation_zip_code_prefix를 PK로 지정
	  (zip_code_prefix 하나 당 1 row로 적재)
	- geolocation_zip_code_prefix의 길이가 5로 고정되어 있기 때문에 CHAR(5)로 타입 변환
	  (조인 안전성을 위해 문자형으로 지정)
	- geolocation_state의 길이가 2로 고정되어 있기 때문에 CHAR(2)로 타입 변환
	- geolocation_zip_code_prefix는 식별자 성격을 가지기 때문에 NOT NULL을 지정
	- 각종 집계, 플래그 컬럼은 NOT NULL을 지정
	- 그 외 컬럼은 데이터 확장성을 고려해 NULL을 허용
	- zip_prefix 단위의 raw row 집계 컬럼 생성 (row_cnt)
	- 브라질 유효 좌표 범위(lat: -35~6 / lng: -75~-30)를 기준으로 좌표 유효성 컬럼 생성
	  (invalid_latlng_cnt)
	- 브라질 유효 좌표 범위를 기준으로 zip_code_prefix 당 유효 좌표 건수 집계 컬럼 생성
	  (mode_cnt)
	- zip_prefix 단위로 대표 좌표(lat/lng)를 산출 (브라질 유효 범위 내 최빈값으로 지정)
	- zip_prefix 단위로 대표 city/state를 산출 
	  (zip_prefix 그룹 내 (city, state) 최빈값으로 선정)
	- zip_prefix 단위로 state 분포 집계 컬럼 생성 (state_cnt)
	- zip_prefix 단위로 최빈값의 신뢰도를 파악할 수 있는 집계 컬럼 생성 (mode_ratio_pct)
	- city와 state를 결합한 geolocation_city_state 파생 컬럼 생성
	- 집계 결과를 기반으로 품질 관리용 플래그 컬럼 생성
		  - is_invalid_latlng_exists: zip_prefix 그룹 내 유효 범위를 벗어난 좌표가 
									  1건 이상 존재할 경우 1
		  - is_multi_state: 동일 zip_prefix가 2개 이상의 state에 매핑될 경우 1

특이 사항 (DQ 결과):
	- geolocation_zip_code_prefix 단위로 변환 후 테이블 row count: 19,015
	- geolocation_lat, geolocation_lng 공백 및 결측: 4건
	  (해당 결측은 is_invalid_latlng_exists와 mode_cnt가 모두 1인 row로 이상치)
	- 대표 좌표가 유효 범위 밖인 케이스: 0건
	- mode_ratio_pct가 낮은 zip_prefix:
		  - 83252, 18243, 95130, 78131은 0%
		  - mode_ratio_pct가 5%보다 낮은 건수: 19,015건 중 2,888건 (15.19%)
	- invalid_latlng_cnt가 많은 zip_prefix:
		  - 68275: 5
		  - 98780: 3
		  - 35179: 2
		  - 29654: 2
		  - 83252: 2
	- 조인 정합성(1): 고객 zip_prefix가 stg_geolocation에 매칭되지 않는 비율
		  - 총 99,441건 중 278건 (0.28%)
	- 조인 정합성(2): 판매자 zip_prefix 매칭 누락률
		  - 총 3,095건 중 7건 (0.23%)
```


- **컬럼 명세(1): 원본 컬럼 - 표준화**

| 컬럼명                         | 타입             | NULL | 설명           | 비고                                |
| :-------------------------- | -------------- | ---- | ------------ | --------------------------------- |
| geolocation_zip_code_prefix | CHAR(5)        | N    | 우편 번호 prefix | PK / 조인 키                         |
| geolocation_lat             | DECIMAL(10, 6) | Y    | 대표 위도        | 유효 좌표 범위 내에서 MODE(lat, lng) 기반 선정 |
| geolocation_lng             | DECIMAL(10, 6) | Y    | 대표 경도        | 유효 좌표 범위 내에서 MODE(lat, lng) 기반 선정 |
| geolocation_city            | VARCHAR(100)   | Y    | 대표 도시        | MODE(city, state) 기반 선정           |
| geolocation_state           | CHAR(2)        | Y    | 대표 주         | MODE(city, state) 기반 선정           |

- **컬럼 명세(2): 파생 컬럼**

| 컬럼명                    | 타입               | NULL | 설명                          | 생성 기준                                             |
| :--------------------- | ---------------- | ---- | --------------------------- | ------------------------------------------------- |
| geolocation_city_state | VARCHAR(200)     | Y    | 도시_주                        | CONCAT(geolocation_city, '__', geolocation_state) |
| row_cnt                | INT<br>UNSIGNED  | N    | 해당 zip_prefix의 raw row 수    | COUNT(*)                                          |
| mode_cnt               | INT<br>UNSIGNED  | N    | 대표 좌표(MODE)의 빈도             | zip_prefix 내 최빈 (lat, lng)의 COUNT                 |
| mode_ratio_pct         | DECIMAL(6, 2)    | N    | 대표 좌표 비중(%)                 | ROUND(mode_cnt / row_cnt * 100, 2)                |
| invalid_latlng_cnt     | INT<br>UNSIGNED  | N    | 브라질 범위 밖 좌표 건수              | SUM(lat/lng가 유효 범위 밖이면 1)                         |
| state_cnt              | TINYINT UNSIGNED | N    | zip_prefix 내 서로 다른 state 개수 | COUNT(DISTINCT geolocation_state)                 |

- **컬럼 명세(3): 플래그 컬럼**

| 컬럼명                      | 타입         | NULL | 설명                            | 생성 기준                                          |
| :----------------------- | ---------- | ---- | ----------------------------- | ---------------------------------------------- |
| is_invalid_latlng_exists | TINYINT(1) | N    | 좌표 이상치 존재 여부                  | 해당 zip_prefix 그룹 내 invalid_latlng_cnt > 0 이면 1 |
| is_multi_state           | TINYINT(1) | N    | 동일 zip_prefix의 복수 state 매핑 여부 | state_cnt > 1 이면 1                             |
