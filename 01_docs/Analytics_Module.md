
# Analysis Module 설계서

```
Analysis Module(AM)은 Data Mart 레이어를 단일 데이터 소스로 활용하여,
커머스 비즈니스의 핵심 KPI를 시계열 및 코호트 관점에서
반복적으로 분석·모니터링할 수 있도록 설계된 분석 전용 계층입니다.

AM의 주요 목적은 Data Mart에서 정의된 Fact / Dimension 구조와 KPI 계산 기준을 기반으로
일회성 쿼리가 아닌 재사용 가능한 표준 분석 데이터셋과 쿼리 패턴을 제공하는 것입니다.

본 프로젝트에서 Analysis Module은
KPI 시계열 분석과 코호트 분석을 중심으로 구성되며,
이후 대시보드 및 추가 분석의 기준 레이어로 활용됩니다.
```

---

## 1. Analysis Module의 역할과 범위

```
Analysis Module은 Data Mart 위에 위치한 분석 전용 계층으로,
KPI 계산 기준을 고정하고 반복적으로 활용 가능한 분석 구조를 제공합니다.

본 모듈은 다음과 같은 역할을 수행합니다.

- KPI 시계열 분석을 위한 표준 집계 데이터셋 제공
- 고객 단위 코호트 및 재구매 분석을 위한 기준 데이터셋 제공
- 분석 및 대시보드에서 반복 사용 가능한 쿼리 구조 정의
```
 
- **AM의 역할**
	- KPI 계산 로직의 재사용성 확보
	- 분석 기준의 일관성 유지
	- 대시보드 구현 단순화

- **AM의 범위**
	- KPI 시계열 분석
	- 코호트 분석 및 재구매율 분석
	- 주문 취소율 기반의 거래 안정성 보조 분석


---

## 2. Analysis Module 공통 기준

```
Analysis Module에서 산출되는 모든 KPI와 분석 결과는
아래 공통 기준을 전제로 계산됩니다.
(예외적으로 거래 안정성 KPI는 취소/미수 주문을 기반으로 한 결과이기 때문에 전체 주문을 기준으로 합니다.)

- 배송 완료된 주문만 포함
- 월 단위 분석 기준
- 고객 단위 KPI는 customer_unique_id 기준

이 기준은 분석자 및 대시보드 간 KPI 해석의 불일치를 방지하기 위해
고정된 규칙으로 적용됩니다.
```

- **배송 완료된 주문만 포함**
	- 모든 매출 및 구매자 관련 KPI는 delivered 상태의 주문을 기준으로 산출
	- 기준 데이터셋:
		- olist_dm.vw_delivered_orders
		- olist_dm.vw_delivered_order_items

- **월 단위 분석**
	- dim_date.year_month 기준으로 시계열 집계
	- 기준 시점:
		- 주문 구매 시점 (order_purchase_date_key)

- **고객 단위 KPI는 customer_unique_id 기준**
	- Active Buyers, 재구매율, 코호트 분석은 고객 고유 식별자 기준으로 계산
	- 주문 단위 조인은 customer_id / 고객 단위 집계는 customer_unique_id를 사용


---

## 3. Analysis Module 데이터 구성

```
Analysis Module은 Data Mart의 Fact / Dimension 및 View를 기반으로,
분석 목적에 맞는 표준 데이터셋을 제공합니다.

Analysis Module에서 사용될 차원 데이터는 아래와 같습니다.

- dim_date (날짜 차원)
- dim_geolocation (지역 차원)
- dim_customer (고객 정보 차원)
- dim_product (상품 정보 차원)
- dim_seller (판매자 정보 차원)

Analysis Module에서 사용될 사실 데이터는 아래와 같습니다.

- fact_orders (주문 단위 데이터)
- fact_order_items (주문상품 단위 데이터)

Analysis Module에서 사용될 기준 데이터는 아래와 같습니다.

- vw_delivered_orders (배송인 완료된 주문 데이터)
- vw_delivered_order_items (배송이 완료된 주문상품 데이터)
- vw_customer_first_purchase_month (고객별 첫 구매 연-월 데이터)
```


---

## 4. Analysis Module 상세


### Analysis Module View 목록


| View 이름                               | 설명                                                  |
| :------------------------------------ | --------------------------------------------------- |
| **vw_base_customer_monthly_purchase** | base 뷰<br>구매 발생 월 기준 월별 주문수, 구매여부, 매출 등의 base 정보 제공 |
| **vw_kpi_monthly_core**               | 월별 핵심 KPI 뷰<br>총 매출, 주문 수, AOV 등 핵심 시계열 KPI 제공      |
| **vw_kpi_monthly_cancellation**       | 월별 취소 KPI 뷰<br>전체 주문 기준 취소율 제공                      |
| **vw_cohort_monthly_core**            | 코호트별 월차 핵심 KPI 뷰<br>코호트 유지 구조 및 변화 분석 지원            |


### Analysis Module View 명세


#### am.vw_base_customer_monthly_purchase

```
View 이름: olist_am.vw_base_customer_monthly_purchase

Source 테이블:
	- olist_dm.vw_delivered_orders
	- olist_dm.vw_delivered_order_items
	- olist_dm.dim_date
	- olist_dm.dim_customer
	- olist_dm.vw_customer_first_purchase_month

그레인(1행 정의): 1 row = 1 customer unique id in 1 year_month

Primary Key(논리): (customer_unique_id, year_month)

필터 규칙:
	- 배송 완료 주문만 포함 (vw_delivered_orders 기준)

설계 목적:
	- 고객 기준 월별 구매 성과를 표준화하여 제공
	- 월별 재구매율/리텐션/코호트 분석의 공통 베이스 집계 뷰로 재사용
	- KPI/코호트 뷰에서 주문/아이템 조인 및 집계 반복을 제거

생성 규칙:
	- 해당 View는 Sparse view로 구매 이력이 있는 row만 생성
	- 따라서 구매 여부를 확인하는 is_active는 항상 1이며, 
	  향후 dense 뷰로 확장 시 동일 컬럼을 0/1로 사용하기 위해 포함
	- delivered 기준은 vw_delivered_orders에서 고정
	- 월 라벨(year_month)은 dim_date를 통해 제공
	- item_cnt/gross_revenue는 delivered_order_items의 row/금액을 고객x월 단위로 집계
	- cohort_year_month는 vw_customer_first_purchase_month 기준
	- month_n은 cohort_year_month 대비 year_month의 월차로 계산
```


- **컬럼 명세**

| 컬럼명                    | 타입            | NULL | Key    | 설명                      | 생성 기준/로직                                                                           |
| :--------------------- | ------------- | ---- | ------ | ----------------------- | ---------------------------------------------------------------------------------- |
| **customer_unique_id** | VARCHAR(50)   | N    | PK(논리) | 고객(고유) 식별자              | `dim_customer 그대로 반영`                                                              |
| **year_month**         | CHAR(7)       | N    | PK(논리) | 연-월 라벨                  | `vw_delivered_orders 그대로 반영`                                                       |
| **cohort_year_month**  | CHAR(7)       | N    | FK     | 첫 구매 월                  | `vw_customer_first_purchase_month의 first_purchase_year_month`                      |
| **month_n**            | INT           | N    |        | 코호트 기준 월차(0부터)          | `TIMESTAMPDIFF(MONTH, CONCAT(cohort_year_month,'-01'), CONCAT(year_month, '-01'))` |
| **order_cnt**          | INT           | N    |        | 해당 월 배송 완료 주문 수 (중복 제거) | `COUNT(DISTINCT order_id)`                                                         |
| **item_cnt**           | INT           | N    |        | 해당 월 배송 완료 주문 상품 수      | `COUNT(*)`                                                                         |
| **gross_revenue**      | DECIMAL(18,2) | N    |        | 해당 월 매출(상품 + 배송비)       | `SUM(item_total_value)`                                                            |
| **is_active**          | TINYINT(1)    | N    |        | 해당 월 구매 여부              | `order_cnt가 0보다 크면 1<br>(해당 view는 sparse view로 모두 1)`                              |
| **is_new_buyer**       | TINYINT(1)    | N    |        | 새 구매 고객 여부              | `cohort_month와 같으면 1`                                                              |
| **is_repeat_buyer**    | TINYINT(1)    | N    |        | 반복 구매 고객 여부             | `cohort_month보다 이후이면 1`                                                            |


#### am.vw_kpi_monthly_core

```
View 이름: olist_am.vw_kpi_monthly_core

Source 테이블:
	- olist_am.vw_base_customer_monthly_purchase
	- olist_dm.dim_date

그레인(1행 정의): 1 row = 1 year_month

Primary Key (논리): year_month

설계 목적:
	- KPI 정의서에 명시된 월별 핵심 KPI를 표준화하여 제공
	- BI/대시보드에서 직접 사용하는 월 단위 Core KPI 집계 View
	- KPI 계산 기준을 고정하여 지표 일관성 확보

포함 KPI:
	- Gross Revenue (총 매출)
	- Order Count (주문 수)
	- Active Buyers (구매자 수)
	- AOV (Average Order Value)
	- Repeat Buyers (월 기준 반복 구매 고객 수)
	- Repeat Buyer Rate (월 기준 반복 구매 고객 비율)

계산 기준:
	- 매출/주문/구매자: 배송 완료 주문 기준
	- 취소 주문은 포함하지 않음 (별도 Cancellation View에서 관리)
	- repeat buyer는 해당 월 구매 고객 중
	  cohort_month < year_month인 고객으로 정의

참고 사항:
	- 해당 View는 Sparse View가 아닙니다. (월 단위 full 집계)
	- 월별 재구매율은 월 기준 repeat buyer 비중이며,
	  전체 기간 기준 재구매율과는 다릅니다.
	- 전체 기간 기준 재구매율은 BI 툴에서 직접 계산으로 다룰 예정입니다.
```


- **컬럼 명세**

| 컬럼명                   | 타입(논리)        | NULL | Key    | 설명              | 계산 기준                                |
| --------------------- | ------------- | ---- | ------ | --------------- | ------------------------------------ |
| **year_month**        | CHAR(7)       | N    | PK(논리) | 연-월 라벨          | `base view 그대로`                      |
| **gross_revenue**     | DECIMAL(18,2) | N    |        | 해당 월 총 매출       | `SUM(gross_revenue)`                 |
| **order_cnt**         | INT           | N    |        | 해당 월 배송 완료 주문 수 | `SUM(order_cnt)`                     |
| **active_buyers**     | INT           | N    |        | 해당 월 구매 고객 수    | `COUNT(DISTINCT customer_unique_id)` |
| **aov**               | DECIMAL(18,2) | N    |        | 주문 1건당 평균 매출    | `gross_revenue / order_cnt`          |
| **repeat_buyers**     | INT           | N    |        | 해당 월 반복 구매 고객 수 | `is_repeat_buyer = 1 고객 수`           |
| **repeat_buyer_rate** | DECIMAL(6,4)  | N    |        | 해당 월 재구매 고객 비율  | `repeat_buyers / active_buyers`      |


#### am.vw_kpi_monthly_cancellation

```
View 이름: olist_am.vw_kpi_monthly_cancellation

Source 테이블:
	- olist_dm.dim_date
	- olist_dm.fact_orders

그레인(1행 정의): 1 row = 1 year_month

Primary Key (논리): year_month

필터 규칙:
	- 전체 주문(주문 생성 기준, total_orders)을 포함 (배송 완료 필터 적용하지 않음)
	- canceled_orders는 order_status = 'canceled' 기준
	- unavailable_orders는 order_status = 'unavailable' 기준
	- failed_orders는 order_status IN ('canceled', 'unavailable') 기준

생성 규칙:
	- dim_date의 year_month를 기준으로 월을 생성하여 Full View를 생성
	- 주문이 없는 월은 orders 컬럼은 0으로, 비율 컬럼은 NULL로 계산
	- cancellation_rate/unavailable_rate/failed_rate 
	  = 각 상태의 주문수/전체 주문수로 계산 (0~1)
```


- **컬럼 명세**

| 컬럼명                |            타입 | NULL |  Key   | 설명                     | 생성 기준/로직                                                                                 |
| ------------------ | ------------: | :--: | :----: | ---------------------- | ---------------------------------------------------------------------------------------- |
| year_month         |       CHAR(7) |  N   | PK(논리) | 연-월 라벨                 | `dim_date.year_month`                                                                    |
| total_orders       |           INT |  N   |        | 해당 월 전체 주문 수(생성 기준)    | `COUNT(DISTINCT order_id)`                                                               |
| canceled_orders    |           INT |  N   |        | 해당 월 취소 주문 수           | `COUNT(DISTINCT CASE WHEN order_status='canceled' THEN order_id END)`                    |
| unavailable_orders |           INT |  N   |        | 해당 월 unavailable 주문 수  | `COUNT(DISTINCT CASE WHEN order_status='unavailable' THEN order_id END)`                 |
| failed_orders      |           INT |  N   |        | 취소+unavailable(주문 미완료) | `COUNT(DISTINCT CASE WHEN order_status IN ('canceled','unavailable') THEN order_id END)` |
| cancel_rate        | DECIMAL(10,6) |  Y   |        | 취소율(0~1)               | `canceled_orders / NULLIF(total_orders,0)`                                               |
| unavailable_rate   | DECIMAL(10,6) |  Y   |        | unavailable 비율(0~1)    | `unavailable_orders / NULLIF(total_orders,0)`                                            |
| failed_rate        | DECIMAL(10,6) |  Y   |        | 확장 취소율(0~1)            | `failed_orders / NULLIF(total_orders,0)`                                                 |


#### am.vw_cohort_monthly_core

```
View 이름: olist_am.vw_cohort_monthly_core

Source 테이블:
	- olist_dm.dim_date
	- olist_am.vw_base_customer_monthly_purchase

그레인(1행 정의): 1 row = 1 cohort_year_month x month_n

Primary Key (논리): (cohort_year_month, month_n)

필터 규칙:
	- 코호트 기준은 배송 완료 주문으로 정의된 첫 구매월 
	  (olist_am.vw_base_customer_monthly_purchase 기준)
	- Full matrix 생성을 위해, 코호트별로 month_n = 0 ~ max_month_n까지 생성
	- 코호트 생성 이후 월차만 포함(음수 month_n은 제외)

설계 목적:
	- 코호트별 KPI를 월차 기준으로 제공
	- 리텐션 히트맵/커브, 코호트 매출 누적, 코호트별 주문/객단가 추적을 용이하게 함
	- Full martix로 구매가 없는 달도 0으로 명시

생성 규칙:
	- cohort_year_month 목록은 고객 첫 구매월 기준으로 생성
	- months는 dim_date의 year_month 기준
	- month_n = TIMESTAMPDIFF(MONTH, cohort_ym, year_month)로 계산
	- month_n >= 0인 조합만 허용
	- 구매가 없는 달은 0 또는 NULL로 채움
```


- **컬럼 명세**

| 컬럼명                       | 타입            | NULL | Key    | 설명                | 생성 기준/로직                                                               |
| ------------------------- | ------------- | ---- | ------ | ----------------- | ---------------------------------------------------------------------- |
| **cohort_year_month**     | CHAR(7)       | N    | PK(논리) | 코호트(첫 구매 월)       | base의 cohort_year_month 기준                                             |
| **year_month**            | CHAR(7)       | N    |        | 관측 월              | dim_date.year_month                                                    |
| **month_n**               | INT           | N    | PK(논리) | 코호트 기준 월차(0부터)    | TIMESTAMPDIFF(MONTH, cohort_ym, ym)                                    |
| **cohort_size**           | INT           | N    |        | 코호트 최초 고객 수       | COUNT(DISTINCT customer_unique_id) where month_n=0 (코호트별 상수로 전 월차에 붙임) |
| **cohort_active_buyers**  | INT           | N    |        | 해당 월 구매한 코호트 고객 수 | base join 후 COUNT(DISTINCT customer_unique_id) (없으면 0)                 |
| **cohort_order_cnt**      | INT           | N    |        | 해당 월 주문 수         | base의 order_cnt 합 (없으면 0)                                              |
| **cohort_gross_revenue**  | DECIMAL(18,2) | N    |        | 해당 월 매출           | base의 gross_revenue 합 (없으면 0)                                          |
| **cohort_retention_rate** | DECIMAL(10,6) | Y    |        | 리텐션(0~1)          | cohort_active_buyers / cohort_size (cohort_size=0이면 NULL)              |
| **cohort_aov**            | DECIMAL(18,2) | Y    |        | 코호트-월 객단가         | 	cohort_gross_revenue / cohort_order_cnt (cohort_order_cnt=0이면 NULL)   |
| **cum_gross_revenue**     | DECIMAL(18,2) | N    |        | 코호트 누적 매출         | 윈도우 SUM over (cohort_year_month order by month_n)                      |
| **cum_order_cnt**         | INT           | N    |        | 코호트 누적 주문 수       | 윈도우 SUM over (cohort_year_month order by month_n)                      |
