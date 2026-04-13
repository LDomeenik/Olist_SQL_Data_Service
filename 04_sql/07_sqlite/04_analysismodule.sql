/******************************************************************************************************************************************************/
/*
 * File: 04_analysismodule.sql
 * Description:
 *  - SQLite 기반 Analysis Module 레이어 통합 View 생성
 *  - vw_base_customer_monthly_purchase
 *  - vw_kpi_monthly_core
 *  - vw_kpi_monthly_cancellation
 *  - vw_cohort_monthly_core
 *  - 검증/DQ 확인 쿼리는 제외하고 View 정의만 포함
 */
/******************************************************************************************************************************************************/



/******************************************************************************************************************************************************/
/*
 * Section: vw_base_customer_monthly_purchase
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_base_customer_monthly_purchase;

CREATE VIEW vw_base_customer_monthly_purchase AS
WITH base_orders AS (
    SELECT  dc.customer_unique_id
            ,dd.year_month
            ,COUNT(DISTINCT vdo.order_id) AS order_cnt
      FROM  vw_delivered_orders AS vdo
      JOIN  dim_customer AS dc
        ON  dc.customer_id = vdo.customer_id
      JOIN  dim_date AS dd
        ON  dd.date_key = vdo.order_purchase_date_key
     GROUP
        BY  dc.customer_unique_id
            ,dd.year_month
),
base_items AS (
    SELECT  dc.customer_unique_id
            ,dd.year_month
            ,COUNT(*) AS item_cnt
            ,ROUND(SUM(vdoi.item_total_value), 2) AS gross_revenue
      FROM  vw_delivered_order_items AS vdoi
      JOIN  dim_customer AS dc
        ON  dc.customer_id = vdoi.customer_id
      JOIN  dim_date AS dd
        ON  dd.date_key = vdoi.order_purchase_date_key
     GROUP
        BY  dc.customer_unique_id
            ,dd.year_month
)
SELECT  o.customer_unique_id
        ,o.year_month
        ,cfpm.first_purchase_year_month AS cohort_year_month
        ,(
            (CAST(substr(o.year_month, 1, 4) AS INTEGER) - CAST(substr(cfpm.first_purchase_year_month, 1, 4) AS INTEGER)) * 12
            + (CAST(substr(o.year_month, 6, 2) AS INTEGER) - CAST(substr(cfpm.first_purchase_year_month, 6, 2) AS INTEGER))
         ) AS month_n
        ,o.order_cnt
        ,COALESCE(i.item_cnt, 0) AS item_cnt
        ,COALESCE(i.gross_revenue, 0.00) AS gross_revenue
        ,CASE WHEN o.order_cnt > 0 THEN 1 ELSE 0 END AS is_active
        ,CASE WHEN o.year_month = cfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_new_buyer
        ,CASE WHEN o.year_month > cfpm.first_purchase_year_month THEN 1 ELSE 0 END AS is_repeat_buyer
  FROM  base_orders AS o
  LEFT
  JOIN  base_items AS i
    ON  i.customer_unique_id = o.customer_unique_id
   AND  i.year_month = o.year_month
  JOIN  vw_customer_first_purchase_month AS cfpm
    ON  cfpm.customer_unique_id = o.customer_unique_id;



/******************************************************************************************************************************************************/
/*
 * Section: vw_kpi_monthly_core
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_kpi_monthly_core;

CREATE VIEW vw_kpi_monthly_core AS
WITH months AS (
    SELECT  DISTINCT dd.year_month
      FROM  dim_date AS dd
),
monthly_base AS (
    SELECT  vbcmp.year_month
            ,SUM(vbcmp.gross_revenue) AS gross_revenue
            ,SUM(vbcmp.order_cnt) AS order_cnt
            ,COUNT(DISTINCT vbcmp.customer_unique_id) AS active_buyers
            ,COUNT(DISTINCT CASE WHEN vbcmp.is_repeat_buyer = 1 THEN vbcmp.customer_unique_id END) AS repeat_buyers
      FROM  vw_base_customer_monthly_purchase AS vbcmp
     GROUP
        BY  vbcmp.year_month
)
SELECT  m.year_month
        ,COALESCE(b.gross_revenue, 0) AS gross_revenue
        ,COALESCE(b.order_cnt, 0) AS order_cnt
        ,COALESCE(b.active_buyers, 0) AS active_buyers
        ,ROUND(CASE WHEN COALESCE(b.order_cnt, 0) = 0 THEN NULL
                    ELSE b.gross_revenue * 1.0 / b.order_cnt
                    END, 2) AS aov
        ,COALESCE(b.repeat_buyers, 0) AS repeat_buyers
        ,ROUND(CASE WHEN COALESCE(b.active_buyers, 0) = 0 THEN NULL
                    ELSE b.repeat_buyers * 1.0 / b.active_buyers
                    END, 6) AS repeat_buyer_rate
  FROM  months AS m
  LEFT
  JOIN  monthly_base AS b
    ON  b.year_month = m.year_month
 ORDER
    BY  m.year_month;



/******************************************************************************************************************************************************/
/*
 * Section: vw_kpi_monthly_cancellation
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_kpi_monthly_cancellation;

CREATE VIEW vw_kpi_monthly_cancellation AS
WITH months AS (
    SELECT  DISTINCT dd.year_month
      FROM  dim_date AS dd
),
cnt AS (
    SELECT  d.year_month
            ,COUNT(DISTINCT o.order_id) AS total_orders
            ,COUNT(DISTINCT CASE WHEN o.order_status = 'canceled' THEN o.order_id END) AS canceled_orders
            ,COUNT(DISTINCT CASE WHEN o.order_status = 'unavailable' THEN o.order_id END) AS unavailable_orders
            ,COUNT(DISTINCT CASE WHEN o.order_status IN ('canceled', 'unavailable') THEN o.order_id END) AS failed_orders
      FROM  fact_orders AS o
      JOIN  dim_date AS d
        ON  d.date_key = o.order_purchase_date_key
     GROUP
        BY  d.year_month
)
SELECT  m.year_month
        ,COALESCE(c.total_orders, 0) AS total_orders
        ,COALESCE(c.canceled_orders, 0) AS canceled_orders
        ,COALESCE(c.unavailable_orders, 0) AS unavailable_orders
        ,COALESCE(c.failed_orders, 0) AS failed_orders
        ,ROUND(CASE WHEN COALESCE(c.total_orders, 0) = 0 THEN NULL
                    ELSE c.canceled_orders * 1.0 / c.total_orders
                    END, 6) AS cancel_rate
        ,ROUND(CASE WHEN COALESCE(c.total_orders, 0) = 0 THEN NULL
                    ELSE c.unavailable_orders * 1.0 / c.total_orders
                    END, 6) AS unavailable_rate
        ,ROUND(CASE WHEN COALESCE(c.total_orders, 0) = 0 THEN NULL
                    ELSE c.failed_orders * 1.0 / c.total_orders
                    END, 6) AS failed_rate
  FROM  months AS m
  LEFT
  JOIN  cnt AS c
    ON  c.year_month = m.year_month
 ORDER
    BY  m.year_month;



/******************************************************************************************************************************************************/
/*
 * Section: vw_cohort_monthly_core
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_cohort_monthly_core;

CREATE VIEW vw_cohort_monthly_core AS
WITH months AS (
    SELECT  DISTINCT dd.year_month
            ,date(dd.year_month || '-01') AS ym_dt
      FROM  dim_date AS dd
),
cohorts AS (
    SELECT  DISTINCT b.cohort_year_month
            ,date(b.cohort_year_month || '-01') AS cohort_dt
      FROM  vw_base_customer_monthly_purchase AS b
),
matrix AS (
    SELECT  c.cohort_year_month
            ,m.year_month
            ,(
                (CAST(substr(m.year_month, 1, 4) AS INTEGER) - CAST(substr(c.cohort_year_month, 1, 4) AS INTEGER)) * 12
                + (CAST(substr(m.year_month, 6, 2) AS INTEGER) - CAST(substr(c.cohort_year_month, 6, 2) AS INTEGER))
             ) AS month_n
      FROM  cohorts AS c
      JOIN  months AS m
        ON  m.ym_dt >= c.cohort_dt
),
base_agg AS (
    SELECT  b.cohort_year_month
            ,b.year_month
            ,b.month_n
            ,COUNT(DISTINCT b.customer_unique_id) AS cohort_active_buyers
            ,SUM(b.order_cnt) AS cohort_order_cnt
            ,ROUND(SUM(b.gross_revenue), 2) AS cohort_gross_revenue
      FROM  vw_base_customer_monthly_purchase AS b
     GROUP
        BY  b.cohort_year_month
            ,b.year_month
            ,b.month_n
),
cohort_size AS (
    SELECT  b.cohort_year_month
            ,COUNT(DISTINCT b.customer_unique_id) AS cohort_size
      FROM  vw_base_customer_monthly_purchase AS b
     WHERE  b.month_n = 0
     GROUP
        BY  b.cohort_year_month
)
SELECT  mx.cohort_year_month
        ,mx.year_month
        ,mx.month_n
        ,cs.cohort_size
        ,COALESCE(ba.cohort_active_buyers, 0) AS cohort_active_buyers
        ,COALESCE(ba.cohort_order_cnt, 0) AS cohort_order_cnt
        ,COALESCE(ba.cohort_gross_revenue, 0) AS cohort_gross_revenue
        ,ROUND(CASE WHEN cs.cohort_size = 0 THEN NULL
                    ELSE COALESCE(ba.cohort_active_buyers, 0) * 1.0 / cs.cohort_size
                    END, 6) AS cohort_retention_rate
        ,ROUND(CASE WHEN COALESCE(ba.cohort_order_cnt, 0) = 0 THEN NULL
                    ELSE COALESCE(ba.cohort_gross_revenue, 0) * 1.0 / ba.cohort_order_cnt
                    END, 2) AS cohort_aov
        ,ROUND(SUM(COALESCE(ba.cohort_gross_revenue, 0)) OVER (
            PARTITION BY mx.cohort_year_month
            ORDER BY mx.month_n
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) AS cum_gross_revenue
        ,SUM(COALESCE(ba.cohort_order_cnt, 0)) OVER (
            PARTITION BY mx.cohort_year_month
            ORDER BY mx.month_n
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_order_cnt
  FROM  matrix AS mx
  JOIN  cohort_size AS cs
    ON  cs.cohort_year_month = mx.cohort_year_month
  LEFT
  JOIN  base_agg AS ba
    ON  ba.cohort_year_month = mx.cohort_year_month
   AND  ba.year_month = mx.year_month
   AND  ba.month_n = mx.month_n
 ORDER
    BY  mx.cohort_year_month
        ,mx.month_n;