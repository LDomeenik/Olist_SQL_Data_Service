/******************************************************************************************************************************************************/
/*
 * File: 03_datamart.sql
 * Description:
 *  - SQLite 기반 Data Mart 레이어 통합 ETL
 *  - dim_date, dim_geolocation, dim_customer, dim_product, dim_seller,
 *    fact_order_items, fact_orders 생성
 *  - vw_delivered_orders, vw_delivered_order_items, vw_customer_first_purchase_month 생성
 *  - 검증/DQ 확인 쿼리는 제외하고 ETL 로직만 포함
 */
/******************************************************************************************************************************************************/



/******************************************************************************************************************************************************/
/*
 * Section: dim_date
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key        INTEGER  NOT NULL,
    date            TEXT     NOT NULL,
    year            INTEGER  NOT NULL,
    quarter         INTEGER  NOT NULL,
    month           INTEGER  NOT NULL,
    day             INTEGER  NOT NULL,
    year_month      TEXT     NOT NULL,
    year_quarter    TEXT     NOT NULL,
    week_of_year    INTEGER  NOT NULL,
    day_of_week     INTEGER  NOT NULL,
    day_name        TEXT     NOT NULL,
    is_weekend      INTEGER  NOT NULL,
    is_month_start  INTEGER  NOT NULL,
    is_month_end    INTEGER  NOT NULL,

    PRIMARY KEY (date_key)
);

INSERT INTO dim_date (
    date_key,
    date,
    year,
    quarter,
    month,
    day,
    year_month,
    year_quarter,
    week_of_year,
    day_of_week,
    day_name,
    is_weekend,
    is_month_start,
    is_month_end
)
WITH bounds AS (
    SELECT  date(MIN(d), '-1 month') AS start_date
            ,date(MAX(d), '+1 month') AS end_date
      FROM  (
            SELECT  date(order_purchase_dt) AS d
              FROM  stg_orders
             WHERE  order_purchase_dt IS NOT NULL
            UNION ALL
            SELECT  date(order_approved_dt) AS d
              FROM  stg_orders
             WHERE  order_approved_dt IS NOT NULL
            UNION ALL
            SELECT  date(order_delivered_carrier_dt) AS d
              FROM  stg_orders
             WHERE  order_delivered_carrier_dt IS NOT NULL
            UNION ALL
            SELECT  date(order_delivered_customer_dt) AS d
              FROM  stg_orders
             WHERE  order_delivered_customer_dt IS NOT NULL
            UNION ALL
            SELECT  date(order_estimated_delivery_dt) AS d
              FROM  stg_orders
             WHERE  order_estimated_delivery_dt IS NOT NULL
            ) t
),
RECURSIVE cal(d) AS (
    SELECT  start_date
      FROM  bounds
    UNION ALL
    SELECT  date(d, '+1 day')
      FROM  cal
     WHERE  d < (SELECT end_date FROM bounds)
)
SELECT  CAST(strftime('%Y%m%d', d) AS INTEGER) AS date_key
        ,d AS date
        ,CAST(strftime('%Y', d) AS INTEGER) AS year
        ,((CAST(strftime('%m', d) AS INTEGER) - 1) / 3) + 1 AS quarter
        ,CAST(strftime('%m', d) AS INTEGER) AS month
        ,CAST(strftime('%d', d) AS INTEGER) AS day
        ,strftime('%Y-%m', d) AS year_month
        ,strftime('%Y', d) || '-Q' || (((CAST(strftime('%m', d) AS INTEGER) - 1) / 3) + 1) AS year_quarter
        ,CAST(strftime('%W', d) AS INTEGER) AS week_of_year
        ,CASE WHEN strftime('%w', d) = '0' THEN 7
              ELSE CAST(strftime('%w', d) AS INTEGER)
              END AS day_of_week
        ,CASE strftime('%w', d)
              WHEN '0' THEN 'Sunday'
              WHEN '1' THEN 'Monday'
              WHEN '2' THEN 'Tuesday'
              WHEN '3' THEN 'Wednesday'
              WHEN '4' THEN 'Thursday'
              WHEN '5' THEN 'Friday'
              WHEN '6' THEN 'Saturday'
              END AS day_name
        ,CASE WHEN strftime('%w', d) IN ('0', '6') THEN 1 ELSE 0 END AS is_weekend
        ,CASE WHEN strftime('%d', d) = '01' THEN 1 ELSE 0 END AS is_month_start
        ,CASE WHEN d = date(d, 'start of month', '+1 month', '-1 day') THEN 1 ELSE 0 END AS is_month_end
  FROM  cal;

CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
    ON dim_date (year_month);



/******************************************************************************************************************************************************/
/*
 * Section: dim_geolocation
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS dim_geolocation;

CREATE TABLE dim_geolocation (
    geolocation_zip_code_prefix  TEXT     NOT NULL,
    geolocation_lat              REAL     NULL,
    geolocation_lng              REAL     NULL,
    geolocation_city             TEXT     NULL,
    geolocation_state            TEXT     NULL,
    geolocation_city_state       TEXT     NULL,
    row_cnt                      INTEGER  NOT NULL,
    mode_cnt                     INTEGER  NOT NULL,
    mode_ratio_pct               REAL     NOT NULL,
    invalid_latlng_cnt           INTEGER  NOT NULL,
    city_cnt                     INTEGER  NOT NULL,
    state_cnt                    INTEGER  NOT NULL,
    is_invalid_latlng_exists     INTEGER  NOT NULL,
    is_multi_city                INTEGER  NOT NULL,
    is_multi_state               INTEGER  NOT NULL,

    PRIMARY KEY (geolocation_zip_code_prefix)
);

INSERT INTO dim_geolocation (
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
SELECT  geolocation_zip_code_prefix
        ,geolocation_lat
        ,geolocation_lng
        ,geolocation_city
        ,geolocation_state
        ,geolocation_city_state
        ,row_cnt
        ,mode_cnt
        ,mode_ratio_pct
        ,invalid_latlng_cnt
        ,city_cnt
        ,state_cnt
        ,is_invalid_latlng_exists
        ,is_multi_city
        ,is_multi_state
  FROM  stg_geolocation;

CREATE INDEX IF NOT EXISTS idx_dim_geolocation_city_state
    ON dim_geolocation (geolocation_city_state);

CREATE INDEX IF NOT EXISTS idx_dim_geolocation_state
    ON dim_geolocation (geolocation_state);



/******************************************************************************************************************************************************/
/*
 * Section: dim_customer
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id               TEXT     NOT NULL,
    customer_unique_id        TEXT     NOT NULL,
    customer_zip_code_prefix  TEXT     NULL,
    customer_city             TEXT     NULL,
    customer_state            TEXT     NULL,
    customer_city_state       TEXT     NULL,

    PRIMARY KEY (customer_id)
);

INSERT INTO dim_customer (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    customer_city_state
)
SELECT  customer_id
        ,customer_unique_id
        ,customer_zip_code_prefix
        ,customer_city
        ,customer_state
        ,customer_city_state
  FROM  stg_customers;

CREATE INDEX IF NOT EXISTS idx_dim_customer_unique_id
    ON dim_customer (customer_unique_id);

CREATE INDEX IF NOT EXISTS idx_dim_customer_zip_prefix
    ON dim_customer (customer_zip_code_prefix);

CREATE INDEX IF NOT EXISTS idx_dim_customer_state
    ON dim_customer (customer_state);

CREATE INDEX IF NOT EXISTS idx_dim_customer_city_state
    ON dim_customer (customer_city_state);



/******************************************************************************************************************************************************/
/*
 * Section: dim_product
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product (
    product_id                  TEXT     NOT NULL,
    product_category_name       TEXT     NULL,
    product_category_name_en    TEXT     NULL,
    product_name_length         INTEGER  NULL,
    product_description_length  INTEGER  NULL,
    product_photos_qty          INTEGER  NULL,
    product_weight_g            INTEGER  NULL,
    product_length_cm           INTEGER  NULL,
    product_height_cm           INTEGER  NULL,
    product_width_cm            INTEGER  NULL,
    product_volume_cm3          INTEGER  NULL,
    is_category_blank           INTEGER  NOT NULL,
    is_category_en_unmapped     INTEGER  NOT NULL,
    is_weight_zero              INTEGER  NOT NULL,

    PRIMARY KEY (product_id)
);

INSERT INTO dim_product (
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
SELECT  product_id
        ,product_category_name
        ,product_category_name_en
        ,product_name_length
        ,product_description_length
        ,product_photos_qty
        ,product_weight_g
        ,product_length_cm
        ,product_height_cm
        ,product_width_cm
        ,product_volume_cm3
        ,is_category_blank
        ,is_category_en_unmapped
        ,is_weight_zero
  FROM  stg_products;

CREATE INDEX IF NOT EXISTS idx_dim_product_category_name
    ON dim_product (product_category_name);

CREATE INDEX IF NOT EXISTS idx_dim_product_category_name_en
    ON dim_product (product_category_name_en);



/******************************************************************************************************************************************************/
/*
 * Section: dim_seller
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS dim_seller;

CREATE TABLE dim_seller (
    seller_id                TEXT     NOT NULL,
    seller_zip_code_prefix   TEXT     NULL,
    seller_city              TEXT     NULL,
    seller_state             TEXT     NULL,
    seller_city_state        TEXT     NULL,

    PRIMARY KEY (seller_id)
);

INSERT INTO dim_seller (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    seller_city_state
)
SELECT  seller_id
        ,seller_zip_code_prefix
        ,seller_city
        ,seller_state
        ,seller_city_state
  FROM  stg_sellers;

CREATE INDEX IF NOT EXISTS idx_dim_seller_zip_prefix
    ON dim_seller (seller_zip_code_prefix);

CREATE INDEX IF NOT EXISTS idx_dim_seller_state
    ON dim_seller (seller_state);

CREATE INDEX IF NOT EXISTS idx_dim_seller_city_state
    ON dim_seller (seller_city_state);



/******************************************************************************************************************************************************/
/*
 * Section: fact_order_items
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS fact_order_items;

CREATE TABLE fact_order_items (
    order_id                  TEXT     NOT NULL,
    order_item_id             TEXT     NOT NULL,
    order_item_seq            INTEGER  NULL,
    customer_id               TEXT     NOT NULL,
    product_id                TEXT     NOT NULL,
    seller_id                 TEXT     NOT NULL,
    order_purchase_date_key   INTEGER  NOT NULL,
    customer_zip_code_prefix  TEXT     NULL,
    seller_zip_code_prefix    TEXT     NULL,
    price                     REAL     NULL,
    freight_value             REAL     NULL,
    item_total_value          REAL     NULL,

    PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO fact_order_items (
    order_id,
    order_item_id,
    order_item_seq,
    customer_id,
    product_id,
    seller_id,
    order_purchase_date_key,
    customer_zip_code_prefix,
    seller_zip_code_prefix,
    price,
    freight_value,
    item_total_value
)
SELECT  soi.order_id
        ,soi.order_item_id
        ,soi.order_item_seq
        ,o.customer_id
        ,soi.product_id
        ,soi.seller_id
        ,CAST(strftime('%Y%m%d', o.order_purchase_date) AS INTEGER) AS order_purchase_date_key
        ,c.customer_zip_code_prefix
        ,s.seller_zip_code_prefix
        ,soi.price
        ,soi.freight_value
        ,soi.item_total_value
  FROM  stg_order_items AS soi
 INNER
  JOIN  stg_orders AS o
    ON  o.order_id = soi.order_id
  LEFT
  JOIN  stg_customers AS c
    ON  c.customer_id = o.customer_id
  LEFT
  JOIN  stg_sellers AS s
    ON  s.seller_id = soi.seller_id;

CREATE INDEX IF NOT EXISTS idx_fact_order_items_date_key
    ON fact_order_items (order_purchase_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_product
    ON fact_order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_seller
    ON fact_order_items (seller_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_customer
    ON fact_order_items (customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_customer_zip_prefix
    ON fact_order_items (customer_zip_code_prefix);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_seller_zip_prefix
    ON fact_order_items (seller_zip_code_prefix);



/******************************************************************************************************************************************************/
/*
 * Section: fact_orders
 */
/******************************************************************************************************************************************************/

DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders (
    order_id                     TEXT     NOT NULL,
    customer_id                  TEXT     NOT NULL,
    order_purchase_date_key      INTEGER  NOT NULL,
    customer_zip_code_prefix     TEXT     NULL,
    order_status                 TEXT     NOT NULL,
    order_purchase_dt            TEXT     NOT NULL,
    order_approved_dt            TEXT     NULL,
    order_delivered_carrier_dt   TEXT     NULL,
    order_delivered_customer_dt  TEXT     NULL,
    order_estimated_delivery_dt  TEXT     NULL,
    approve_lead_days            INTEGER  NULL,
    delivery_lead_days           INTEGER  NULL,
    delivery_delay_days          INTEGER  NULL,
    is_delivered                 INTEGER  NOT NULL,
    is_canceled                  INTEGER  NOT NULL,

    PRIMARY KEY (order_id)
);

INSERT INTO fact_orders (
    order_id,
    customer_id,
    order_purchase_date_key,
    customer_zip_code_prefix,
    order_status,
    order_purchase_dt,
    order_approved_dt,
    order_delivered_carrier_dt,
    order_delivered_customer_dt,
    order_estimated_delivery_dt,
    approve_lead_days,
    delivery_lead_days,
    delivery_delay_days,
    is_delivered,
    is_canceled
)
SELECT  so.order_id
        ,so.customer_id
        ,CAST(strftime('%Y%m%d', so.order_purchase_dt) AS INTEGER) AS order_purchase_date_key
        ,sc.customer_zip_code_prefix
        ,so.order_status
        ,so.order_purchase_dt
        ,so.order_approved_dt
        ,so.order_delivered_carrier_dt
        ,so.order_delivered_customer_dt
        ,so.order_estimated_delivery_dt
        ,so.approve_lead_days
        ,so.delivery_lead_days
        ,so.delivery_delay_days
        ,so.is_delivered
        ,so.is_canceled
  FROM  stg_orders AS so
  LEFT
  JOIN  stg_customers AS sc
    ON  sc.customer_id = so.customer_id;

CREATE INDEX IF NOT EXISTS idx_fact_orders_purchase_date_key
    ON fact_orders (order_purchase_date_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_id
    ON fact_orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_orders_status
    ON fact_orders (order_status);

CREATE INDEX IF NOT EXISTS idx_fact_orders_zip_prefix
    ON fact_orders (customer_zip_code_prefix);



/******************************************************************************************************************************************************/
/*
 * Section: vw_delivered_orders
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_delivered_orders;

CREATE VIEW vw_delivered_orders AS
SELECT  fo.order_id
        ,fo.customer_id
        ,fo.customer_zip_code_prefix
        ,fo.order_purchase_date_key
        ,dd.year_month
        ,fo.order_status
        ,fo.is_delivered
        ,fo.is_canceled
        ,fo.order_purchase_dt
        ,fo.order_approved_dt
        ,fo.order_delivered_carrier_dt
        ,fo.order_delivered_customer_dt
        ,fo.order_estimated_delivery_dt
        ,fo.approve_lead_days
        ,fo.delivery_lead_days
        ,fo.delivery_delay_days
  FROM  fact_orders AS fo
 INNER
  JOIN  dim_date AS dd
    ON  dd.date_key = fo.order_purchase_date_key
 WHERE  fo.order_status = 'delivered'
   AND  fo.is_delivered = 1;



/******************************************************************************************************************************************************/
/*
 * Section: vw_delivered_order_items
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_delivered_order_items;

CREATE VIEW vw_delivered_order_items AS
SELECT  foi.order_id
        ,foi.order_item_id
        ,foi.order_item_seq
        ,vdo.customer_id
        ,vdo.customer_zip_code_prefix
        ,foi.product_id
        ,foi.seller_id
        ,foi.seller_zip_code_prefix
        ,vdo.order_purchase_date_key
        ,vdo.year_month
        ,foi.price
        ,foi.freight_value
        ,foi.item_total_value
  FROM  fact_order_items AS foi
 INNER
  JOIN  vw_delivered_orders AS vdo
    ON  vdo.order_id = foi.order_id;



/******************************************************************************************************************************************************/
/*
 * Section: vw_customer_first_purchase_month
 */
/******************************************************************************************************************************************************/

DROP VIEW IF EXISTS vw_customer_first_purchase_month;

CREATE VIEW vw_customer_first_purchase_month AS
WITH first_purchase AS (
    SELECT  dc.customer_unique_id
            ,MIN(vdo.order_purchase_date_key) AS first_purchase_date_key
      FROM  vw_delivered_orders AS vdo
     INNER
      JOIN  dim_customer AS dc
        ON  dc.customer_id = vdo.customer_id
     GROUP
        BY  dc.customer_unique_id
)
SELECT  fp.customer_unique_id
        ,fp.first_purchase_date_key
        ,dd.year_month AS first_purchase_year_month
  FROM  first_purchase AS fp
 INNER
  JOIN  dim_date AS dd
    ON  dd.date_key = fp.first_purchase_date_key;