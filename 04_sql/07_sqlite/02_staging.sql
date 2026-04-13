/****************************************************************************************************************************************/
/*
 * File: 02_staging.sql
 * Description:
 *  - SQLite 기반 Staging 레이어 통합 ETL
 *  - raw 테이블을 기준으로 stg_orders, stg_customers, stg_order_items, stg_products,
 *    stg_sellers, stg_order_payments, stg_order_reviews, stg_geolocation 생성
 *  - 검증/DQ 확인 쿼리는 제외하고 ETL 로직만 포함
 */
/****************************************************************************************************************************************/



/****************************************************************************************************************************************/
/*
 * Section: stg_orders
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_orders;

CREATE TABLE stg_orders (
    order_id                     TEXT     NOT NULL,
    customer_id                  TEXT     NOT NULL,
    order_status                 TEXT     NOT NULL,

    order_purchase_dt            TEXT     NOT NULL,
    order_approved_dt            TEXT     NULL,
    order_delivered_carrier_dt   TEXT     NULL,
    order_delivered_customer_dt  TEXT     NULL,
    order_estimated_delivery_dt  TEXT     NULL,

    order_purchase_date          TEXT     NOT NULL,
    order_year                   INTEGER  NOT NULL,
    order_month                  INTEGER  NOT NULL,
    order_year_month             TEXT     NOT NULL,

    approve_lead_days            INTEGER  NULL,
    delivery_lead_days           INTEGER  NULL,
    delivery_delay_days          INTEGER  NULL,

    is_delivered                 INTEGER  NOT NULL,
    is_canceled                  INTEGER  NOT NULL,

    is_time_inconsistent         INTEGER  NOT NULL,
    is_status_inconsistent       INTEGER  NOT NULL,
    is_carrier_dt_missing        INTEGER  NOT NULL,

    PRIMARY KEY (order_id)
);

INSERT INTO stg_orders (
    order_id,
    customer_id,
    order_status,

    order_purchase_dt,
    order_approved_dt,
    order_delivered_carrier_dt,
    order_delivered_customer_dt,
    order_estimated_delivery_dt,

    order_purchase_date,
    order_year,
    order_month,
    order_year_month,

    approve_lead_days,
    delivery_lead_days,
    delivery_delay_days,

    is_delivered,
    is_canceled,

    is_time_inconsistent,
    is_status_inconsistent,
    is_carrier_dt_missing
)
WITH parsed AS (
    SELECT  trim(order_id) AS order_id
            ,trim(customer_id) AS customer_id
            ,lower(trim(replace(CAST(order_status AS TEXT), char(13), ''))) AS order_status

            ,datetime(replace(CAST(order_purchase_timestamp AS TEXT), char(13), '')) AS purchase_dt
            ,datetime(nullif(replace(CAST(order_approved_at AS TEXT), char(13), ''), '')) AS approved_dt
            ,datetime(nullif(replace(CAST(order_delivered_carrier_date AS TEXT), char(13), ''), '')) AS delivered_carrier_dt
            ,datetime(nullif(replace(CAST(order_delivered_customer_date AS TEXT), char(13), ''), '')) AS delivered_customer_dt
            ,datetime(nullif(replace(CAST(order_estimated_delivery_date AS TEXT), char(13), ''), '')) AS estimated_delivery_dt
      FROM  raw_orders
)
SELECT  order_id
        ,customer_id
        ,order_status

        ,purchase_dt AS order_purchase_dt
        ,approved_dt AS order_approved_dt
        ,delivered_carrier_dt AS order_delivered_carrier_dt
        ,delivered_customer_dt AS order_delivered_customer_dt
        ,date(estimated_delivery_dt) AS order_estimated_delivery_dt

        ,date(purchase_dt) AS order_purchase_date
        ,CAST(strftime('%Y', purchase_dt) AS INTEGER) AS order_year
        ,CAST(strftime('%m', purchase_dt) AS INTEGER) AS order_month
        ,strftime('%Y-%m', purchase_dt) AS order_year_month

        ,CASE WHEN approved_dt IS NULL THEN NULL
              WHEN approved_dt < purchase_dt THEN NULL
              ELSE CAST(julianday(date(approved_dt)) - julianday(date(purchase_dt)) AS INTEGER)
              END AS approve_lead_days

        ,CASE WHEN delivered_customer_dt IS NULL THEN NULL
              WHEN delivered_customer_dt < purchase_dt THEN NULL
              ELSE CAST(julianday(date(delivered_customer_dt)) - julianday(date(purchase_dt)) AS INTEGER)
              END AS delivery_lead_days

        ,CASE WHEN delivered_customer_dt IS NULL OR estimated_delivery_dt IS NULL THEN NULL
              WHEN delivered_customer_dt < purchase_dt THEN NULL
              ELSE CAST(julianday(date(delivered_customer_dt)) - julianday(date(estimated_delivery_dt)) AS INTEGER)
              END AS delivery_delay_days

        ,CASE WHEN delivered_customer_dt IS NULL THEN 0 ELSE 1 END AS is_delivered
        ,CASE WHEN order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END AS is_canceled

        ,CASE WHEN (approved_dt IS NOT NULL AND approved_dt < purchase_dt)
                OR (delivered_carrier_dt IS NOT NULL AND delivered_carrier_dt < purchase_dt)
                OR (delivered_customer_dt IS NOT NULL AND delivered_customer_dt < purchase_dt) THEN 1
              ELSE 0
              END AS is_time_inconsistent

        ,CASE WHEN (order_status = 'delivered' AND delivered_customer_dt IS NULL)
                OR (order_status IN ('canceled', 'unavailable') AND delivered_customer_dt IS NOT NULL) THEN 1
              ELSE 0
              END AS is_status_inconsistent

        ,CASE WHEN order_status IN ('shipped', 'invoiced') AND delivered_carrier_dt IS NULL THEN 1
              ELSE 0
              END AS is_carrier_dt_missing
  FROM  parsed
 WHERE  purchase_dt IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id
    ON stg_orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_purchase_dt
    ON stg_orders (order_purchase_dt);

CREATE INDEX IF NOT EXISTS idx_stg_orders_year_month
    ON stg_orders (order_year_month);



/****************************************************************************************************************************************/
/*
 * Section: stg_customers
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_customers;

CREATE TABLE stg_customers (
    customer_id               TEXT     NOT NULL,
    customer_unique_id        TEXT     NOT NULL,
    customer_zip_code_prefix  TEXT     NULL,
    customer_city             TEXT     NULL,
    customer_state            TEXT     NULL,
    customer_city_state       TEXT     NULL,

    PRIMARY KEY (customer_id)
);

INSERT INTO stg_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    customer_city_state
)
SELECT  customer_id
        ,customer_unique_id
        ,zip_norm AS customer_zip_code_prefix
        ,city_norm AS customer_city
        ,state_norm AS customer_state
        ,CASE WHEN city_norm IS NOT NULL AND state_norm IS NOT NULL THEN city_norm || '_' || state_norm
              ELSE NULL
              END AS customer_city_state
  FROM  (
        SELECT  trim(customer_id) AS customer_id
                ,trim(customer_unique_id) AS customer_unique_id
                ,CASE WHEN customer_zip_code_prefix IS NULL
                           OR trim(replace(CAST(customer_zip_code_prefix AS TEXT), char(13), '')) = '' THEN NULL
                      ELSE substr('00000' || trim(replace(CAST(customer_zip_code_prefix AS TEXT), char(13), '')), -5, 5)
                      END AS zip_norm
                ,NULLIF(lower(trim(replace(CAST(customer_city AS TEXT), char(13), ''))), '') AS city_norm
                ,NULLIF(upper(trim(replace(CAST(customer_state AS TEXT), char(13), ''))), '') AS state_norm
          FROM  raw_customers
        ) cleaned
 WHERE  customer_id IS NOT NULL
   AND  customer_id <> '';

CREATE INDEX IF NOT EXISTS idx_stg_customers_unique_id
    ON stg_customers (customer_unique_id);

CREATE INDEX IF NOT EXISTS idx_stg_customers_zip_prefix
    ON stg_customers (customer_zip_code_prefix);

CREATE INDEX IF NOT EXISTS idx_stg_customers_state
    ON stg_customers (customer_state);



/****************************************************************************************************************************************/
/*
 * Section: stg_order_items
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_order_items;

CREATE TABLE stg_order_items (
    order_id             TEXT     NOT NULL,
    order_item_id        TEXT     NOT NULL,
    product_id           TEXT     NOT NULL,
    seller_id            TEXT     NOT NULL,
    shipping_limit_dt    TEXT     NULL,
    price                REAL     NULL,
    freight_value        REAL     NULL,

    shipping_limit_date  TEXT     NULL,

    order_item_seq       INTEGER  NULL,
    item_total_value     REAL     NULL,

    PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO stg_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_dt,
    price,
    freight_value,

    shipping_limit_date,

    order_item_seq,
    item_total_value
)
SELECT  order_id
        ,order_item_id
        ,product_id
        ,seller_id
        ,shipping_limit_dt
        ,price
        ,freight_value

        ,date(shipping_limit_dt) AS shipping_limit_date

        ,CAST(trim(order_item_id) AS INTEGER) AS order_item_seq
        ,CASE WHEN price IS NULL OR freight_value IS NULL THEN NULL
              ELSE ROUND(price + freight_value, 2)
              END AS item_total_value
  FROM  (
        SELECT  trim(order_id) AS order_id
                ,trim(order_item_id) AS order_item_id
                ,trim(product_id) AS product_id
                ,trim(seller_id) AS seller_id
                ,datetime(replace(trim(CAST(shipping_limit_date AS TEXT)), char(13), '')) AS shipping_limit_dt
                ,CAST(price AS REAL) AS price
                ,CAST(freight_value AS REAL) AS freight_value
          FROM  raw_order_items
        ) cleaned
 WHERE  order_id IS NOT NULL
   AND  order_id <> ''
   AND  order_item_id IS NOT NULL
   AND  order_item_id <> ''
   AND  product_id IS NOT NULL
   AND  product_id <> ''
   AND  seller_id IS NOT NULL
   AND  seller_id <> '';

CREATE INDEX IF NOT EXISTS idx_stg_order_items_product_id
    ON stg_order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_seller_id
    ON stg_order_items (seller_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_shipping_limit_dt
    ON stg_order_items (shipping_limit_dt);



/****************************************************************************************************************************************/
/*
 * Section: stg_products
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_products;

CREATE TABLE stg_products (
    product_id                   TEXT     NOT NULL,
    product_category_name        TEXT     NULL,
    product_category_name_en     TEXT     NULL,
    product_name_length          INTEGER  NULL,
    product_description_length   INTEGER  NULL,
    product_photos_qty           INTEGER  NULL,
    product_weight_g             INTEGER  NULL,
    product_length_cm            INTEGER  NULL,
    product_height_cm            INTEGER  NULL,
    product_width_cm             INTEGER  NULL,

    product_volume_cm3           INTEGER  NULL,

    is_category_blank            INTEGER  NOT NULL,
    is_category_en_unmapped      INTEGER  NOT NULL,
    is_weight_zero               INTEGER  NOT NULL,

    PRIMARY KEY (product_id)
);

INSERT INTO stg_products (
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
              ELSE 0
              END AS is_weight_zero
  FROM  (
        SELECT  trim(product_id) AS product_id
                ,NULLIF(lower(trim(replace(CAST(product_category_name AS TEXT), char(13), ''))), '') AS product_category_name_norm
                ,CAST(product_name_length AS INTEGER) AS product_name_length
                ,CAST(product_description_length AS INTEGER) AS product_description_length
                ,CAST(product_photos_qty AS INTEGER) AS product_photos_qty
                ,CAST(product_weight_g AS INTEGER) AS product_weight_g
                ,CAST(product_length_cm AS INTEGER) AS product_length_cm
                ,CAST(product_height_cm AS INTEGER) AS product_height_cm
                ,CAST(product_width_cm AS INTEGER) AS product_width_cm
          FROM  raw_products
        ) c
  LEFT
  JOIN  (
        SELECT  NULLIF(lower(trim(replace(CAST(product_category_name AS TEXT), char(13), ''))), '') AS product_category_name_norm
                ,NULLIF(lower(trim(replace(CAST(product_category_name_english AS TEXT), char(13), ''))), '') AS product_category_name_english_norm
          FROM  raw_product_category_name_translation
        ) t
    ON  t.product_category_name_norm = c.product_category_name_norm
 WHERE  c.product_id IS NOT NULL
   AND  c.product_id <> '';

CREATE INDEX IF NOT EXISTS idx_stg_products_category_name
    ON stg_products (product_category_name);



/****************************************************************************************************************************************/
/*
 * Section: stg_sellers
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_sellers;

CREATE TABLE stg_sellers (
    seller_id                TEXT     NOT NULL,
    seller_zip_code_prefix   TEXT     NULL,
    seller_city              TEXT     NULL,
    seller_state             TEXT     NULL,
    seller_city_state        TEXT     NULL,

    PRIMARY KEY (seller_id)
);

INSERT INTO stg_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    seller_city_state
)
SELECT  seller_id
        ,zip_code_norm AS seller_zip_code_prefix
        ,city_norm AS seller_city
        ,state_norm AS seller_state
        ,CASE WHEN city_norm IS NOT NULL AND state_norm IS NOT NULL THEN city_norm || '_' || state_norm
              ELSE NULL
              END AS seller_city_state
  FROM  (
        SELECT  trim(seller_id) AS seller_id
                ,CASE WHEN seller_zip_code_prefix IS NULL
                           OR trim(replace(CAST(seller_zip_code_prefix AS TEXT), char(13), '')) = '' THEN NULL
                      ELSE substr('00000' || trim(replace(CAST(seller_zip_code_prefix AS TEXT), char(13), '')), -5, 5)
                      END AS zip_code_norm
                ,NULLIF(lower(trim(replace(CAST(seller_city AS TEXT), char(13), ''))), '') AS city_norm
                ,NULLIF(upper(trim(replace(CAST(seller_state AS TEXT), char(13), ''))), '') AS state_norm
          FROM  raw_sellers
        ) cleaned
 WHERE  seller_id IS NOT NULL
   AND  seller_id <> '';

CREATE INDEX IF NOT EXISTS idx_stg_sellers_zip_prefix
    ON stg_sellers (seller_zip_code_prefix);

CREATE INDEX IF NOT EXISTS idx_stg_sellers_state
    ON stg_sellers (seller_state);



/****************************************************************************************************************************************/
/*
 * Section: stg_order_payments
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_order_payments;

CREATE TABLE stg_order_payments (
    order_id                TEXT     NOT NULL,
    payment_sequential      INTEGER  NOT NULL,
    payment_type            TEXT     NULL,
    payment_installments    INTEGER  NULL,
    payment_value           REAL     NULL,

    is_installments_zero    INTEGER  NOT NULL,
    is_payment_value_zero   INTEGER  NOT NULL,

    PRIMARY KEY (order_id, payment_sequential)
);

INSERT INTO stg_order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,

    is_installments_zero,
    is_payment_value_zero
)
SELECT  trim(order_id) AS order_id
        ,CAST(payment_sequential AS INTEGER) AS payment_sequential
        ,lower(trim(replace(CAST(payment_type AS TEXT), char(13), ''))) AS payment_type
        ,CAST(payment_installments AS INTEGER) AS payment_installments
        ,CAST(payment_value AS REAL) AS payment_value

        ,CASE WHEN CAST(payment_installments AS INTEGER) = 0 THEN 1
              ELSE 0
              END AS is_installments_zero
        ,CASE WHEN CAST(payment_value AS REAL) = 0 THEN 1
              ELSE 0
              END AS is_payment_value_zero
  FROM  raw_order_payments
 WHERE  order_id IS NOT NULL
   AND  trim(order_id) <> ''
   AND  payment_sequential IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stg_order_payments_order_id
    ON stg_order_payments (order_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_payments_type
    ON stg_order_payments (payment_type);



/****************************************************************************************************************************************/
/*
 * Section: stg_order_reviews
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_order_reviews;

CREATE TABLE stg_order_reviews (
    review_id                TEXT     NOT NULL,
    order_id                 TEXT     NOT NULL,
    review_score             INTEGER  NOT NULL,
    review_comment_title     TEXT     NULL,
    review_comment_message   TEXT     NULL,
    review_creation_dt       TEXT     NULL,
    review_answer_dt         TEXT     NULL,

    review_creation_date     TEXT     NULL,
    review_answer_date       TEXT     NULL,

    is_title_blank           INTEGER  NOT NULL,
    is_message_blank         INTEGER  NOT NULL,

    PRIMARY KEY (review_id, order_id)
);

INSERT INTO stg_order_reviews (
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

        ,date(review_creation_dt) AS review_creation_date
        ,date(review_answer_dt) AS review_answer_date

        ,CASE WHEN title_norm IS NULL THEN 1
              ELSE 0
              END AS is_title_blank
        ,CASE WHEN message_norm IS NULL THEN 1
              ELSE 0
              END AS is_message_blank
  FROM  (
        SELECT  trim(review_id) AS review_id
                ,trim(order_id) AS order_id
                ,CAST(review_score AS INTEGER) AS review_score
                ,NULLIF(trim(replace(CAST(review_comment_title AS TEXT), char(13), '')), '') AS title_norm
                ,NULLIF(trim(replace(CAST(review_comment_message AS TEXT), char(13), '')), '') AS message_norm
                ,datetime(replace(CAST(review_creation_date AS TEXT), char(13), '')) AS review_creation_dt
                ,datetime(replace(CAST(review_answer_timestamp AS TEXT), char(13), '')) AS review_answer_dt
          FROM  raw_order_reviews
        ) t
 WHERE  review_id IS NOT NULL
   AND  review_id <> ''
   AND  order_id IS NOT NULL
   AND  order_id <> ''
   AND  review_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stg_order_reviews_order_id
    ON stg_order_reviews (order_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_reviews_creation_dt
    ON stg_order_reviews (review_creation_dt);



/****************************************************************************************************************************************/
/*
 * Section: stg_geolocation
 */
/****************************************************************************************************************************************/

DROP TABLE IF EXISTS stg_geolocation;

CREATE TABLE stg_geolocation (
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

INSERT INTO stg_geolocation (
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
    SELECT  substr('00000' || trim(replace(CAST(geolocation_zip_code_prefix AS TEXT), char(13), '')), -5, 5) AS zip_prefix
            ,CAST(geolocation_lat AS REAL) AS lat
            ,CAST(geolocation_lng AS REAL) AS lng
            ,NULLIF(lower(trim(replace(CAST(geolocation_city AS TEXT), char(13), ''))), '') AS city_norm
            ,NULLIF(upper(trim(replace(CAST(geolocation_state AS TEXT), char(13), ''))), '') AS state_norm
            ,CASE WHEN CAST(geolocation_lat AS REAL) BETWEEN -35 AND 6
                    AND CAST(geolocation_lng AS REAL) BETWEEN -75 AND -30 THEN 1
                  ELSE 0
                  END AS is_valid_latlng
      FROM  raw_geolocation
     WHERE  geolocation_zip_code_prefix IS NOT NULL
       AND  trim(replace(CAST(geolocation_zip_code_prefix AS TEXT), char(13), '')) <> ''
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
            ,ROW_NUMBER() OVER (PARTITION BY zip_prefix ORDER BY cnt DESC, city_norm ASC, state_norm ASC) AS rnk
      FROM  city_state_freq
)
SELECT  a.zip_prefix AS geolocation_zip_code_prefix
        ,lm.lat AS geolocation_lat
        ,lm.lng AS geolocation_lng
        ,csm.city_norm AS geolocation_city
        ,csm.state_norm AS geolocation_state

        ,CASE WHEN csm.city_norm IS NULL OR csm.state_norm IS NULL THEN NULL
              ELSE csm.city_norm || '_' || csm.state_norm
              END AS geolocation_city_state
        ,a.row_cnt
        ,COALESCE(lm.cnt, 0) AS mode_cnt
        ,ROUND(COALESCE(lm.cnt, 0) * 100.0 / a.row_cnt, 2) AS mode_ratio_pct
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

CREATE INDEX IF NOT EXISTS idx_stg_geolocation_state
    ON stg_geolocation (geolocation_state);

CREATE INDEX IF NOT EXISTS idx_stg_geolocation_city_state
    ON stg_geolocation (geolocation_city_state);