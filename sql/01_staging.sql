CREATE SCHEMA IF NOT EXISTS staging;

-- Orders (matches your raw.features_orders columns)
CREATE OR REPLACE VIEW staging.orders AS
SELECT
  fo.order_id,
  fo.cust_uid,
  fo.order_purchase_timestamp       AS order_ts,
  fo.order_delivered_customer_date  AS delivered_ts,
  fo.order_estimated_delivery_date  AS eta_ts,
  fo.customer_id,
  fo.cust_state,
  fo.gmv,
  fo.merchandise_total,
  fo.freight_total,
  fo.pay_total,
  fo.review_score,
  fo.main_payment_type,
  CASE
    WHEN fo.order_delivered_customer_date IS NOT NULL
     AND fo.order_estimated_delivery_date IS NOT NULL
     AND fo.order_delivered_customer_date <= fo.order_estimated_delivery_date
    THEN 1 ELSE 0
  END AS ontime_flag
FROM raw.features_orders fo;

-- Items
CREATE OR REPLACE VIEW staging.order_items AS
SELECT
  oi.order_id,
  oi.product_id,
  oi.seller_id,
  COALESCE(oi.price, 0)::numeric(18,6)        AS item_price,
  COALESCE(oi.freight_value, 0)::numeric(18,6) AS item_freight
FROM raw.order_items_clean oi;

-- Products + heavy/bulky heuristic
CREATE OR REPLACE VIEW staging.products AS
SELECT
  p.product_id,
  p.product_category_name,
  NULLIF(p.product_weight_g,'')::numeric AS product_weight_g,
  NULLIF(p.product_length_cm,'')::numeric AS product_length_cm,
  NULLIF(p.product_height_cm,'')::numeric AS product_height_cm,
  NULLIF(p.product_width_cm,'')::numeric  AS product_width_cm,
  CASE
    WHEN NULLIF(p.product_weight_g,'')::numeric >= 5000 THEN 1
    WHEN (NULLIF(p.product_length_cm,'')::numeric
        * NULLIF(p.product_height_cm,'')::numeric
        * NULLIF(p.product_width_cm,'')::numeric) >= 45000 THEN 1
    ELSE 0
  END AS heavy_bulky_flag
FROM raw.products_clean p;

-- Reviews
CREATE OR REPLACE VIEW staging.reviews AS
SELECT
  r.order_id,
  NULLIF(r.review_score,'')::numeric AS review_score
FROM raw.order_reviews_dedup r;

-- Payments
CREATE OR REPLACE VIEW staging.payments AS
SELECT
  pa.order_id,
  pa.payment_type,
  NULLIF(pa.payment_value,'')::numeric AS payment_value
FROM raw.payments_order_agg pa;
