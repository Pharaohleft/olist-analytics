CREATE SCHEMA IF NOT EXISTS staging;

-- Items (already numeric from ingest)
CREATE OR REPLACE VIEW staging.order_items AS
SELECT
  order_id,
  product_id,
  seller_id,
  COALESCE(price, 0)::numeric(18,6)         AS item_price,
  COALESCE(freight_value, 0)::numeric(18,6) AS item_freight
FROM raw.order_items_clean;

-- Products (use boolean is_heavy_bulky from ingest)
CREATE OR REPLACE VIEW staging.products AS
SELECT
  product_id,
  product_category_name,
  product_weight_g::numeric(18,6)  AS product_weight_g,
  product_length_cm::numeric(18,6) AS product_length_cm,
  product_height_cm::numeric(18,6) AS product_height_cm,
  product_width_cm::numeric(18,6)  AS product_width_cm,
  product_volume_cm3::numeric(18,6) AS product_volume_cm3,
  CASE WHEN is_heavy_bulky THEN 1 ELSE 0 END AS heavy_bulky_flag
FROM raw.products_clean;

-- Reviews
CREATE OR REPLACE VIEW staging.reviews AS
SELECT
  order_id,
  review_score::numeric(10,2) AS review_score
FROM raw.order_reviews_dedup;

-- Payments (map to your actual columns)
CREATE OR REPLACE VIEW staging.payments AS
SELECT
  pa.order_id,
  pa.main_payment_type AS payment_type,
  pa.pay_total::numeric(18,6)     AS payment_value,
  pa.installments_max,
  pa.n_payment_methods
FROM raw.payments_order_agg pa;
