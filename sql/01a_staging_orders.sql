CREATE SCHEMA IF NOT EXISTS staging;

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
  fo.any_heavy_bulky,
  CASE
    WHEN fo.order_delivered_customer_date IS NOT NULL
     AND fo.order_estimated_delivery_date IS NOT NULL
     AND fo.order_delivered_customer_date <= fo.order_estimated_delivery_date
    THEN 1 ELSE 0
  END AS ontime_flag
FROM raw.features_orders fo;
