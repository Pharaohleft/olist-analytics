CREATE SCHEMA IF NOT EXISTS mart;

-- Optional: pin a reference date (else uses max(order_ts))
-- SET olist.ref_date = '2018-10-17';

CREATE TABLE IF NOT EXISTS mart.churn_features (
  cust_uid text PRIMARY KEY,
  recency_days int,
  frequency int,
  monetary numeric(18,6),
  r int, f int, m int,
  orders_30d int,
  orders_60d int,
  orders_90d int,
  avg_order_value numeric(18,6),
  avg_delivery_days numeric(18,6),
  avg_review_score numeric(18,6),
  pay_share_card numeric(18,6),
  pay_share_boleto numeric(18,6),
  pay_share_voucher numeric(18,6),
  heavy_bulky_share numeric(18,6),
  ontime_rate numeric(18,6)
);
TRUNCATE mart.churn_features;

WITH ref AS (
  SELECT COALESCE(
    NULLIF(current_setting('olist.ref_date', true), '')::date,
    (SELECT max(order_ts)::date FROM staging.orders)
  ) AS ref_date
),
orders AS (
  SELECT * FROM staging.orders
),
deliver AS (
  SELECT o.order_id, o.cust_uid,
         EXTRACT(EPOCH FROM (o.delivered_ts - o.order_ts))/86400.0 AS delivery_days
  FROM orders o
  WHERE o.delivered_ts IS NOT NULL AND o.order_ts IS NOT NULL
),
pmix AS (
  SELECT
    o.cust_uid,
    AVG(CASE WHEN LOWER(o.main_payment_type)='credit_card' THEN 1 ELSE 0 END)::numeric(18,6) AS pay_share_card,
    AVG(CASE WHEN LOWER(o.main_payment_type)='boleto'      THEN 1 ELSE 0 END)::numeric(18,6) AS pay_share_boleto,
    AVG(CASE WHEN LOWER(o.main_payment_type)='voucher'     THEN 1 ELSE 0 END)::numeric(18,6) AS pay_share_voucher
  FROM orders o
  GROUP BY o.cust_uid
),
o_recent AS (
  SELECT o.cust_uid,
         SUM(CASE WHEN o.order_ts::date > (ref.ref_date - INTERVAL '30 days') THEN 1 ELSE 0 END) AS orders_30d,
         SUM(CASE WHEN o.order_ts::date > (ref.ref_date - INTERVAL '60 days') THEN 1 ELSE 0 END) AS orders_60d,
         SUM(CASE WHEN o.order_ts::date > (ref.ref_date - INTERVAL '90 days') THEN 1 ELSE 0 END) AS orders_90d
  FROM orders o, ref
  GROUP BY o.cust_uid
),
aov AS (
  SELECT o.cust_uid, AVG(COALESCE(o.gmv,0))::numeric(18,6) AS avg_order_value
  FROM orders o
  GROUP BY o.cust_uid
),
deliv_cust AS (
  SELECT d.cust_uid, AVG(d.delivery_days)::numeric(18,6) AS avg_delivery_days
  FROM deliver d
  GROUP BY d.cust_uid
),
review_cust AS (
  SELECT o.cust_uid, AVG(o.review_score)::numeric(18,6) AS avg_review_score
  FROM orders o
  GROUP BY o.cust_uid
),
heavy_cust AS (
  SELECT o.cust_uid, AVG(CASE WHEN o.any_heavy_bulky THEN 1 ELSE 0 END)::numeric(18,6) AS heavy_bulky_share
  FROM orders o
  GROUP BY o.cust_uid
),
ontime AS (
  SELECT o.cust_uid, AVG(o.ontime_flag)::numeric(18,6) AS ontime_rate
  FROM orders o
  GROUP BY o.cust_uid
)
INSERT INTO mart.churn_features
SELECT
  r.cust_uid, r.recency_days, r.frequency, r.monetary,
  rs.r, rs.f, rs.m,
  orc.orders_30d, orc.orders_60d, orc.orders_90d,
  a.avg_order_value,
  d.avg_delivery_days,
  rv.avg_review_score,
  COALESCE(p.pay_share_card,0), COALESCE(p.pay_share_boleto,0), COALESCE(p.pay_share_voucher,0),
  COALESCE(h.heavy_bulky_share,0),
  otime.ontime_rate
FROM mart.rfm r
JOIN mart.rfm_scored rs USING (cust_uid)
LEFT JOIN o_recent   orc ON orc.cust_uid = r.cust_uid
LEFT JOIN aov        a   ON a.cust_uid   = r.cust_uid
LEFT JOIN deliv_cust d   ON d.cust_uid   = r.cust_uid
LEFT JOIN review_cust rv ON rv.cust_uid  = r.cust_uid
LEFT JOIN pmix       p   ON p.cust_uid   = r.cust_uid
LEFT JOIN heavy_cust h   ON h.cust_uid   = r.cust_uid
LEFT JOIN ontime     otime ON otime.cust_uid = r.cust_uid;
