CREATE SCHEMA IF NOT EXISTS mart;

-- Final snapshot table (create first, then fill)
CREATE TABLE IF NOT EXISTS mart.churn_snapshot (
  cust_uid text PRIMARY KEY,
  snapshot_date date,
  churn_90d int,
  recency_days int,
  frequency int,
  monetary numeric(18,6),
  r int, f int, m int,
  orders_30d int, orders_60d int, orders_90d int,
  avg_order_value numeric(18,6),
  avg_delivery_days numeric(18,6),
  avg_review_score numeric(18,6),
  pay_share_card numeric(18,6),
  pay_share_boleto numeric(18,6),
  pay_share_voucher numeric(18,6),
  heavy_bulky_share numeric(18,6),
  ontime_rate numeric(18,6)
);

TRUNCATE mart.churn_snapshot;

WITH
ref AS (
  SELECT (SELECT max(order_ts)::date FROM staging.orders) AS ref_end
),
snap AS (
  SELECT (ref_end - INTERVAL '90 days')::date AS t0 FROM ref
),
-- Orders up to snapshot t0
o_pre AS (
  SELECT o.*
  FROM staging.orders o, snap
  WHERE o.order_ts::date <= snap.t0
),
-- Orders after t0 within 90 days
o_post AS (
  SELECT o.*
  FROM staging.orders o, snap
  WHERE o.order_ts::date >  snap.t0
    AND o.order_ts::date <= (snap.t0 + INTERVAL '90 days')::date
),
-- RFM as of t0
rfm_pre AS (
  SELECT
    o.cust_uid,
    (SELECT t0 FROM snap) - max(o.order_ts)::date AS recency_days,
    COUNT(DISTINCT o.order_id)                     AS frequency,
    COALESCE(SUM(o.gmv),0)::numeric(18,6)          AS monetary
  FROM o_pre o
  GROUP BY o.cust_uid
),
-- Tie-robust R/M quintiles at t0 (5 = best)
r_dist AS (
  SELECT cust_uid, recency_days,
         cume_dist() OVER (ORDER BY recency_days ASC) AS cd_r
  FROM rfm_pre
),
m_dist AS (
  SELECT cust_uid, monetary,
         cume_dist() OVER (ORDER BY monetary DESC) AS cd_m
  FROM rfm_pre
),
r_b AS (
  SELECT cust_uid, recency_days,
         (6 - (1 + floor(LEAST(cd_r, 0.9999999999) * 5))::int) AS r
  FROM r_dist
),
m_b AS (
  SELECT cust_uid, monetary,
         (6 - (1 + floor(LEAST(cd_m, 0.9999999999) * 5))::int) AS m
  FROM m_dist
),
-- Rule-based F at t0
f_b AS (
  SELECT
    cust_uid, frequency,
    CASE
      WHEN frequency IS NULL THEN 1
      WHEN frequency = 1 THEN 1
      WHEN frequency = 2 THEN 3
      WHEN frequency BETWEEN 3 AND 4 THEN 4
      ELSE 5
    END AS f
  FROM rfm_pre
),
-- Windows as of t0
o_30 AS (
  SELECT cust_uid, COUNT(*) AS orders_30d
  FROM o_pre, snap
  WHERE order_ts::date > (snap.t0 - INTERVAL '30 days')::date
  GROUP BY cust_uid
),
o_60 AS (
  SELECT cust_uid, COUNT(*) AS orders_60d
  FROM o_pre, snap
  WHERE order_ts::date > (snap.t0 - INTERVAL '60 days')::date
  GROUP BY cust_uid
),
o_90 AS (
  SELECT cust_uid, COUNT(*) AS orders_90d
  FROM o_pre, snap
  WHERE order_ts::date > (snap.t0 - INTERVAL '90 days')::date
  GROUP BY cust_uid
),
-- Aggregates as of t0
aov AS (
  SELECT cust_uid, AVG(COALESCE(gmv,0))::numeric(18,6) AS avg_order_value
  FROM o_pre GROUP BY cust_uid
),
deliv AS (
  SELECT
    o.cust_uid,
    AVG(EXTRACT(EPOCH FROM (o.delivered_ts - o.order_ts))/86400.0)::numeric(18,6) AS avg_delivery_days
  FROM o_pre o
  WHERE o.delivered_ts IS NOT NULL AND o.order_ts IS NOT NULL
  GROUP BY o.cust_uid
),
rev AS (
  SELECT cust_uid, AVG(review_score)::numeric(18,6) AS avg_review_score
  FROM o_pre GROUP BY cust_uid
),
pmix AS (
  SELECT
    o.cust_uid,
    AVG(CASE WHEN LOWER(o.main_payment_type)='credit_card' THEN 1 ELSE 0 END)::numeric(18,6) AS pay_share_card,
    AVG(CASE WHEN LOWER(o.main_payment_type)='boleto'      THEN 1 ELSE 0 END)::numeric(18,6) AS pay_share_boleto,
    AVG(CASE WHEN LOWER(o.main_payment_type)='voucher'     THEN 1 ELSE 0 END)::numeric(18,6) AS pay_share_voucher
  FROM o_pre o GROUP BY o.cust_uid
),
heavy AS (
  SELECT cust_uid,
         AVG(CASE WHEN any_heavy_bulky THEN 1 ELSE 0 END)::numeric(18,6) AS heavy_bulky_share
  FROM o_pre GROUP BY cust_uid
),
ontime AS (
  SELECT cust_uid, AVG(ontime_flag)::numeric(18,6) AS ontime_rate
  FROM o_pre GROUP BY cust_uid
),
-- Label using FUTURE window after t0
label AS (
  SELECT r.cust_uid,
         CASE WHEN EXISTS (
           SELECT 1 FROM o_post p WHERE p.cust_uid = r.cust_uid
         ) THEN 0 ELSE 1 END AS churn_90d
  FROM rfm_pre r
)
INSERT INTO mart.churn_snapshot (
  cust_uid, snapshot_date, churn_90d,
  recency_days, frequency, monetary,
  r, f, m,
  orders_30d, orders_60d, orders_90d,
  avg_order_value, avg_delivery_days, avg_review_score,
  pay_share_card, pay_share_boleto, pay_share_voucher,
  heavy_bulky_share, ontime_rate
)
SELECT
  r.cust_uid,
  (SELECT t0 FROM snap) AS snapshot_date,
  lb.churn_90d,
  r.recency_days,
  r.frequency,
  r.monetary,
  r_b.r, f_b.f, m_b.m,
  COALESCE(o_30.orders_30d,0),
  COALESCE(o_60.orders_60d,0),
  COALESCE(o_90.orders_90d,0),
  aov.avg_order_value,
  deliv.avg_delivery_days,
  rev.avg_review_score,
  COALESCE(pmix.pay_share_card,0),
  COALESCE(pmix.pay_share_boleto,0),
  COALESCE(pmix.pay_share_voucher,0),
  COALESCE(heavy.heavy_bulky_share,0),
  COALESCE(ontime.ontime_rate,0)
FROM rfm_pre r
JOIN r_b   USING (cust_uid)
JOIN m_b   USING (cust_uid)
JOIN f_b   USING (cust_uid)
JOIN label lb USING (cust_uid)
LEFT JOIN o_30  USING (cust_uid)
LEFT JOIN o_60  USING (cust_uid)
LEFT JOIN o_90  USING (cust_uid)
LEFT JOIN aov   USING (cust_uid)
LEFT JOIN deliv USING (cust_uid)
LEFT JOIN rev   USING (cust_uid)
LEFT JOIN pmix  USING (cust_uid)
LEFT JOIN heavy USING (cust_uid)
LEFT JOIN ontime USING (cust_uid);
