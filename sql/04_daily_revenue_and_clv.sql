CREATE SCHEMA IF NOT EXISTS mart;

-- ========== DAILY REVENUE (overall) ==========
CREATE TABLE IF NOT EXISTS mart.daily_revenue (
  d date PRIMARY KEY,
  orders int,
  gmv numeric(18,6)
);
TRUNCATE mart.daily_revenue;

INSERT INTO mart.daily_revenue (d, orders, gmv)
SELECT
  o.order_ts::date AS d,
  COUNT(*)         AS orders,
  COALESCE(SUM(o.gmv),0)::numeric(18,6) AS gmv
FROM staging.orders o
GROUP BY o.order_ts::date
ORDER BY d;

-- ========== CLV PROXY (rolling 90-day average per customer) ==========
-- Approach:
--   1) Aggregate to customer-day GMV
--   2) For each (cust_uid, day), compute sum over prior 90 days (inclusive) / 90

CREATE TABLE IF NOT EXISTS mart.clv_proxy (
  cust_uid text,
  as_of_date date,
  clv_90d numeric(18,6),
  PRIMARY KEY (cust_uid, as_of_date)
);
TRUNCATE mart.clv_proxy;

WITH daily_cust AS (
  SELECT
    o.cust_uid,
    o.order_ts::date AS as_of_date,
    COALESCE(SUM(o.gmv),0)::numeric(18,6) AS gmv_day
  FROM staging.orders o
  GROUP BY o.cust_uid, o.order_ts::date
),
roll AS (
  SELECT
    a.cust_uid,
    a.as_of_date,
    (
      SELECT COALESCE(SUM(b.gmv_day),0)
      FROM daily_cust b
      WHERE b.cust_uid = a.cust_uid
        AND b.as_of_date > a.as_of_date - INTERVAL '90 days'
        AND b.as_of_date <= a.as_of_date
    ) / 90.0 AS clv_90d
  FROM daily_cust a
)
INSERT INTO mart.clv_proxy (cust_uid, as_of_date, clv_90d)
SELECT cust_uid, as_of_date, clv_90d::numeric(18,6)
FROM roll;

-- Helpful indexes for snappy lookups
CREATE INDEX IF NOT EXISTS idx_clv_proxy_cust_date ON mart.clv_proxy (cust_uid, as_of_date DESC);
