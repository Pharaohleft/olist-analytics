CREATE SCHEMA IF NOT EXISTS mart;

-- Optional: SET olist.ref_date = '2018-10-17';

CREATE TABLE IF NOT EXISTS mart.rfm (
  cust_uid text PRIMARY KEY,
  recency_days int,
  frequency int,
  monetary numeric(18,6)
);
TRUNCATE mart.rfm;

WITH ref AS (
  SELECT COALESCE(
    NULLIF(current_setting('olist.ref_date', true), '')::date,
    (SELECT max(order_ts)::date FROM staging.orders)
  ) AS ref_date
)
INSERT INTO mart.rfm (cust_uid, recency_days, frequency, monetary)
SELECT
  o.cust_uid,
  (SELECT ref_date FROM ref) - max(o.order_ts)::date AS recency_days,
  COUNT(DISTINCT o.order_id) AS frequency,
  COALESCE(SUM(o.gmv),0)::numeric(18,6) AS monetary
FROM staging.orders o
GROUP BY o.cust_uid;

CREATE TABLE IF NOT EXISTS mart.rfm_scored (
  cust_uid text PRIMARY KEY,
  recency_days int,
  frequency int,
  monetary numeric(18,6),
  r int, f int, m int,
  rfm_sum int
);
TRUNCATE mart.rfm_scored;

WITH r_score AS (
  SELECT cust_uid, recency_days, NTILE(5) OVER (ORDER BY recency_days ASC, cust_uid) AS r FROM mart.rfm
),
f_score AS (
  SELECT cust_uid, NTILE(5) OVER (ORDER BY frequency DESC, cust_uid) AS f FROM mart.rfm
),
m_score AS (
  SELECT cust_uid, NTILE(5) OVER (ORDER BY monetary DESC, cust_uid) AS m FROM mart.rfm
)
INSERT INTO mart.rfm_scored
SELECT rf.cust_uid, rf.recency_days, rf.frequency, rf.monetary, r.r, f.f, m.m, (r.r + f.f + m.m) AS rfm_sum
FROM mart.rfm rf
JOIN r_score r USING (cust_uid)
JOIN f_score f USING (cust_uid)
JOIN m_score m USING (cust_uid);
