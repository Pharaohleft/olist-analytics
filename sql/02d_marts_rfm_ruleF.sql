CREATE SCHEMA IF NOT EXISTS mart;

-- Rebuild rfm_scored using:
--   R: quintiles (5 = most recent)
--   M: quintiles (5 = highest monetary)
--   F: RULE-BASED buckets to avoid collapse
--        F1 = 1 order
--        F3 = 2 orders
--        F4 = 3–4 orders
--        F5 = 5+ orders

TRUNCATE mart.rfm_scored;

WITH r_dist AS (
  SELECT
    cust_uid,
    recency_days,
    cume_dist() OVER (ORDER BY recency_days ASC) AS cd_r
  FROM mart.rfm
),
m_dist AS (
  SELECT
    cust_uid,
    monetary,
    cume_dist() OVER (ORDER BY monetary DESC) AS cd_m
  FROM mart.rfm
),
r_b AS (
  SELECT
    cust_uid,
    recency_days,
    -- clamp 1.0 a hair down so bucket is 1..5 then flip so 5=best (most recent)
    (6 - (1 + floor(LEAST(cd_r, 0.9999999999) * 5))::int) AS r
  FROM r_dist
),
m_b AS (
  SELECT
    cust_uid,
    monetary,
    -- 5 = highest monetary
    (6 - (1 + floor(LEAST(cd_m, 0.9999999999) * 5))::int) AS m
  FROM m_dist
),
f_b AS (
  SELECT
    cust_uid,
    frequency,
    CASE
      WHEN frequency IS NULL THEN 1
      WHEN frequency = 1 THEN 1
      WHEN frequency = 2 THEN 3
      WHEN frequency BETWEEN 3 AND 4 THEN 4
      ELSE 5
    END AS f
  FROM mart.rfm
)
INSERT INTO mart.rfm_scored (cust_uid, recency_days, frequency, monetary, r, f, m, rfm_sum)
SELECT
  rf.cust_uid,
  rf.recency_days,
  rf.frequency,
  rf.monetary,
  r_b.r,
  f_b.f,
  m_b.m,
  (r_b.r + f_b.f + m_b.m) AS rfm_sum
FROM mart.rfm rf
JOIN r_b   ON r_b.cust_uid = rf.cust_uid
JOIN m_b   ON m_b.cust_uid = rf.cust_uid
JOIN f_b   ON f_b.cust_uid = rf.cust_uid;
