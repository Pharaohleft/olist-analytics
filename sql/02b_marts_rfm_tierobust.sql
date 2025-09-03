CREATE SCHEMA IF NOT EXISTS mart;

-- Rebuild rfm_scored with tie-robust quintiles (5=best)
CREATE TABLE IF NOT EXISTS mart.rfm_scored (
  cust_uid      text PRIMARY KEY,
  recency_days  int,
  frequency     int,
  monetary      numeric(18,6),
  r             int,
  f             int,
  m             int,
  rfm_sum       int
);
TRUNCATE mart.rfm_scored;

WITH r_q AS (
  SELECT
    cust_uid,
    recency_days,
    -- small recency = more recent = better -> 5
    (6 - width_bucket(cume_dist() OVER (ORDER BY recency_days ASC), 0, 1, 5))::int AS r
  FROM mart.rfm
),
f_q AS (
  SELECT
    cust_uid,
    (6 - width_bucket(cume_dist() OVER (ORDER BY frequency DESC), 0, 1, 5))::int AS f
  FROM mart.rfm
),
m_q AS (
  SELECT
    cust_uid,
    (6 - width_bucket(cume_dist() OVER (ORDER BY monetary DESC), 0, 1, 5))::int AS m
  FROM mart.rfm
)
INSERT INTO mart.rfm_scored (cust_uid, recency_days, frequency, monetary, r, f, m, rfm_sum)
SELECT
  rf.cust_uid,
  rf.recency_days,
  rf.frequency,
  rf.monetary,
  r_q.r,
  f_q.f,
  m_q.m,
  (r_q.r + f_q.f + m_q.m) AS rfm_sum
FROM mart.rfm rf
JOIN r_q USING (cust_uid)
JOIN f_q USING (cust_uid)
JOIN m_q USING (cust_uid);
