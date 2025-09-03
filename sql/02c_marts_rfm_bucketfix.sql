CREATE SCHEMA IF NOT EXISTS mart;

-- Rebuild rfm_scored with clamped CUME_DIST -> 5 buckets (no zeros)
TRUNCATE mart.rfm_scored;

WITH dist AS (
  SELECT
    cust_uid, recency_days, frequency, monetary,
    cume_dist() OVER (ORDER BY recency_days ASC)  AS cd_r,
    cume_dist() OVER (ORDER BY frequency    DESC) AS cd_f,
    cume_dist() OVER (ORDER BY monetary     DESC) AS cd_m
  FROM mart.rfm
),
buckets AS (
  SELECT
    cust_uid, recency_days, frequency, monetary,
    -- Map (0,1] -> {1..5} by clamping 1.0 down just a hair
    (1 + floor(LEAST(cd_r, 0.9999999999) * 5))::int AS br,
    (1 + floor(LEAST(cd_f, 0.9999999999) * 5))::int AS bf,
    (1 + floor(LEAST(cd_m, 0.9999999999) * 5))::int AS bm
  FROM dist
)
INSERT INTO mart.rfm_scored (cust_uid, recency_days, frequency, monetary, r, f, m, rfm_sum)
SELECT
  cust_uid, recency_days, frequency, monetary,
  (6 - br) AS r,  -- smaller recency = more recent = better
  (6 - bf) AS f,  -- larger frequency = better
  (6 - bm) AS m,  -- larger monetary  = better
  (6 - br) + (6 - bf) + (6 - bm) AS rfm_sum
FROM buckets;
