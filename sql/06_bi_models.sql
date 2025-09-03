CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- 1) Items view (safe casts)
CREATE OR REPLACE VIEW staging.order_items AS
SELECT
  oi.order_id,
  oi.product_id,
  oi.seller_id,
  COALESCE(NULLIF(oi.price::text,'')::numeric, 0)::numeric(18,6)         AS item_price,
  COALESCE(NULLIF(oi.freight_value::text,'')::numeric, 0)::numeric(18,6) AS item_freight
FROM raw.order_items_clean oi;

-- 2) Daily KPI table
CREATE TABLE IF NOT EXISTS mart.bi_orders_daily (
  d date PRIMARY KEY,
  orders int,
  gmv numeric(18,6),
  ontime_rate numeric(10,6),
  late_rate numeric(10,6),
  avg_review numeric(10,6),
  heavy_bulky_share numeric(10,6)
);
TRUNCATE mart.bi_orders_daily;
INSERT INTO mart.bi_orders_daily (d, orders, gmv, ontime_rate, late_rate, avg_review, heavy_bulky_share)
SELECT
  o.order_ts::date AS d,
  COUNT(*) AS orders,
  COALESCE(SUM(o.gmv),0)::numeric(18,6) AS gmv,
  AVG(o.ontime_flag)::numeric(10,6) AS ontime_rate,
  (1-AVG(o.ontime_flag))::numeric(10,6) AS late_rate,
  AVG(o.review_score)::numeric(10,6) AS avg_review,
  AVG(CASE WHEN o.any_heavy_bulky THEN 1 ELSE 0 END)::numeric(10,6) AS heavy_bulky_share
FROM staging.orders o
GROUP BY o.order_ts::date
ORDER BY d;

-- 3) Seller Pareto table
CREATE TABLE IF NOT EXISTS mart.seller_pareto (
  seller_id text,
  state text,
  gmv numeric(18,6),
  rank int,
  cumulative_share numeric(10,6)
);
TRUNCATE mart.seller_pareto;
WITH item_dest AS (
  SELECT
    oi.seller_id,
    o.cust_state,
    (oi.item_price + oi.item_freight) AS gmv_piece
  FROM staging.order_items oi
  JOIN staging.orders o USING(order_id)
),
seller_tot AS (
  SELECT seller_id, SUM(gmv_piece)::numeric(18,6) AS gmv_total
  FROM item_dest GROUP BY seller_id
),
seller_state_counts AS (
  SELECT seller_id, cust_state, COUNT(*) AS c
  FROM item_dest GROUP BY seller_id, cust_state
),
seller_dominant_state AS (
  SELECT seller_id, cust_state AS state
  FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY c DESC) AS rn
    FROM seller_state_counts s
  ) z WHERE z.rn=1
),
seller_rank AS (
  SELECT
    t.seller_id,
    COALESCE(d.state,'UNK') AS state,
    t.gmv_total,
    RANK() OVER (ORDER BY t.gmv_total DESC) AS rnk,
    SUM(t.gmv_total) OVER () AS gmv_all,
    SUM(t.gmv_total) OVER (ORDER BY t.gmv_total DESC) AS gmv_cum
  FROM seller_tot t
  LEFT JOIN seller_dominant_state d USING(seller_id)
)
INSERT INTO mart.seller_pareto (seller_id, state, gmv, rank, cumulative_share)
SELECT
  seller_id,
  state,
  gmv_total AS gmv,
  rnk AS rank,
  (gmv_cum / NULLIF(gmv_all,0))::numeric(10,6) AS cumulative_share
FROM seller_rank;

-- 4) Geo state table
CREATE TABLE IF NOT EXISTS mart.geo_state (
  state text PRIMARY KEY,
  cust_share numeric(10,6),
  seller_share numeric(10,6),
  gmv_share numeric(10,6),
  desert_index numeric(10,6)
);
TRUNCATE mart.geo_state;
WITH custs AS (
  SELECT cust_state AS state, COUNT(DISTINCT cust_uid) AS n_cust
  FROM staging.orders GROUP BY cust_state
),
cust_tot AS ( SELECT SUM(n_cust) AS n_all FROM custs ),
sell AS (
  SELECT state, COUNT(*) AS n_sellers
  FROM (SELECT DISTINCT seller_id, state FROM mart.seller_pareto) s
  GROUP BY state
),
sell_tot AS ( SELECT SUM(n_sellers) AS n_all FROM sell ),
gmv_s AS (
  SELECT cust_state AS state, SUM(gmv)::numeric(18,6) AS gmv_s
  FROM staging.orders GROUP BY cust_state
),
gmv_tot AS ( SELECT SUM(gmv)::numeric(18,6) AS gmv_all FROM staging.orders )
INSERT INTO mart.geo_state (state, cust_share, seller_share, gmv_share, desert_index)
SELECT
  s.state,
  (COALESCE(c.n_cust,0) / NULLIF(ct.n_all,0))::numeric(10,6) AS cust_share,
  (COALESCE(se.n_sellers,0) / NULLIF(st.n_all,0))::numeric(10,6) AS seller_share,
  (COALESCE(gs.gmv_s,0) / NULLIF(gt.gmv_all,0))::numeric(10,6) AS gmv_share,
  ((COALESCE(c.n_cust,0) / NULLIF(ct.n_all,0)) - (COALESCE(se.n_sellers,0) / NULLIF(st.n_all,0)))::numeric(10,6) AS desert_index
FROM (
  SELECT state FROM custs
  UNION SELECT state FROM sell
  UNION SELECT state FROM gmv_s
) s
LEFT JOIN custs c ON c.state = s.state
CROSS JOIN cust_tot ct
LEFT JOIN sell se ON se.state = s.state
CROSS JOIN sell_tot st
LEFT JOIN gmv_s gs ON gs.state = s.state
CROSS JOIN gmv_tot gt;

-- 5) Law 3 distribution table
CREATE TABLE IF NOT EXISTS mart.law3_scores (
  ontime_flag int,
  review_score int,
  n int,
  pct numeric(10,6)
);
TRUNCATE mart.law3_scores;
WITH base AS (
  SELECT
    CASE WHEN o.ontime_flag=1 THEN 1 ELSE 0 END AS ontime_flag,
    COALESCE(ROUND(o.review_score)::int, 0) AS review_score
  FROM staging.orders o
),
agg AS (
  SELECT ontime_flag, review_score, COUNT(*) AS n
  FROM base
  GROUP BY ontime_flag, review_score
),
tot AS (SELECT SUM(n) AS n_all FROM agg)
INSERT INTO mart.law3_scores (ontime_flag, review_score, n, pct)
SELECT a.ontime_flag, a.review_score, a.n, (a.n / NULLIF(t.n_all,0))::numeric(10,6)
FROM agg a CROSS JOIN tot t;
