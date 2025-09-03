import os
import pandas as pd

# Try to reuse your existing db.get_conn(); fall back to env/psycopg2 if needed.
def get_conn():
    try:
        from db import get_conn as _gc
        return _gc()
    except Exception:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv()
        import os as _os
        return psycopg2.connect(
            host=_os.getenv('PG_HOST', 'localhost'),
            port=int(_os.getenv('PG_PORT', '5432')),
            dbname=_os.getenv('PG_DB', 'olist'),
            user=_os.getenv('PG_USER', 'olist'),
            password=_os.getenv('PG_PASSWORD', 'olist'),
        )

OUTDIR = os.path.join('outputs', 'bi_exports')
os.makedirs(OUTDIR, exist_ok=True)

def q(sql):
    conn = get_conn()
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()

def export_core():
    print('-> bi_orders_daily.csv')
    q('SELECT d, orders, gmv, ontime_rate, late_rate, avg_review, heavy_bulky_share FROM mart.bi_orders_daily ORDER BY d;'
     ).to_csv(os.path.join(OUTDIR, 'bi_orders_daily.csv'), index=False)

    print('-> bi_seller_pareto.csv')
    q('SELECT seller_id, state, gmv, rank, cumulative_share FROM mart.seller_pareto ORDER BY rank;'
     ).to_csv(os.path.join(OUTDIR, 'bi_seller_pareto.csv'), index=False)

    print('-> bi_geo_state.csv')
    q('SELECT state, cust_share, seller_share, gmv_share, desert_index FROM mart.geo_state ORDER BY state;'
     ).to_csv(os.path.join(OUTDIR, 'bi_geo_state.csv'), index=False)

    print('-> bi_law3_scores.csv')
    q('SELECT ontime_flag, review_score, n, pct FROM mart.law3_scores ORDER BY ontime_flag, review_score;'
     ).to_csv(os.path.join(OUTDIR, 'bi_law3_scores.csv'), index=False)

def export_rfm_customers():
    print('-> bi_rfm_customers.csv')
    rfm = q('SELECT cust_uid, r, f, m, rfm_sum FROM mart.rfm_scored ORDER BY cust_uid;')
    snap = q('SELECT cust_uid, avg_order_value AS aov, ontime_rate, heavy_bulky_share FROM mart.churn_snapshot;')

    df = rfm.merge(snap, on='cust_uid', how='left')

    # Attach churn_prob from local predictions (optional)
    preds_path = os.path.join('outputs', 'churn_predictions_snapshot.csv')
    if os.path.exists(preds_path):
        preds = pd.read_csv(preds_path, usecols=['cust_uid','churn_prob'])
        preds = preds.sort_values(['cust_uid','churn_prob'], ascending=[True, False]).drop_duplicates('cust_uid')
        df = df.merge(preds, on='cust_uid', how='left')
    else:
        df['churn_prob'] = None

    cols = ['cust_uid','r','f','m','rfm_sum','aov','ontime_rate','heavy_bulky_share','churn_prob']
    df[cols].to_csv(os.path.join(OUTDIR, 'bi_rfm_customers.csv'), index=False)

def main():
    export_core()
    export_rfm_customers()
    print(f'All BI CSVs written to {OUTDIR}')

if __name__ == '__main__':
    main()
