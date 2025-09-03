import os
import pandas as pd
from db import get_conn

OUTDIR = os.path.join(os.getcwd(), 'outputs')
os.makedirs(OUTDIR, exist_ok=True)

QUERIES = {
    'rfm_scored.csv':     'SELECT * FROM mart.rfm_scored ORDER BY cust_uid',
    'clv_proxy.csv':      'SELECT * FROM mart.clv_proxy ORDER BY cust_uid, as_of_date',
    'churn_features.csv': 'SELECT * FROM mart.churn_features ORDER BY cust_uid',
    'churn_snapshot.csv': 'SELECT * FROM mart.churn_snapshot ORDER BY cust_uid',
}

def main():
    conn = get_conn()
    try:
        for fname, sql in QUERIES.items():
            print(f'-> Exporting {fname} ...')
            df = pd.read_sql_query(sql, conn)
            path = os.path.join(OUTDIR, fname)
            df.to_csv(path, index=False)
            print(f'   Wrote {path} ({len(df):,} rows)')
    finally:
        conn.close()

if __name__ == '__main__':
    main()
