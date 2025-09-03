import os, sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PG_HOST','localhost'),
        port=int(os.getenv('PG_PORT','5432')),
        dbname=os.getenv('PG_DB','olist'),
        user=os.getenv('PG_USER','olist'),
        password=os.getenv('PG_PASSWORD','olist'),
    )

def run(path):
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    # Strip UTF-8 BOM if present and normalize line endings
    sql = sql.lstrip('\ufeff').replace('\r\n', '\n')

    conn = get_conn()
    try:
        cur = conn.cursor()
        # reasonable safety net so long queries don't hang forever
        cur.execute("SET statement_timeout = '300s';")
        cur.execute(sql)
        conn.commit()
        print(f'Executed {path} ✅')
    finally:
        conn.close()

if __name__ == '__main__':
    run(sys.argv[1])
