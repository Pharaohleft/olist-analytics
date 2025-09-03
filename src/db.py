import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "olist"),
        user=os.getenv("PG_USER", "olist"),
        password=os.getenv("PG_PASSWORD", "olist"),
    )
