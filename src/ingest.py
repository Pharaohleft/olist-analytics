# src/ingest.py
import os, re
import pandas as pd
from db import get_conn

DATA_CLEAN = os.path.join(os.getcwd(), "data_clean")

FILE_WHITELIST = {
    "features_orders.csv",
    "order_items_clean.csv",
    "products_clean.csv",
    "order_reviews_dedup.csv",
    "payments_order_agg.csv",
}

# Force specific types for known columns (lowercased names)
COLUMN_TYPE_OVERRIDES = {
    # booleans
    "delivered_late": "BOOLEAN",
    "late_flag": "BOOLEAN",
    "ontime_flag": "BOOLEAN",
    "is_delivered": "BOOLEAN",
    "is_heavy_bulky": "BOOLEAN",

    # ids / categorical text
    "order_id": "TEXT",
    "customer_id": "TEXT",
    "customer_unique_id": "TEXT",
    "cust_uid": "TEXT",
    "seller_id": "TEXT",
    "product_id": "TEXT",
    "product_category_name": "TEXT",
    "payment_type": "TEXT",
    "customer_state": "TEXT",
    "seller_state": "TEXT",

    # numeric that may appear as "1.0"
    "n_items": "NUMERIC(18,6)",
}

DATE_HINT_PAT = re.compile(r"[-/T:]", re.I)
BOOLEAN_TOKENS = {"true","false","0","1","yes","no","t","f","y","n"}

def infer_pg_type(series: pd.Series, col_name: str = "") -> str:
    s = series.dropna()
    if s.empty:
        return "TEXT"

    cname = (col_name or "").strip().lower()
    if cname in COLUMN_TYPE_OVERRIDES:
        return COLUMN_TYPE_OVERRIDES[cname]

    # BOOLEAN first
    vals = [str(v).strip() for v in s.head(10000).tolist()]
    vals_nb = [v for v in vals if v != ""]
    lows = {v.lower() for v in vals_nb}
    name_hints_bool = any(h in cname for h in ["_flag","is_","has_","delivered_late","late_flag","ontime"])
    if (len(vals_nb) > 0 and all(v in BOOLEAN_TOKENS for v in lows)) or (name_hints_bool and lows <= BOOLEAN_TOKENS):
        return "BOOLEAN"

    # NUMERIC: if any decimal/exponent in raw strings, use DECIMAL
    looks_decimal = any(("." in v) or ("e" in v.lower()) for v in vals_nb[:2000])
    try:
        nums = pd.to_numeric(s, errors="raise")
        if looks_decimal:
            return "NUMERIC(18,6)"
        if (nums.dropna() % 1 == 0).all():
            return "BIGINT"
        return "NUMERIC(18,6)"
    except Exception:
        pass

    # TIMESTAMP only if strings look like dates
    head = s.astype(str).head(50)
    if any(DATE_HINT_PAT.search(v) for v in head):
        try:
            pd.to_datetime(s.head(10000), errors="raise")
            return "TIMESTAMP"
        except Exception:
            pass

    return "TEXT"

def sanitize_table_name(fname: str) -> str:
    name = re.sub(r"\.csv$", "", fname, flags=re.I).lower()
    return re.sub(r"[^a-z0-9_]", "_", name)

def ensure_schemas(cur):
    sql = r"""
    DO $$
    DECLARE u text := current_user;
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'raw') THEN
        EXECUTE format('CREATE SCHEMA raw AUTHORIZATION %I', u);
      END IF;
      IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'staging') THEN
        EXECUTE format('CREATE SCHEMA staging AUTHORIZATION %I', u);
      END IF;
      IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'mart') THEN
        EXECUTE format('CREATE SCHEMA mart AUTHORIZATION %I', u);
      END IF;
      EXECUTE 'GRANT USAGE, CREATE ON SCHEMA raw TO ' || quote_ident(u);
      EXECUTE 'GRANT USAGE, CREATE ON SCHEMA staging TO ' || quote_ident(u);
      EXECUTE 'GRANT USAGE, CREATE ON SCHEMA mart TO ' || quote_ident(u);
    END$$;
    """
    cur.execute(sql)

def table_exists(cur, schema, table):
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
    """, (schema, table))
    return cur.fetchone() is not None

def table_has_rows(cur, schema, table):
    cur.execute(f'SELECT 1 FROM {schema}."{table}" LIMIT 1;')
    return cur.fetchone() is not None

def create_table(cur, schema, table, df: pd.DataFrame):
    cols_sql = []
    print(f"   Inferring column types for {schema}.{table} ...")
    for col in df.columns:
        pg_type = infer_pg_type(df[col], col_name=col)
        safe_col = re.sub(r"[^a-zA-Z0-9_]", "_", col).lower()
        cols_sql.append(f'"{safe_col}" {pg_type}')
        print(f"     - {safe_col} -> {pg_type}")
    cur.execute(f'CREATE TABLE IF NOT EXISTS {schema}."{table}" ({", ".join(cols_sql)});')

def copy_csv(cur, schema, table, path):
    with open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            sql=f'''COPY {schema}."{table}"
                    FROM STDIN
                    WITH (FORMAT csv, HEADER true, NULL '', QUOTE '\"')''',
            file=f,
        )

def main():
    if not os.path.isdir(DATA_CLEAN):
        raise SystemExit("data_clean/ not found. Put your cleaned CSVs there.")

    all_csvs = [f for f in os.listdir(DATA_CLEAN) if f.lower().endswith(".csv")]
    files = [f for f in all_csvs if f.lower() in {x.lower() for x in FILE_WHITELIST}]

    missing = sorted(list({x.lower() for x in FILE_WHITELIST} - {f.lower() for f in all_csvs}))
    if missing:
        print("⚠️  Missing required CSVs in data_clean/:")
        for m in missing: print("   -", m)

    conn = get_conn(); conn.autocommit = False
    try:
        with conn.cursor() as cur:
            ensure_schemas(cur); conn.commit()

            if not files:
                raise SystemExit("No whitelisted CSVs found to load. Add your clean files to data_clean/.")

            for fname in files:
                path = os.path.join(DATA_CLEAN, fname)
                table = sanitize_table_name(fname)
                print(f"-> {fname} -> raw.{table}")

                df_sample = pd.read_csv(path, nrows=2000)

                if not table_exists(cur, "raw", table):
                    create_table(cur, "raw", table, df_sample); conn.commit()

                if table_has_rows(cur, "raw", table):
                    print(f"   Skip: raw.{table} already has rows"); continue

                copy_csv(cur, "raw", table, path); conn.commit()
                print(f"   Loaded {fname} ✅")

            ignored = sorted(set(all_csvs) - set(files))
            if ignored:
                print("ℹ️  Ignored non-pipeline CSVs:", ", ".join(ignored))
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        import checks; checks.run_all(); print("Data checks OK ✅")
    except Exception as e:
        print("⚠️ Checks warning:", e)
    main()
    print("Ingest complete ✅")

