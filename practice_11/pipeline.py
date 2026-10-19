from azure.storage.blob import ContainerClient
from pathlib import Path
import duckdb
import os

# Keep the real SAS token out of Git.
SOURCE_SAS = "https://<storage_account>.blob.core.windows.net/<container>?<SAS_TOKEN>"

RAW_DIR = Path("raw")
LAKE_DIR = Path("lake")
DATA_DIR = LAKE_DIR / "data"
EXPECTED_FILES = [
    "products.csv",
    "customers_2026_01.csv",
    "customers_2026_02.csv",
    "orders_2026_01.csv",
    "orders_2026_02.csv",
]


def download_source_files():
    RAW_DIR.mkdir(exist_ok=True)
    source = ContainerClient.from_container_url(SOURCE_SAS)
    for name in EXPECTED_FILES:
        target = RAW_DIR / name
        with target.open("wb") as f:
            stream = source.download_blob(name)
            stream.readinto(f)
        print(f"Downloaded: {name}")


def count_data_files():
    if not DATA_DIR.exists():
        return 0
    return sum(1 for p in DATA_DIR.rglob("*") if p.is_file())


def scalar(con, sql):
    return con.sql(sql).fetchone()[0]


def merge_customers(con, source_file):
    con.sql(f"""
    MERGE INTO silver.customers AS t
    USING (
        SELECT DISTINCT ON (customer_id)
               customer_id::INTEGER AS customer_id,
               trim(full_name) AS full_name,
               upper(left(trim(city), 1)) || lower(substr(trim(city), 2)) AS city,
               lower(trim(segment)) AS segment,
               registered_at::DATE AS registered_at
        FROM bronze.customers
        WHERE _source_file = '{source_file}'
    ) AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET
        full_name = s.full_name,
        city = s.city,
        segment = s.segment,
        registered_at = s.registered_at
    WHEN NOT MATCHED THEN INSERT
        (customer_id, full_name, city, segment, registered_at)
        VALUES (s.customer_id, s.full_name, s.city, s.segment, s.registered_at)
    """)


def merge_orders(con, source_file):
    con.sql(f"""
    MERGE INTO silver.orders AS t
    USING (
        SELECT DISTINCT ON (order_id)
               order_id::INTEGER AS order_id,
               customer_id::INTEGER AS customer_id,
               product_id::INTEGER AS product_id,
               order_date::DATE AS order_date,
               quantity::INTEGER AS quantity,
               replace(replace(amount::VARCHAR, ' ', ''), ',', '.')::DECIMAL(10,2) AS amount,
               lower(trim(status)) AS status
        FROM bronze.orders
        WHERE _source_file = '{source_file}'
          AND customer_id::INTEGER IN (SELECT customer_id FROM silver.customers)
    ) AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET
        customer_id = s.customer_id,
        product_id = s.product_id,
        order_date = s.order_date,
        quantity = s.quantity,
        amount = s.amount,
        status = s.status
    WHEN NOT MATCHED THEN INSERT
        (order_id, customer_id, product_id, order_date, quantity, amount, status)
        VALUES (s.order_id, s.customer_id, s.product_id, s.order_date,
                s.quantity, s.amount, s.status)
    """)


def main():
    # Uncomment after inserting the teacher-provided SAS URL.
    # download_source_files()

    missing = [name for name in EXPECTED_FILES if not (RAW_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(
            "Missing raw files: " + ", ".join(missing) +
            ". Put the real SAS URL into SOURCE_SAS and uncomment download_source_files()."
        )

    LAKE_DIR.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.sql("INSTALL ducklake")
    con.sql("LOAD ducklake")
    con.sql("ATTACH 'ducklake:lake/catalog.ducklake' AS lake (DATA_PATH 'lake/data/')")
    con.sql("USE lake")

    for schema in ("bronze", "silver", "gold"):
        con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # BRONZE
    con.sql("""
    CREATE OR REPLACE TABLE bronze.customers AS
    SELECT * EXCLUDE (filename),
           regexp_extract(filename, '[^/]+$') AS _source_file,
           now() AS _ingested_at
    FROM read_csv('raw/customers_*.csv', header = true, filename = true)
    """)
    con.sql("""
    CREATE OR REPLACE TABLE bronze.orders AS
    SELECT * EXCLUDE (filename),
           regexp_extract(filename, '[^/]+$') AS _source_file,
           now() AS _ingested_at
    FROM read_csv('raw/orders_*.csv', header = true, filename = true, all_varchar = true)
    """)
    con.sql("""
    CREATE OR REPLACE TABLE bronze.products AS
    SELECT * EXCLUDE (filename),
           regexp_extract(filename, '[^/]+$') AS _source_file,
           now() AS _ingested_at
    FROM read_csv('raw/products.csv', header = true, filename = true)
    """)

    print("Bronze customers:", scalar(con, "SELECT count(*) FROM bronze.customers"))
    print("Bronze orders:", scalar(con, "SELECT count(*) FROM bronze.orders"))
    print("Bronze products:", scalar(con, "SELECT count(*) FROM bronze.products"))

    # SILVER is rebuilt deterministically, then populated only via MERGE INTO.
    con.sql("""
    CREATE OR REPLACE TABLE silver.customers (
        customer_id INTEGER,
        full_name VARCHAR,
        city VARCHAR,
        segment VARCHAR,
        registered_at DATE
    )
    """)
    con.sql("""
    CREATE OR REPLACE TABLE silver.orders (
        order_id INTEGER,
        customer_id INTEGER,
        product_id INTEGER,
        order_date DATE,
        quantity INTEGER,
        amount DECIMAL(10,2),
        status VARCHAR
    )
    """)

    merge_customers(con, "customers_2026_01.csv")
    merge_orders(con, "orders_2026_01.csv")
    print("After January customers:", scalar(con, "SELECT count(*) FROM silver.customers"))
    print("After January orders:", scalar(con, "SELECT count(*) FROM silver.orders"))

    merge_customers(con, "customers_2026_02.csv")
    version_before_feb_orders = scalar(con, "SELECT max(snapshot_id) FROM lake.snapshots()")
    merge_orders(con, "orders_2026_02.csv")
    print("After February customers:", scalar(con, "SELECT count(*) FROM silver.customers"))
    print("After February orders:", scalar(con, "SELECT count(*) FROM silver.orders"))

    orphans = scalar(con, """
        SELECT count(DISTINCT order_id)
        FROM bronze.orders
        WHERE _source_file = 'orders_2026_02.csv'
          AND customer_id::INTEGER NOT IN (SELECT customer_id FROM silver.customers)
    """)
    print("February orphan orders:", orphans)

    merge_orders(con, "orders_2026_02.csv")
    print("After repeated February MERGE:", scalar(con, "SELECT count(*) FROM silver.orders"))

    print("Distinct cities:", scalar(con, "SELECT count(DISTINCT city) FROM silver.customers"))
    print(con.sql("SELECT status, count(*) AS orders FROM silver.orders GROUP BY status ORDER BY status").df())

    # GOLD
    con.sql("""
    CREATE OR REPLACE TABLE gold.dim_customer AS
    SELECT customer_id AS customer_key, full_name, city, segment
    FROM silver.customers
    """)
    con.sql("""
    CREATE OR REPLACE TABLE gold.dim_product AS
    SELECT product_id::INTEGER AS product_key,
           trim(product_name) AS product_name,
           trim(category) AS category,
           replace(replace(price::VARCHAR, ' ', ''), ',', '.')::DECIMAL(10,2) AS price
    FROM bronze.products
    """)
    con.sql("""
    CREATE OR REPLACE TABLE gold.fact_sales AS
    SELECT order_id,
           customer_id AS customer_key,
           product_id AS product_key,
           order_date,
           date_trunc('month', order_date)::DATE AS order_month,
           quantity,
           amount
    FROM silver.orders
    WHERE status <> 'cancelled'
    """)

    outputs = [
        "gold_revenue_by_city.parquet",
        "gold_revenue_by_category.parquet",
        "gold_revenue_by_month.parquet",
    ]
    for p in outputs:
        Path(p).unlink(missing_ok=True)

    con.sql("""
    COPY (
        SELECT c.city, count(*) AS orders, sum(f.amount) AS revenue
        FROM gold.fact_sales f
        JOIN gold.dim_customer c USING (customer_key)
        GROUP BY c.city
        ORDER BY revenue DESC
    ) TO 'gold_revenue_by_city.parquet' (FORMAT PARQUET)
    """)
    con.sql("""
    COPY (
        SELECT p.category, count(*) AS orders, sum(f.amount) AS revenue
        FROM gold.fact_sales f
        JOIN gold.dim_product p USING (product_key)
        GROUP BY p.category
        ORDER BY revenue DESC
    ) TO 'gold_revenue_by_category.parquet' (FORMAT PARQUET)
    """)
    con.sql("""
    COPY (
        SELECT order_month, count(*) AS orders, sum(amount) AS revenue
        FROM gold.fact_sales
        GROUP BY order_month
        ORDER BY order_month
    ) TO 'gold_revenue_by_month.parquet' (FORMAT PARQUET)
    """)

    print("Gold dim_customer:", scalar(con, "SELECT count(*) FROM gold.dim_customer"))
    print("Gold dim_product:", scalar(con, "SELECT count(*) FROM gold.dim_product"))
    print("Gold fact_sales:", scalar(con, "SELECT count(*) FROM gold.fact_sales"))
    print("Total revenue:", scalar(con, "SELECT sum(amount) FROM gold.fact_sales"))
    print(con.sql("SELECT * FROM read_parquet('gold_revenue_by_city.parquet') LIMIT 3").df())
    print(con.sql("SELECT * FROM read_parquet('gold_revenue_by_category.parquet') LIMIT 1").df())
    print(con.sql("SELECT * FROM read_parquet('gold_revenue_by_month.parquet')").df())

    # VERSIONS / SNAPSHOTS
    snapshots = con.sql("SELECT * FROM lake.snapshots() ORDER BY snapshot_id").df()
    print(snapshots)
    print("Snapshot count:", len(snapshots))
    print("Version before February orders MERGE:", version_before_feb_orders)
    print(
        "silver.orders at that version:",
        scalar(con, f"SELECT count(*) FROM silver.orders AT (VERSION => {version_before_feb_orders})")
    )

    before = count_data_files()
    con.sql("CALL lake.merge_adjacent_files()")
    after = count_data_files()
    print("Lake data files before merge_adjacent_files:", before)
    print("Lake data files after merge_adjacent_files:", after)

    con.close()


if __name__ == "__main__":
    main()
