from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# DQ TASKS 
# define a reusable data-quality task to check row count in any table
@task()
def dq_rowcount(table: str, min_rows: int = 1):
    # initialize Postgres connection
    pg = PostgresHook(postgres_conn_id="postgres_dwh")
    # open DB connection
    with pg.get_conn() as conn:
        # create cursor to run SQL
        with conn.cursor() as cur:
            # run row count query
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            # fetch count result
            (cnt,) = cur.fetchone()

    # print the number of rows found for logging
    print(f"[DQ] {table} → {cnt} rows")

    # fail the task if rows are fewer than expected
    if cnt < min_rows:
        raise ValueError(f"DQ ERROR: {table} has only {cnt} rows")

# define DQ check for null foreign keys in fact_orders
@task()
def dq_no_null_keys():
    # connect to Postgres
    pg = PostgresHook(postgres_conn_id="postgres_dwh")
    # open connection
    with pg.get_conn() as conn:
        # create cursor for SQL execution
        with conn.cursor() as cur:
            # query to find NULL customer_id or product_id
            cur.execute("""
                SELECT COUNT(*) 
                FROM fact_orders
                WHERE customer_id IS NULL OR product_id IS NULL
            """)
            # fetch count of invalid rows
            (bad,) = cur.fetchone()
    # log number of problematic rows
    print(f"[DQ] null foreign keys = {bad}")
    # fail if any null foreign keys exist
    if bad > 0:
        raise ValueError("DQ ERROR: fact_orders contains NULL customer_id or product_id")

# DAG DEFINITION 
# define the standalone data quality DAG
@dag(
    dag_id="shopverse_data_quality_only",  # unique DAG ID
    description="Runs only DQ checks on warehouse tables",  # DAG description
    schedule="0 */6 * * *",  # run every 6 hours
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # starting from Jan 1, 2025
    catchup=False,  # do not backfill past runs
    tags=["shopverse", "data_quality"],  # tags for UI grouping
)
def shopverse_data_quality_only():
    # run rowcount on three tables using dynamic task mapping
    rowcount_checks = dq_rowcount.expand(
        table=["dim_customers", "dim_products", "fact_orders"],
        min_rows=[1, 1, 1],
    )
    # run null key validation after rowcount checks
    null_check = dq_no_null_keys()
    # set task ordering: rowcount  null check
    rowcount_checks >> null_check

# instantiate DAG so Airflow can detect it
dq_dag = shopverse_data_quality_only()
