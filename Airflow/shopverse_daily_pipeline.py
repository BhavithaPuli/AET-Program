# import os for file path handling
import os  
# import timedelta for retry delays
from datetime import timedelta 
import pendulum
import pandas as pd
from airflow.decorators import dag, task  
from airflow.models import Variable  
from airflow.utils.task_group import TaskGroup  
from airflow.operators.empty import EmptyOperator  
from airflow.operators.email import EmailOperator  
from airflow.providers.postgres.hooks.postgres import PostgresHook  
from airflow.operators.python import PythonOperator  
from airflow.sensors.filesystem import FileSensor  
# helper function that generates sensor file paths dynamically using logical date
def _sensor_path(file_type: str) -> str:
    base = Variable.get("shopverse_data_base_path")  # get base data folder from variable
    ext = "json" if file_type == "orders" else "csv"  # choose file extension based on type
    return f"{base}/landing/{file_type}/{file_type}_{{{{ ds_nodash }}}}.{ext}"  # return full path template
# function that loads CSV/JSON into staging tables
def _load_staging_table(file_type: str, data_interval_start=None, **kwargs):
    # if logical date not passed, read it from Airflow context
    if data_interval_start is None:
        data_interval_start = kwargs["data_interval_start"]
    # format logical date
    ds_nodash = data_interval_start.strftime("%Y%m%d")  
    # connect to postgres
    pg = PostgresHook(postgres_conn_id="postgres_dwh")  
    # create SQLAlchemy engine
    engine = pg.get_sqlalchemy_engine()  
    base_path = Variable.get("shopverse_data_base_path")  
    # choose extension based on file type
    ext = "json" if file_type == "orders" else "csv" 
     # full file path
    path = f"{base_path}/landing/{file_type}/{file_type}_{ds_nodash}.{ext}" 
    # log file path
    print(f"[Staging] Loading {file_type} from: {path}")  
    # load file into DataFrame
    df = pd.read_json(path) if file_type == "orders" else pd.read_csv(path)  
    # staging table name
    table_name = f"stg_{file_type}"  
    with engine.begin() as conn:  
         # clear staging table before loading
        conn.execute(f"TRUNCATE TABLE {table_name};") 
        # load data into staging table
    df.to_sql(table_name, engine, index=False, if_exists="append")  
     # log row count
    print(f"[Staging] Loaded {len(df)} rows → {table_name}") 

# function that builds warehouse tables
def _build_warehouse():
    # connect to database
    pg = PostgresHook(postgres_conn_id="postgres_dwh")  
    # get engine
    engine = pg.get_sqlalchemy_engine()  
    # run warehouse SQL inside transaction
    with engine.begin() as conn:  

        conn.execute("""  # refresh dimension table
            TRUNCATE TABLE dim_customers;
            INSERT INTO dim_customers
            SELECT DISTINCT customer_id, first_name, last_name, email, signup_date, country
            FROM stg_customers;
        """)

        conn.execute("""  # refresh product dimension
            TRUNCATE TABLE dim_products;
            INSERT INTO dim_products
            SELECT DISTINCT product_id, product_name, category, unit_price
            FROM stg_products;
        """)

        conn.execute("""  # rebuild fact_orders
            TRUNCATE TABLE fact_orders;

            INSERT INTO fact_orders(
                order_id, order_timestamp_utc, customer_id, product_id,
                quantity, total_amount, currency_mismatch_flag, status
            )
            SELECT
                o.order_id,
                o.order_timestamp AT TIME ZONE 'UTC',
                o.customer_id,
                o.product_id,
                o.quantity,
                o.total_amount,
                CASE WHEN o.currency <> 'USD' THEN 1 ELSE 0 END,
                o.status
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY order_timestamp DESC
                ) AS rn
                FROM stg_orders
                WHERE quantity > 0
                  AND customer_id IS NOT NULL
                  AND product_id IS NOT NULL
            ) o
            WHERE rn = 1;
        """)
    # log final status
    print("[Warehouse] Build complete.")  

# default arguments for DAG tasks
default_args = {
    "owner": "data_engineer", 
    "email": ["bhavithapuli21@gmail.com"],  # email for alerts
    "email_on_failure": True,  # send email when tasks fail
    "email_on_retry": False,  # do not email when retrying
    "retries": 1,  # allow one retry
    "retry_delay": timedelta(minutes=5),  # retry after 5 minutes
}

# define DAG using decorator
@dag(
    # DAG name
    dag_id="shopverse_daily_pipeline",  
    # description
    description="Daily Shopverse ETL Pipeline",  
    # run daily at 1 AM UTC
    schedule="0 1 * * *",  
    # DAG start date
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # enable backfilling
    catchup=True, 
    # allow only one run at a time
    max_active_runs=1,  
    default_args=default_args,  
)
def shopverse_daily_pipeline():
    # starting node
    start = EmptyOperator(task_id="start")  
    # file sensors for customers file
    wait_customers = FileSensor(
        task_id="wait_customers_file",
        filepath=_sensor_path("customers"),
        poke_interval=30,  # check every 30 sec
        timeout=3600,  # fail after 1 hour
    )

    # file sensor for products file
    wait_products = FileSensor(
        task_id="wait_products_file",
        filepath=_sensor_path("products"),
        poke_interval=30,
        timeout=3600,
    )

    # file sensor for orders file
    wait_orders = FileSensor(
        task_id="wait_orders_file",
        filepath=_sensor_path("orders"),
        poke_interval=30,
        timeout=3600,
    )

    # staging group for ETL staging tasks
    with TaskGroup("staging") as staging_group:

        load_customers = PythonOperator(
            task_id="load_stg_customers",
            python_callable=_load_staging_table,
            op_kwargs={"file_type": "customers"},  # send file type argument
        )

        load_products = PythonOperator(
            task_id="load_stg_products",
            python_callable=_load_staging_table,
            op_kwargs={"file_type": "products"},
        )

        load_orders = PythonOperator(
            task_id="load_stg_orders",
            python_callable=_load_staging_table,
            op_kwargs={"file_type": "orders"},
        )

        wait_customers >> load_customers  # sensor to load
        wait_products >> load_products  # sensor to load
        wait_orders >> load_orders  # sensor to load

    # warehouse group
    with TaskGroup("warehouse") as warehouse_group:
        build = PythonOperator(
            task_id="build_warehouse",
             # build warehouse function
            python_callable=_build_warehouse, 
        )

    # simple row count check function
    @task()
    def dq_rowcount(table: str, min_rows: int = 1):
        pg = PostgresHook(postgres_conn_id="postgres_dwh")  # connect
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table};")  # count rows
                (cnt,) = cur.fetchone()
        if cnt < min_rows:  # fail if too few rows
            raise ValueError(f"{table} has insufficient rows")

    # check for null foreign keys
    @task()
    def dq_no_null_keys():
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM fact_orders
                    WHERE customer_id IS NULL OR product_id IS NULL
                """)
                (bad,) = cur.fetchone()
        if bad > 0:
            raise ValueError("NULL keys found in fact_orders")

    dq_checks = dq_rowcount.expand(  # dynamic mapping for multiple tables
        table=["dim_customers", "dim_products", "fact_orders"],
        min_rows=[1, 1, 1],
    )

    null_check = dq_no_null_keys()  # run null check

    # compute number of orders
    @task()
    def compute_orders():
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM fact_orders;")  # count orders
                (cnt,) = cur.fetchone()
        return cnt

    # branching based on row count
    @task.branch()
    def branch(order_count: int):
        # read threshold
        threshold = int(Variable.get("shopverse_min_order_threshold", 10)) 
        # return branch task id
        return "warn_low_volume" if order_count < threshold else "normal_completion"  
    # compute count
    order_count = compute_orders()  
    # apply branch logic
    branch_task = branch(order_count) 
    # low volume branch
    warn = EmptyOperator(task_id="warn_low_volume") 
    # normal finish
    ok = EmptyOperator(task_id="normal_completion")  

    # email notification for failures
    notify_fail = EmailOperator(
        task_id="notify_failure",
        to="bhavithapuli21@gmail.com",
        subject="Shopverse ETL FAILED",
        html_content="<h3>Your ETL Pipeline Failed — Check Airflow</h3>",
        trigger_rule="one_failed",  # trigger when something fails
    )
    # final task
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success") 
    # begin flow
    start >> [wait_customers, wait_products, wait_orders]  
    # staging warehouse
    staging_group >> warehouse_group 
    # quality checks
    warehouse_group >> dq_checks >> null_check >> order_count >> branch_task 
    # branch flow to end
    branch_task >> [warn, ok] >> end  

    [  # failure handlers
        staging_group, warehouse_group,
        dq_checks, null_check,
        order_count, branch_task
    ] >> notify_fail

# instantiate DAG
dag = shopverse_daily_pipeline()
