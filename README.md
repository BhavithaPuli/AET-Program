README.md
Overview
This project implements a daily ETL pipeline in Apache Airflow to ingest customer, product, and order data into a Postgres data warehouse. The workflow uses modern Airflow features such as TaskFlow API, Sensors, TaskGroups, dynamic tasks, and data quality validations to ensure reliable and scalable execution.
---
1. Setting Up Airflow Variables and Connections
Airflow Variables
To avoid hard-coding configuration values, the DAG uses Airflow Variables. Configure the following in **Admin → Variables**:
* `input_base_path` – Base folder path where the daily input files will be placed.
* `min_row_threshold` – Minimum acceptable row count for data quality checks.
* `file_wait_timeout` – Timeout value for file sensors (in seconds).
Airflow Connections
Create a Postgres connection named `postgres_dwh` in Admin → Connections:
* Conn Id: `postgres_dwh`
* Conn Type:`Postgres`
* Host: `postgres` (default for Docker)
* Schema: `dwh`
* Login:`airflow`
* Password: `airflow`
* Port: `5432`
This enables secure, parameterized access to the data warehouse.
---
2. Placing Input Files
The DAG expects one file per dataset per day:
* `customers_YYYYMMDD.csv`
* `products_YYYYMMDD.csv`
* `orders_YYYYMMDD.json`
Place all input files into the directory specified by `input_base_path` (typically `/opt/airflow/data/input/`).
FileSensors in the DAG will detect the presence of these files based on the execution date.
Example folder structure:
```
dags/
│
├── shopverse_daily_pipeline.py
└── data/
    └── input/
        ├── customers_20251205.csv
        ├── products_20251205.csv
        └── orders_20251205.json
```


3. Triggering and Backfilling the DAG
Triggering from the UI
1. Open the Airflow web UI
2. Navigate to the DAG: *shopverse_daily_pipeline*
3. Click **Trigger DAG** and select your execution date
Triggering from CLI
```
docker exec -it airflow-airflow-scheduler-1 \ airflow dags trigger shopverse_daily_pipeline
```
With a specific date:
```
airflow dags trigger shopverse_daily_pipeline --exec-date 2025-12-05
```
Backfilling Historical Data
To process past dates:
```
docker exec -it airflow-airflow-scheduler-1 \ airflow dags backfill shopverse_daily_pipeline \ -s 2025-12-01 -e 2025-12-07
```
Ensure that all input files for the backfill date range are present in the input directory.
4. Data Quality Checks
The pipeline includes several safeguards to ensure reliable data ingestion:

* Row Count Validation:Ensures each staging table contains at least the minimum required number of records. Prevents empty or incomplete loads.
* Null Foreign Key Checks:Validates that `customer_id` and `product_id` fields in the orders dataset are non-null and consistent. Ensures referential integrity before loading into fact tables.
* Schema Verification:Confirms that all expected columns exist in each input file. Prevents schema drift or malformed data from breaking the pipeline.
* Failure Notifications:Optional email or Slack alerts are triggered when any DQ test fails, enabling quick detection and remediation.

--
If you'd like, I can also generate a **diagram**, **architecture section**, or a **more academic-style README**.
