from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Databricks Job IDs for various tasks in the pipeline
jobs= {
    # Job ID for the Data Ingestion step
    "ingestion": 483018986857595,
    # Job ID for Data Cleaning & Transformation   
    "cleaning": 422910039010084,
    # Job ID for Analytics and Gold Layer Generation    
    "analytics": 781420722412774,   
}

# Default arguments that will be applied to all tasks in the DAG
default_args = {
    "owner": "education_analytics_team",   
    # Number of retries in case of failure
    "retries": 2,
    # Delay between retries                          
    "retry_delay": timedelta(minutes=5),   
}

# Defining the DAG (Directed Acyclic Graph) 
with DAG(
    # Unique identifier for the DAG
    dag_id="school_enrollment_education_pipeline",  
    # Start date of the DAG's first run
    start_date=datetime(2025, 1, 1),  
    # The schedule for the DAG to run (weekly)               
    schedule="@weekly", 
    # Don't backfill missed runs                            
    catchup=False,     
    # Apply the default_args to all tasks                           
    default_args=default_args,   
    # Add tags for categorization                   
    tags=["databricks", "education", "analytics", "serverless"],  
) as dag:

    # Step 1: Data Ingestion-Bronze Layer
    data_ingestion = DatabricksRunNowOperator(
        # Unique task ID
        task_id="data_ingestion",   
        # Databricks connection ID                 
        databricks_conn_id="databricks_conn",  
        # Databricks Job ID for ingestion task       
        job_id=jobs["ingestion"],         
    )

    # Step 2: Data Cleaning & Transformation-Silver Layer
    data_cleaning = DatabricksRunNowOperator(
        # Unique task ID
        task_id="data_cleaning_transformation",   
           # Databricks connection ID    
        databricks_conn_id="databricks_conn",   
        # Databricks Job ID for cleaning task   
        job_id=jobs["cleaning"],           
    )

    # Step 3: Analytics & Gold Layer Generation
    data_analytics = DatabricksRunNowOperator(
        # Unique task ID
        task_id="education_enrollment_analytics",   
        # Databricks connection ID  
        databricks_conn_id="databricks_conn",   
        # Databricks Job ID for analytics task      
        job_id=jobs["analytics"],        
    )

    # Step 4: Power BI Dashboard Refresh 
    # Python function that simulates the refresh of Power BI dashboard.
    def refresh_powerbi_dashboard(**context):
        run_date = context["ds"]  # Get the execution date from the Airflow context
        print(f"Power BI dataset refresh triggered for run date: {run_date}")
    # Define the task to trigger the refresh of Power BI dashboard
    powerbi_refresh = PythonOperator(
         # Unique task ID for Power BI refresh
        task_id="refresh_powerbi_dashboard", 
        # The function that triggers the refresh
        python_callable=refresh_powerbi_dashboard,  
         # Provide context for the task to access execution date
        provide_context=True, 
    )

    # Task Dependency Order: 
    # The tasks will run sequentially in the following order:
    data_ingestion >> data_cleaning >> data_analytics >> powerbi_refresh
