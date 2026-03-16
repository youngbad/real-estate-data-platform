from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Use the absolute path to your project root to ensure Airflow finds the scripts
# In a production environment, you might use an environment variable (e.g. $AIRFLOW_HOME or $PROJECT_HOME)
PROJECT_ROOT = "/Users/jakub/Desktop/real-estate-data-platform"

# Define the DAG
with DAG(
    'real_estate_etl_pipeline',
    default_args=default_args,
    description='A daily ETL pipeline for real estate listings and GUS macro indicators',
    schedule_interval=timedelta(days=1), # Run once a day
    start_date=datetime(2026, 3, 15),
    catchup=False,
    tags=['real_estate', 'etl'],
) as dag:

    # Task 1: Initialize Database (Safe to run multiple times if using SQLAlchemy's create_all with IF NOT EXISTS)
    init_db_task = BashOperator(
        task_id='initialize_database',
        bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=. python3 src/jobs/init_db.py',
    )

    # Task 2: Scrape Data
    scrape_data_task = BashOperator(
        task_id='scrape_data',
        bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=. python3 src/ingestion/scraper.py',
    )

    # Task 3: Transform Data (PySpark)
    transform_data_task = BashOperator(
        task_id='transform_data',
        bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=. python3 src/processing/transformer.py',
    )

    # Task 4: Load Data to PostgreSQL
    load_data_task = BashOperator(
        task_id='load_to_postgres',
        bash_command=f'cd {PROJECT_ROOT} && PYTHONPATH=. python3 src/jobs/load_to_postgres.py',
    )

    # Define the task execution order
    init_db_task >> scrape_data_task >> transform_data_task >> load_data_task
