from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable

# Production SLA and retry configurations
default_args = {
    'owner': 'karsonfu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gh_archive_medallion_orchestrator',
    default_args=default_args,
    description='Triggers the Databricks Fan-Out Medallion Pipeline hourly',
    schedule='@hourly',  # Aligns with the GH Archive raw file release frequency
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'databricks', 'lakehouse'],
) as dag:

    # The token and job identity are cleanly decoupled using Airflow connections/variables
    trigger_lakehouse_pipeline = DatabricksRunNowOperator(
        task_id='trigger_databricks_medallion_workflow',
        databricks_conn_id='databricks_default',
        job_id=Variable.get('databricks_job_id'),
    )

    trigger_lakehouse_pipeline