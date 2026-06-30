import os
import logging
import requests
from datatime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.assets import Asset
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#1 Define the Airflow DAG 3.0 Data Asset destination URL
GH_ARCHIVE_BUCKET = Variable.get("gh_archive_bucket")
gh_archive_asset = Asset(f"{GH_ARCHIVE_BUCKET}/raw/gh_archive")

@dag(
    dag_id="produce_gh_archive_assets",
    schedule="@hourly",
    start_date=datetime(2026, 6, 20),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "gh_archive"]
)
def gh_archive_ingestion_pipeline():

    @task(outlets=[gh_archive_asset])
    def fetch_hourly_gh_events(**context):
        """
        Calculates the logical execution window,
        pulls the gzipped JSON lines from GH archive,
        uploads to S3 bronze bucket.
        """
        
        # Calculate the logical execution window
        logical_time = context["data_interval_start"]

        year = logical_time.strftime("%Y")
        month = logical_time.strftime("%m")
        day = logical_time.strftime("%d")
        hour = logical_time.strftime("%H")

        # Construct GH Archive source URL and S3 key
        filename = f"{year}-{month}-{day}-{hour}.json.gz"
        source_url = f"https://data.gharchive.org/{filename}"
        s3_key = f"raw/gh_archive/{year}/{month}/{day}/{filename}"

        logging.info(f"Fetching GH Archive data from {source_url}")

        # Stream download directly into ephemeral local container worker storage
        local_tmp_path = f"/tmp/{filename}"
        response = requests.get(source_url, stream=True)

        if response.status_code == 200:
            with open(local_tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024*1024):  # 1 MB chunks
                    if chunk:
                        f.write(chunk)
            logging.info(f"Successfully staged {filename} locally")
        else:
            raise RuntimeError(f"GH Archive return code {response.status_code} for {source_url}")
        
        # Initialize the native Airflow Amazon Provider hook
        logging.info(f"Connecting to S3 bucket {GH_ARCHIVE_BUCKET}")
        s3 = S3Hook(aws_conn_id="aws_default")

        s3.load_file(
            filename=local_tmp_path,
            key=s3_key,
            bucket_name=GH_ARCHIVE_BUCKET,
            replace=True
        )
        logging.info(f"Successfully uploaded {filename} to S3 at {s3_key}")

        if os.path.exists(local_tmp_path):
            os.remove(local_tmp_path)
    
    fetch_hourly_gh_events()

gh_archive_ingestion_pipeline()