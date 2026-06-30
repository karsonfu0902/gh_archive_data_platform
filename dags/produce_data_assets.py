import logging
import os
from datetime import datetime, timezone, timedelta

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.asset import Asset

# Airflow 3.0 style asset definition for the GH Archive raw data.
GH_ARCHIVE_BUCKET = Variable.get("gh_archive_bucket")
gh_archive_asset = Asset(f"{GH_ARCHIVE_BUCKET}/raw/gh_archive")


@dag(
    dag_id="produce_gh_archive_assets",
    schedule="@hourly",
    start_date=datetime(2026, 6, 20, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "gh_archive"],
)
def gh_archive_ingestion_pipeline():
    @task(
            outlets=[gh_archive_asset],
            task_id="fetch_hourly_gh_events",
            retries=3,
            retry_delay=timedelta(minutes=10),
    )
    def fetch_hourly_gh_events(**context):
        """
        Calculate the logical execution window, download the GH Archive file,
        and upload it to the configured S3 bronze location.
        """

        logical_time = context["data_interval_start"]
        target_time = logical_time - timedelta(hours=2) 
        year = target_time.strftime("%Y")
        month = target_time.strftime("%m")
        day = target_time.strftime("%d")
        hour = str(target_time.hour)

        filename = f"{year}-{month}-{day}-{hour}.json.gz"
        source_url = f"https://data.gharchive.org/{filename}"
        s3_key = f"raw/gh_archive/{year}/{month}/{day}/{filename}"
        local_tmp_path = f"/tmp/{filename}"

        logging.info("Fetching GH Archive data from %s", source_url)

        try:
            response = requests.get(source_url, stream=True, timeout=60)
            response.raise_for_status()

            with open(local_tmp_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

            logging.info("Successfully staged %s locally", filename)

            logging.info("Connecting to S3 bucket %s", GH_ARCHIVE_BUCKET)
            s3 = S3Hook(aws_conn_id="aws_default")
            s3.load_file(
                filename=local_tmp_path,
                key=s3_key,
                bucket_name=GH_ARCHIVE_BUCKET,
                replace=True,
            )
            logging.info("Successfully uploaded %s to S3 at %s", filename, s3_key)
        finally:
            if os.path.exists(local_tmp_path):
                os.remove(local_tmp_path)

    fetch_hourly_gh_events()


gh_archive_ingestion_pipeline()