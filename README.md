# GitHub Archive Data Platform

This project implements an end-to-end data platform for ingesting GitHub event data from the GH Archive and transforming it into analytics-ready datasets through a medallion lakehouse architecture.

## What this project does

The pipeline is designed around a practical production workflow:

- Airflow pulls hourly GH Archive event files
- Raw files are landed in AWS S3
- Databricks processes the data through Bronze, Silver, and Gold layers
- The result is a governed, queryable data foundation for downstream analytics

## Architecture overview

```mermaid
flowchart LR
    A[GH Archive API] --> B[Airflow Ingestion DAG]
    B --> C[S3 Raw Zone]
    C --> D[Databricks Bronze]
    D --> E[Databricks Silver]
    E --> F[Databricks Gold]
    F --> G[Analytics Marts]
```

The deployment also uses Kubernetes, Helm, and Git Sync so DAG updates can flow from GitHub into the Airflow environment without rebuilding container images.

The platform is designed for a high-volume event feed: GH Archive publishes large hourly payloads, so the implementation prioritizes incremental processing, checkpointed state, and efficient re-runs over full-table reloads.

## Tech stack

- Python
- Apache Airflow
- Kubernetes with kind and Helm
- AWS S3 and IAM
- Databricks + Apache Spark
- Delta Lake and Unity Catalog

## Repository structure

- [dags](dags): Airflow DAGs for ingestion and orchestration
- [notebooks](notebooks): Databricks notebooks for Bronze, Silver, and Gold processing
- [chart](chart): Helm values and deployment overrides for Airflow
- [k8s](k8s): Kubernetes manifests and persistent volume definitions
- [cicd](cicd): Container build assets for deployment

## Engineering highlights

This project is strongest where it demonstrates real data engineering tradeoffs rather than just stack coverage:

- Fixed an Airflow container permission issue in a kind-based Kubernetes environment by adjusting host-path permissions so the scheduler and DAG processor could write logs.
- Replaced local clock-based file selection with Airflow’s execution context so the pipeline uses deterministic scheduling and avoids race conditions with upstream file publication.
- Added defensive skip handling for delayed or missing GH Archive files, which prevents transient 404s from being treated as hard failures.
- Designed the workflow around 3 medallion layers and 4 downstream analytical marts, with hourly ingestion and a 2-hour offset to align with GH Archive publication timing.
- Implemented checkpointed streaming-style processing so new files can be processed incrementally rather than forcing full reprocessing.
- Used Unity Catalog-managed tables as the foundation for governed data assets, with an eye toward access control, schema enforcement, and lineage as the platform matures.
- Wired the deployment flow around a lightweight container build path in the CI/CD folder so the Airflow image can be rebuilt and loaded into the local cluster as part of the environment setup.

## Quickstart

### Prerequisites

- Docker
- kind
- kubectl
- Helm

### Run locally

The helper script provisions a kind cluster, builds the Airflow image, loads it into the cluster, installs Airflow with Helm, and applies the persistent log-volume manifests needed for local runs.

```bash
git clone https://github.com/karsonfu0902/gh_archive_data_platform.git
cd gh_archive_data_platform
./install_airflow.sh
```

After the cluster is up, open the Airflow UI:

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

Then visit http://localhost:8080.

## Example analytics use case

A typical downstream question for this platform would be:

```sql
SELECT *
FROM mart_top_repos
LIMIT 3;
```

Example output:

| repo_name | count |
| --- | --- |
| otrieu1082/db-backup | 89617 |
| dabsanddollars2024-cpu/basani-data | 85320 |
| ishashwatt/LeetSync | 69160 |



That is the kind of workload this architecture is intended to support: reliable ingestion, governed transformations, and analyst-friendly data products.

## Contact

Built by Karson Fu.
