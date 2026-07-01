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

    %% Class Definitions for Visual Accents

    classDef git fill:#24292e,stroke:#fff,stroke-width:1px,color:#fff;

    classDef k8s fill:#326ce5,stroke:#fff,stroke-width:1px,color:#fff;

    classDef aws fill:#ff9900,stroke:#fff,stroke-width:1px,color:#000;

    classDef databricks fill:#ff4f00,stroke:#fff,stroke-width:1px,color:#fff;

    classDef box fill:#f6f8fa,stroke:#d1d5db,stroke-width:1px,color:#000;



    %% PART 3: SOURCE & DEPLOYMENT MANAGEMENT

    subgraph GitHub_SCM ["📁 Part 3: GitHub / SCM Plane"]

        direction LR

        A1["Databricks Notebooks <br> (.py Code)"]:::git

        A2["Airflow DAG Repo <br> & Configs"]:::git

    end



    %% PART 2: LOCAL ORCHESTRATION PLANE

    subgraph Local_Infra ["☸️ Part 2: Local Orchestration (Kubernetes / 'kind')"]

        direction TB

        subgraph Airflow_Core ["Apache Airflow Cluster (CeleryExecutor)"]

            C1["Git-Sync Sidecar <br> Container"]:::box

            C2["Airflow Scheduler & <br> DAG Processor"]:::box

            

            subgraph K8s_Pods ["Ephemeral Worker Pods"]

                D1["Ingestion Task <br> (HTTP Ingest)"]:::k8s

                D2["Trigger Task <br> (Databricks Operator)"]:::k8s

            end

        end

    end



    %% PART 1: CLOUD DATA PLANE

    subgraph Cloud_Data ["❄️ Part 1: AWS & Databricks Cloud Plane"]

        direction TB

        E1[("AWS S3 Bucket <br> (Raw JSON Landing Point)")]:::aws

        

        subgraph Databricks_Lakehouse ["Databricks Workspace (Spark Compute)"]

            F1["Bronze Tier <br> (Auto Loader Stream)"]:::databricks

            F2["Silver Tier <br> (Deduplicated Enriched)"]:::databricks

            F3["Gold Tier <br> (Analytical Marts)"]:::databricks

        end

        G1[["Unity Catalog <br> (gh_archive_lakehouse_dev)"]]:::box

    end



    %% PIPELINE INTERACTIONS & CONNECTIONS

    %% Development / Deployment Cycle

    A1 -->|Syncs Code Changes| Databricks_Lakehouse

    A2 -->|Live Code Poll| C1

    C1 -->|Hot-Reloads Volume| C2

    C2 -->|Spawns Pod Tasks| K8s_Pods



    %% Live Pipeline Execution Flow

    D1 -->|1. Fetch & Stream Raw JSON| E1

    D2 -->|2. Trigger Workflow API Call| Databricks_Lakehouse

    E1 -->|3. Read Incremental Checkpoints| F1

    F1 --> F2 --> F3

    G1 -.->|Governs Tables & Access| Databricks_Lakehouse
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
- Designed the workflow around 3 medallion layers and 4 downstream analytical mart tables, with hourly ingestion and a 2-hour offset to align with GH Archive publication timing.
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
