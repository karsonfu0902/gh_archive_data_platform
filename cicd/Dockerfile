FROM apache/airflow:3.0.2-python3.11

USER root

RUN rm -f /etc/apt/sources.list.d/mysql.list || true \
    && apt-get update || true \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


USER airflow
WORKDIR /opt/airflow


COPY --chown=airflow:root requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.11.txt"