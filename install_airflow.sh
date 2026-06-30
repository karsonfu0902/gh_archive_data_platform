#!/bin/bash
set -e

CLUSTER_NAME="data-platform-cluster"
NAMESPACE="airflow"

kind delete cluster --name ${CLUSTER_NAME} || true
kind create cluster --name ${CLUSTER_NAME}

docker build -t my-dags:0.0.1 -f cicd/Dockerfile .

kind load docker-image my-dags:0.0.1 --name ${CLUSTER_NAME}

helm repo add apache-airflow https://airflow.apache.org
helm repo update

kubectl create namespace ${NAMESPACE} || true

kubectl apply -f k8s/volumes/airflow-logs-pv.yaml
kubectl apply -f k8s/volumes/airflow-logs-pvc.yaml

helm install airflow apache-airflow/airflow \
    --namespace ${NAMESPACE} \
    --values chart/values-override.yaml \
    --debug