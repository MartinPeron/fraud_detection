# Fraud Detection Pipeline

This project trains a credit-card fraud model and runs it with Airflow so alerts and daily stats go out automatically.

## What Lives Where
- **`train/` notebooks**: prepare data, log experiments in MLflow, and save the `fraud_pipeline` model.
- **`fraud_detection_airflow/`**: Docker setup with Airflow DAGs for live scoring and morning summaries.
- **`dags/inference.py`**: picks the best MLflow run, scores new payments, saves results to S3, and emails a fraud alert if needed.
- **`dags/daily_stats.py`**: reads yesterday’s S3 files, computes totals and fraud rate, then emails a quick report.
- **Settings**: `.env` stores secrets; Airflow connections point to MLflow, AWS, and SMTP.

## Quick Start
1. Fill `.env` with MLflow, AWS, data file, and alert email settings.
2. From `fraud_detection_airflow/`, run `docker compose up airflow_init` then `docker compose up` to launch Airflow.
3. Make sure MLflow has a trained `fraud_pipeline` run in the chosen experiment.
4. Trigger `fraud_detection_dag` for real-time alerts and `daily_fraud_summary_dag` for the daily overview.

## Main Tools
- Apache Airflow (Celery executor, Redis, PostgreSQL) via Docker
- MLflow for experiment tracking and model registry
- scikit-learn fraud model
- AWS S3 for storing scored transactions and machine learning model
