from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def load_env(**context):
    env = {
        "AWS_BUCKET_NAME": os.getenv("AWS_BUCKET_NAME"),
        "AWS_DATA_SAVE_PATH": os.getenv("AWS_DATA_SAVE_PATH"),
        "ALERT_EMAIL": os.getenv("ALERT_EMAIL"),
    }
    missing = [k for k, v in env.items() if not v]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    context["ti"].xcom_push(key="env", value=env)

def list_s3_files(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    s3 = S3Hook(aws_conn_id="aws_default")
    files = s3.list_keys(bucket_name=env["AWS_BUCKET_NAME"], prefix=env["AWS_DATA_SAVE_PATH"])
    if not files:
        raise ValueError("No files found in S3 path")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    target_files = [f for f in files if yesterday in f]
    context["ti"].xcom_push(key="files", value=target_files)

def aggregate_data(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    files = context["ti"].xcom_pull(key="files", task_ids="list_s3_files")
    if not files:
        raise ValueError("No files found for yesterday")
    s3 = S3Hook(aws_conn_id="aws_default")
    dfs = []
    for key in files:
        data = s3.read_key(key, bucket_name=env["AWS_BUCKET_NAME"])
        df = pd.read_csv(StringIO(data))
        if "expected" in df.columns:
            df = df.drop(columns=["expected"])
        dfs.append(df)
    df_all = pd.concat(dfs, ignore_index=True)
    context["ti"].xcom_push(key="data", value=df_all.to_json())

def compute_stats(**context):
    data_json = context["ti"].xcom_pull(key="data", task_ids="aggregate_data")
    df = pd.read_json(data_json)
    total = len(df)
    frauds = df[df["prediction"] == 1]
    fraud_count = len(frauds)
    fraud_rate = (fraud_count / total * 100) if total > 0 else 0
    stats = {
        "total": total,
        "frauds": fraud_count,
        "fraud_rate": round(fraud_rate, 2),
        "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
    }
    context["ti"].xcom_push(key="stats", value=stats)

def send_summary_email(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    stats = context["ti"].xcom_pull(key="stats", task_ids="compute_stats")
    conn = BaseHook.get_connection("smtp_default")
    extras = json.loads(conn.extra or "{}")
    host = conn.host
    port = conn.port or 587
    user = conn.login
    password = conn.password
    use_tls = extras.get("smtp_starttls", True)
    use_ssl = extras.get("smtp_ssl", False)
    recipient = env["ALERT_EMAIL"]
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = recipient
    msg["Subject"] = f"📊 Daily Fraud Summary - {stats['date']}"
    body = f"""
    <h3>Fraud Detection Summary - {stats['date']}</h3>
    <ul>
        <li>Total transactions: {stats['total']}</li>
        <li>Frauds detected: {stats['frauds']}</li>
        <li>Fraud rate: {stats['fraud_rate']}%</li>
    </ul>
    """
    msg.attach(MIMEText(body, "html"))
    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port)
        else:
            server = smtplib.SMTP(host, port)
            if use_tls:
                server.starttls()
        server.login(user, password)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        raise e

with DAG(
    "daily_fraud_summary_dag",
    start_date=datetime(2022, 6, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    load_env_task = PythonOperator(task_id="load_env", python_callable=load_env)
    list_s3_files_task = PythonOperator(task_id="list_s3_files", python_callable=list_s3_files)
    aggregate_data_task = PythonOperator(task_id="aggregate_data", python_callable=aggregate_data)
    compute_stats_task = PythonOperator(task_id="compute_stats", python_callable=compute_stats)
    send_summary_email_task = PythonOperator(task_id="send_summary_email", python_callable=send_summary_email)

    load_env_task >> list_s3_files_task >> aggregate_data_task >> compute_stats_task >> send_summary_email_task
