from uu import Error
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import mlflow
from mlflow import MlflowClient
import mlflow.sklearn
import pandas as pd
from io import StringIO
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

def load_env(**context):
    env = {
        "DATA_PATH": os.getenv("DATA_PATH"),
        "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI"),
        "MLFLOW_EXPERIMENT_NAME": os.getenv("MLFLOW_EXPERIMENT_NAME"),
        "AWS_BUCKET_NAME": os.getenv("AWS_BUCKET_NAME"),
        "AWS_DATA_SAVE_PATH": os.getenv("AWS_DATA_SAVE_PATH"),
        "ALERT_EMAIL": os.getenv("ALERT_EMAIL"),
    }
    missing = [k for k, v in env.items() if not v]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    context["ti"].xcom_push(key="env", value=env)

def get_best_run(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    mlflow.set_tracking_uri(env["MLFLOW_TRACKING_URI"])
    experiment_name = env["MLFLOW_EXPERIMENT_NAME"]
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if not experiment:
        raise ValueError(f"Experiment '{experiment_name}' not found in MLflow.")
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.f1_score DESC"],
        max_results=1,
    )
    if not runs:
        raise ValueError(f"No runs found for experiment '{experiment_name}'.")
    best_run = runs[0]
    result = {
        "run_id": best_run.info.run_id,
        "best_f1": best_run.data.metrics.get("f1_score", None),
        "experiment_id": experiment.experiment_id,
    }
    context["ti"].xcom_push(key="best_run", value=result)

def load_model(**context):
    best_run = context["ti"].xcom_pull(key="best_run", task_ids="model_branch.get_best_run")
    print(best_run)
    run_id = best_run["run_id"]
    model_uri = f"runs:/{run_id}/fraud_pipeline"
    context["ti"].xcom_push(key="model_uri", value=model_uri)

def retrieve_real_time_payment(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    df_raw = pd.read_csv(env["DATA_PATH"])
    new_data = df_raw.sample(1).reset_index(drop=True)
    expected_result = int(new_data.at[0, "is_fraud"])
    new_data = new_data.drop(columns=["is_fraud"])
    result = {
        "data": new_data.to_dict(orient="records"),
        "expected": expected_result
    }
    context["ti"].xcom_push(key="real_time_payment", value=result)

def predict(**context):
    best_run = context["ti"].xcom_pull(key="best_run", task_ids="model_branch.get_best_run")
    model_uri = context["ti"].xcom_pull(key="model_uri", task_ids="model_branch.load_model")
    payment = context["ti"].xcom_pull(key="real_time_payment", task_ids="retrieve_real_time_payment")
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    aws_conn = BaseHook.get_connection("aws_default")

    os.environ["AWS_ACCESS_KEY_ID"] = aws_conn.login
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_conn.password

    mlflow.set_tracking_uri(env["MLFLOW_TRACKING_URI"])
    model = mlflow.sklearn.load_model(model_uri)
    new_data = pd.DataFrame(payment["data"])
    prediction = model.predict(new_data)
    result = {
        "run_id": best_run["run_id"],
        "expected": payment["expected"],
        "prediction": int(prediction[0]),
        "data": new_data.to_dict(orient="records"),
    }
    context["ti"].xcom_push(key="prediction", value=result)

def save_to_s3(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    pred = context["ti"].xcom_pull(key="prediction", task_ids="predict")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{env['AWS_DATA_SAVE_PATH']}{pred['run_id']}_{timestamp}.csv"
    df = pd.DataFrame(pred["data"])
    df["prediction"] = pred["prediction"]
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    # Load it to S3
    try:
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=key,
            bucket_name=env["AWS_BUCKET_NAME"],
            replace=True,
        )
        print(f"File successfully saved to s3://{env['AWS_BUCKET_NAME']}/{key}")
    except Exception as e:
        print(f"Failed to upload file to S3: {e}")
        raise


def send_alert_email(**context):
    env = context["ti"].xcom_pull(key="env", task_ids="load_env")
    prediction = context["ti"].xcom_pull(key="prediction", task_ids="predict")

    prediction["prediction"] = 1 # to check email is sent properly

    if prediction["prediction"] != 1:
        print("✅ No fraud detected, skipping email.")
        return

    # airflow.utils.email.send_email didn't work so we are sending an email through smtplib instead
    conn = BaseHook.get_connection("smtp_default")
    extras = json.loads(conn.extra or "{}")

    host = conn.host
    port = conn.port or 587
    user = conn.login
    password = conn.password
    use_tls = extras.get("smtp_starttls", True)
    use_ssl = extras.get("smtp_ssl", False)
    recipient = env["ALERT_EMAIL"]

    # ✉️ Prépare le message
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = recipient
    msg["Subject"] = "🚨 Fraudulent Transaction Detected!"
    body = f"""
    <h3>Fraud Alert</h3>
    <p>A potential fraudulent transaction was detected.</p>
    <ul>
        <li><b>Run ID:</b> {prediction['run_id']}</li>
        <li><b>Expected:</b> {prediction['expected']}</li>
        <li><b>Predicted:</b> {prediction['prediction']}</li>
    </ul>
    <pre>{prediction['data']}</pre>
    """
    msg.attach(MIMEText(body, "html"))

    try:
        print(f"📤 Connecting to {host}:{port} (TLS={use_tls}, SSL={use_ssl})")
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port)
        else:
            server = smtplib.SMTP(host, port)
            if use_tls:
                server.starttls()
        server.login(user, password)
        server.send_message(msg)
        server.quit()
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        raise e



with DAG(
    "fraud_detection_dag",
    start_date=datetime(2022, 6, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    load_env_task = PythonOperator(task_id="load_env", python_callable=load_env)
    

    with TaskGroup(group_id="model_branch") as model_branch:
        get_best_run_task = PythonOperator(task_id="get_best_run", python_callable=get_best_run)
        load_model_task = PythonOperator(task_id="load_model", python_callable=load_model)
        get_best_run_task >> load_model_task

    retrieve_real_time_payment_task = PythonOperator(task_id="retrieve_real_time_payment", python_callable=retrieve_real_time_payment)
    predict_task = PythonOperator(task_id="predict", python_callable=predict)
    save_to_s3_task = PythonOperator(task_id="save_to_s3", python_callable=save_to_s3)

    send_alert_email_task = PythonOperator(task_id="send_email_if_fraud", python_callable=send_alert_email)

    load_env_task >> [model_branch, retrieve_real_time_payment_task] >> predict_task >> save_to_s3_task >> send_alert_email_task
