from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import mlflow
from mlflow import MlflowClient
import mlflow.sklearn
import pandas as pd
from dotenv import load_dotenv
import os
import boto3
from io import StringIO
from datetime import datetime


def print_hello():
    print("Hello")

def do_all():
    load_dotenv()

    mlflow.set_tracking_uri("http://host.docker.internal:5000")
    EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME")
    mlflow.set_experiment(EXPERIMENT_NAME)

    # get all runs
    client = MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.f1_score DESC"],
        max_results=1
    )

    # Find best run
    best_run = runs[0]
    best_run_id = best_run.info.run_id
    best_f1 = best_run.data.metrics.get("f1_score", None)
    print(f"Best f1-score : {best_f1:.4f}")

    # load the model
    model_uri = f"runs:/{best_run_id}/fraud_pipeline"
    model = mlflow.sklearn.load_model(model_uri)

    # simulate real-time data by sampling from test file
    df_raw = pd.read_csv("/Users/martinper/Downloads/fraudTest_real_time.csv")
    new_data = df_raw.sample(1).reset_index()
    expected_result = new_data.at[0, 'is_fraud']
    new_data = new_data.drop(columns=['is_fraud'])

    print(new_data)
    prediction = model.predict(new_data)
    print("Prediction:", prediction)
    print("Expected:", expected_result)


    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    AWS_ARTIFACT_PATH = os.getenv("AWS_ARTIFACT_PATH")
    AWS_DATA_SAVE_PATH = os.getenv("AWS_DATA_SAVE_PATH")

    # save to s3
    s3 = boto3.client("s3")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = AWS_ARTIFACT_PATH + AWS_DATA_SAVE_PATH + f"{best_run_id}_{timestamp}.csv"

    output_df = new_data.copy()
    output_df["prediction"] = prediction

    csv_buffer = StringIO()
    output_df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=AWS_BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())

    print(f"Saved on S3 : s3://{AWS_BUCKET_NAME}/{key}")

with DAG("crypto_dag", start_date=datetime(2022, 6, 1), schedule_interval="@once", catchup=False) as dag:
    
    print_hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )
    
    do_all_task = PythonOperator(
        task_id="do_all",
        python_callable=do_all,
    )

    print_hello_task >> do_all_task
