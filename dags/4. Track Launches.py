import json
from datetime import datetime

import pandas as pd
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_postgres import BigQueryToPostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor


def check_launches_today(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='api_get_launches', key='return_value')

    if json_data:
        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        if isinstance(json_data, dict) and "count" in json_data:
            if json_data["count"] > 0:
                print("Count is greater than 0.")
                return True
            else:
                raise AirflowSkipException("Count is 0 or less.")
        else:
            raise AirflowException("Invalid JSON.")
    else:
        raise AirflowException("No data found in XCom.")


def store_launches_data(**context):
    ti = context['ti']
    launch_data = json.loads(ti.xcom_pull(task_ids='api_get_launches', key='return_value'))
    if launch_data is None:
        raise ValueError("No launch data found in XCom")

    date = context['data_interval_start']
    formatted_date = date.strftime("%Y-%m-%d")

    launch_df = pd.DataFrame([{
        "date": formatted_date,
        "rocket_id": launch["rocket"]["id"],
        "rocket_name": launch["rocket"]["configuration"]["name"],
        "mission_name": launch["mission"]["name"],
        "launch_status": launch["status"]["name"],
        "country": launch["pad"]["country"]["name"],
        "launch_service_provider_name": launch["launch_service_provider"]["name"],
        "launch_service_provider_type": launch["launch_service_provider"]["type"]["name"]
    } for launch in launch_data["results"]])

    launch_df.to_parquet(f"/tmp/launches_{formatted_date}.parquet")


with DAG(
        dag_id="04_track_launches",
        start_date=datetime(year=2025, month=3, day=2),
        end_date=None,
        schedule="@daily",
):
    api_check_up = HttpSensor(
        task_id="api_check_up",
        http_conn_id="thespacedevs_dev",
        endpoint="",
        mode="poke",
    )

    api_get_launches = HttpOperator(
        task_id="api_get_launches",
        http_conn_id="thespacedevs_dev",
        method="GET",
        endpoint="",
        data={
            "window_start__gte": "{{ data_interval_start | ds }}",
            "window_end__lte": "{{ data_interval_end | ds }}"
        },
    )

    have_launches_today = PythonOperator(
        task_id="have_launches_today",
        python_callable=check_launches_today,
    )

    filter_launches_data = PythonOperator(
        task_id="filter_launches_data",
        python_callable=store_launches_data,
    )

    save_to_gcs = LocalFilesystemToGCSOperator(
        task_id="save_to_gcs",
        gcp_conn_id="new_google_connection",
        src="/tmp/launches_{{ data_interval_start | ds }}.parquet",
        dst="idb/",
        bucket="aflow-training-bol-2024-03-12",
    )

    save_to_bq = GCSToBigQueryOperator(
        task_id="save_to_bq",
        gcp_conn_id="new_google_connection",
        bucket="aflow-training-bol-2024-03-12",
        source_objects=["idb/launches_{{ data_interval_start | ds }}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="aflow-training-bol-2024-03-12.aflow_training_bol_2024_03_12.idb_table",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
    )

    create_launches_table = SQLExecuteQueryOperator(
        task_id="create_launches_table",
        sql="""
            CREATE TABLE IF NOT EXISTS launches (
            date DATE NOT NULL,
            rocket_id INT NOT NULL,
            rocket_name TEXT NOT NULL,
            mission_name TEXT NOT NULL,
            launch_status TEXT NOT NULL,
            country TEXT NOT NULL,
            launch_service_provider_name TEXT NOT NULL,
            launch_service_provider_type TEXT NOT NULL);
          """,
        conn_id="mypostgres",
    )

    bigquery_to_pg = BigQueryToPostgresOperator(
        task_id="bigquery_to_pg",
        postgres_conn_id="mypostgres",
        gcp_conn_id="new_google_connection",
        dataset_table=f"aflow_training_bol_2024_03_12.idb_table",
        target_table_name="launches",
        replace=False
    )

api_check_up >> api_get_launches >> have_launches_today >> filter_launches_data >> save_to_gcs >> save_to_bq >> create_launches_table >> bigquery_to_pg
