from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator


def _print_context(**context):
    # INFO - Task 'ctx' is running in the '03_v2_context' pipeline
    # pprint(context)
    print(f"Task '{context['task'].task_id}' is running in the '{context['dag'].dag_id}' pipeline.")
    print(f"Script was executed at '{context['execution_date'].date()}'")
    print(f"Three days after execution '{context['execution_date'].date() + timedelta(days=3)}'")
    print(f"This script run date is'{context['data_interval_start'].date()}'")


with DAG(
        dag_id="03_v2_context",
        start_date=datetime(year=2025, month=3, day=1),
        end_date=None,
        schedule="@daily",
):
    context = PythonOperator(task_id="ctx", python_callable=_print_context)

context
