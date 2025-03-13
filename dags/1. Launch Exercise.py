from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id="01_launch_exercise",
        start_date=datetime(year=2019, month=1, day=1),
        end_date=datetime(year=2019, month=1, day=5),
        schedule="@daily",
):
    procedure_rocket_material = EmptyOperator(task_id="procedure_rocket_material")
    procure_fuel = EmptyOperator(task_id="procure_fuel")
    build_stage_1 = EmptyOperator(task_id="build_stage_1")
    build_stage_2 = EmptyOperator(task_id="build_stage_2")
    build_stage_3 = EmptyOperator(task_id="build_stage_3")
    launch = EmptyOperator(task_id="launch")

procedure_rocket_material >> [build_stage_1, build_stage_2, build_stage_3] >> launch
procure_fuel >> build_stage_3 >> launch
