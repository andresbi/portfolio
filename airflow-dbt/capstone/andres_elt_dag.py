import os
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
# from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from dags.capstone.python_scripts.load_polygon_into_snowflake import load_polygon_main 
from dags.capstone.python_scripts.load_treasury_into_snowflake import load_treasury_main 
from dags.capstone.python_scripts.load_treasury_refunds_into_snowflake import fetch_refund_data 


dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_capstone_andres', 'dbt.env')
load_dotenv(dbt_env_path)

# Retrieve environment variables
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECT = f'{airflow_home}/dags/capstone/dbt_capstone_andres'
PATH_TO_DBT_PROFILES = f'{airflow_home}/dags/capstone/dbt_capstone_andres/profiles.yml'

profile_config = ProfileConfig(
    profile_name="dbt_capstone_andres",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

default_args = {
  "owner": "Andres",
  "retries": 1,
  "execution_timeout": timedelta(hours=1),
}

@dag(
    start_date=datetime(2025, 1, 3),
    # schedule='@once',
    description ="Dag to ingest API data into Snowflake and transform with dbt",
    schedule="0 1 * * *",
    catchup=True,
    tags=["andres"],
    default_args=default_args,
)
def andres_elt_dag():

    load_polygon_data_task = PythonOperator(
    task_id='load_polygon_data_task',
    python_callable=load_polygon_main,
    depends_on_past=True,
    op_kwargs={
        "date_to_ingest": '{{ yesterday_ds }}',
        "polygon_key": 'Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q'
    },
    )

    load_treasury_data_task = PythonOperator(    
    task_id='load_treasury_data_task',
    python_callable=load_treasury_main,
    depends_on_past=True,
    op_kwargs={
        "date_to_ingest": '{{ yesterday_ds }}'
    },
    )

    load_refund_data_task = PythonOperator(    
    task_id='load_refund_data_task',
    python_callable=fetch_refund_data,
    depends_on_past=True,
    op_kwargs={
        "date_to_ingest": '{{ yesterday_ds }}'
    },
    )

    dbt_freshness = BashOperator(
        task_id='dbt_freshness',
        bash_command='dbt source freshness',
        cwd=PATH_TO_DBT_PROJECT,
    )

    dbt_build_weekly_metrics_task = DbtTaskGroup(
        group_id="dbt_build_weekly_metrics_task",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["+weekly_metrics"],
        ),
    )

    # Define task dependencies
    # load_polygon_data_task >> load_treasury_data_task
    load_polygon_data_task >> load_treasury_data_task >> load_refund_data_task >> dbt_freshness >> dbt_build_weekly_metrics_task


andres_elt_dag()