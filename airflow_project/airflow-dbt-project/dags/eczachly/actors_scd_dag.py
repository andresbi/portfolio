from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.poke_tabular_partition import poke_tabular_partition
from include.eczachly.trino_queries import execute_trino_query
import os
from airflow.models import Variable
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')
tabular_credential = Variable.get("TABULAR_CREDENTIAL")

@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(1914, 1, 1),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(1914, 1, 1),
    # end_date=datetime(1920, 1, 1),
    max_active_runs=1,
    schedule_interval="@yearly",
    catchup=False,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def actors_scd_dag():
    # TODO make sure to rename this if you're testing this dag out!
    upstream_table = 'andres_79435.mini_actors'
    production_table = 'andres_79435.actors_history_scd'
    

    # #NEED TO UNDERSTAND THIS PARTITION SENSOR THING
    wait_for_web_events = PythonOperator(
    task_id='wait_for_web_events',
    depends_on_past=True,
    python_callable=poke_tabular_partition,
    op_kwargs={
        "tabular_credential": tabular_credential,
        "table": upstream_table,
        "partition": 'year={{ ds[:4] }}'
    },
    provide_context=True  # This allows you to pass additional context to the function
    )
    

    #is the partitioning here at the end for performance? any other reason?
    create_step = PythonOperator(
        task_id="create_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
             CREATE TABLE IF NOT EXISTS {production_table}
                (
                actor_id VARCHAR,
                actor VARCHAR,
                is_active BOOLEAN,
                start_date INT,
                end_date INT,
                year INT
                )
                WITH (
                partitioning = ARRAY['year']
                )
             """
        }
    )

#Understand the depends on past = True
    yesterday_ds = '{{ yesterday_ds[:4] }}'
    ds = '{{ ds[:4] }}'
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
               DELETE FROM {production_table} 
               WHERE year = {ds}
               """
        }
    )

#Understand why this one doesnt need depends on past
    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 insert into {production_table}

                with yesterday as (
                select
                    *
                from
                    {production_table}
                where
                    year = { yesterday_ds }
                ),
                today as (
                select
                    actor,
                    actor_id,
                    is_active,
                    max(year) year
                    from
                    {upstream_table}
                    where year = {ds}
                group by
                    actor,
                    actor_id,
                    is_active
                )

                ,
                actor_status as (
                    select
                    coalesce(y.actor_id, t.actor_id) as actor_id,
                    coalesce(y.actor, t.actor) as actor,
                    t.year is not null as is_active,
                    case
                    when coalesce(y.is_active,FALSE) <> coalesce(t.is_active,FALSE) then 1
                    when coalesce(y.is_active,FALSE) = coalesce(t.is_active,FALSE) then 0
                    end as rec_change, 
                    coalesce(y.start_date, t.year) as start_date,
                    coalesce(y.end_date, t.year) as end_date,
                    coalesce(t.year, y.year+1) as year
                from
                    yesterday y full join today t on y.actor_id = t.actor_id
                )

                , 
                update_status as (
                select
                actor_id,
                actor,
                is_active,
                case 
                    when rec_change = 0 or rec_change is null or start_date = year then start_date 
                    else start_date+1 end as start_date,
                case
                    when rec_change = 1 then end_date
                    when rec_change = 0 then end_date + 1
                    else year end as end_date,
                year
                from
                actor_status
                )

                select
                actor_id,
                actor,
                is_active,
                start_date,
                end_date,
                year
                from
                update_status

                 """
        }
    )

    wait_for_web_events >> create_step >> clear_step >> cumulate_step


actors_scd_dag()

