import pendulum
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from plugins.slack import send_fail_alert


@dag(
    dag_id='elt_fact_bike_usage',
    tags=['elt', 'fact', 'bike', 'usage'],
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule='@daily',
    on_failure_callback=send_fail_alert,
    default_args={
        'owner': 'dongyeong',
    }
)
def elt_fact_bike_usage():
    # Check loaded raw data
    wait_for_load_raw = ExternalTaskSensor(
        task_id='wait_for_load_raw',
        external_dag_id='bike_rental_pipeline',
        external_task_id='load_data',
        mode='reschedule',
        timeout=3600 * 24,
        poke_interval=300
    )
    
    with TaskGroup('dim_group', tooltip='Dimension Table Load') as dim_group:
        # Check loaded data to dimension table
        
        # dim_date
        ExternalTaskSensor(
            task_id='wait_for_dim_date',
            external_dag_id='elt_dim_date',
            external_task_id='run_dbt',
            mode='reschedule',
            timeout=3600 * 24,
            poke_interval=300
        )
        
        # # dim_station
        ExternalTaskSensor(
            task_id='wait_for_dim_station',
            external_dag_id='elt_dim_station',
            external_task_id='run_dbt',
            execution_date_fn=lambda exec_date: exec_date.replace(day=1), # dim_station is a month cycle, fix it on the 1st every month
            mode='reschedule',
            timeout=3600 * 24,
            poke_interval=300
        )
        
        # dim_user
        ExternalTaskSensor(
            task_id='wait_for_dim_user',
            external_dag_id='elt_dim_user',
            external_task_id='run_dbt',
            mode='reschedule',
            timeout=3600 * 24,
            poke_interval=300
        )

    project_dir = Variable.get('DBT_PROJECT_DIR')
    profiles_dir = Variable.get('DBT_PROFILES_DIR')
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=f'dbt run --project-dir {project_dir} --profiles-dir {profiles_dir} --select fact_bike_usage',
    )

    [wait_for_load_raw, dim_group] >> run_dbt

dag_instance = elt_fact_bike_usage()