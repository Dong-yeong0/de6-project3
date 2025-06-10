import pendulum
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from plugins.slack import send_fail_alert


@dag(
    dag_id='elt_dim_date',
    tags=['elt', 'dim', 'date'],
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule='@daily',
    on_failure_callback=send_fail_alert,
    default_args={
        'owner': 'dongyeong',
    }
)
def elt_dim_date():
    wait_for_load_bike_usage_data = ExternalTaskSensor(
        task_id='wait_for_load_bike_usage_data',
        external_dag_id='bike_rental_pipeline',
        external_task_id='load_data',
        mode='reschedule',
        timeout=3600 * 24,
        poke_interval=300
    )
    
    wait_for_load_bus_usage_data = ExternalTaskSensor(
        task_id='wait_for_load_bus_usage_data',
        external_dag_id='bus_usage_pipeline',
        external_task_id='load_data',
        mode='reschedule',
        timeout=3600 * 24,
        poke_interval=300
    )

    wait_for_load_subway_usagedata = ExternalTaskSensor(
        task_id='wait_for_load_subway_usage_data',
        external_dag_id='bike_rental_pipeline',
        external_task_id='load_data',
        mode='reschedule',
        timeout=3600 * 24,
        poke_interval=300
    )

    project_dir = Variable.get('DBT_PROJECT_DIR')
    profiles_dir = Variable.get('DBT_PROFILES_DIR')
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=f'dbt run --project-dir {project_dir} --profiles-dir {profiles_dir} --select dim_date',
    )

    [
        wait_for_load_bike_usage_data,
        wait_for_load_bus_usage_data,
        # wait_for_load_subway_usagedata,
    ] >> run_dbt

dag_instance = elt_dim_date()