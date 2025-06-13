import pendulum
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from plugins.slack import send_fail_alert


@dag(
    dag_id='elt_dim_station',
    tags=['elt', 'dim', 'station'],
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule='@monthly',
    on_failure_callback=send_fail_alert,
    default_args={
        'owner': 'dongyeong',
    }
)
def elt_dim_station():
    wait_for_load_bike_location_data = ExternalTaskSensor(
        task_id='wait_for_load_bike_location_data',
        external_dag_id='dag_bike_pipeline',
        external_task_id='load_bike_data',
        mode='reschedule',
        timeout=3600 * 24 * 3,
        poke_interval=3600
    )
    
    wait_for_load_subway_location_data = ExternalTaskSensor(
        task_id='wait_for_load_subway_location_data',
        external_dag_id='dag_subway_pipeline',
        external_task_id='load_subway_data',
        mode='reschedule',
        timeout=3600 * 24 * 3,
        poke_interval=3600
    )
    
    wait_for_load_bus_location_data = ExternalTaskSensor(
        task_id='wait_for_load_bus_location_data',
        external_dag_id='dag_bus_pipeline',
        external_task_id='load_bus_data',
        mode='reschedule',
        timeout=3600 * 24 * 3,
        poke_interval=3600
    )

    project_dir = Variable.get('DBT_PROJECT_DIR')
    profiles_dir = Variable.get('DBT_PROFILES_DIR')
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=f'dbt run --project-dir {project_dir} --profiles-dir {profiles_dir} --select dim_station'
    )

    [
        wait_for_load_bike_location_data, 
        wait_for_load_subway_location_data,
        wait_for_load_bus_location_data
    ] >> run_dbt

dag_instance = elt_dim_station()
