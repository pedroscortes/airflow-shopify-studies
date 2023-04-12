import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


args = {
    'owner': 'Pedro Cortes',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='sallve_incremental_refresh',
    default_args=args,
    schedule_interval=timedelta(minutes=5),
    dagrun_timeout=timedelta(minutes=3)
)

incremental_refresh_orders = BashOperator(
    task_id='orders',
    bash_command='cd ${AIRFLOW_HOME}/scripts && python incremental_refresh_orders',
    dag=dag)

incremental_refresh_products = BashOperator(
    task_id='products',
    bash_command='cd ${AIRFLOW_HOME}/scripts && python incremental_refresh_products',
    dag=dag)


incremental_refresh_products >> incremental_refresh_orders