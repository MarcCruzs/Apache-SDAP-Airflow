from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Example functions
def push_data(ti) -> int:
    # Push a value to XCom
    value = 42
    print(value)
    ti.xcom_push(key="my_key", value=value)
    print("DEBUG: Pushed value 42 to XCom with key='my_key'")
    return value

def pull_data(ti):
    # Pull the value from XCom
    value = ti.xcom_pull(task_ids="push_task", key="my_key")
    print(f"DEBUG: Pulled value {value} from XCom with key='my_key'")

# DAG definition
with DAG(
    dag_id="example_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_data,
        provide_context=True,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_data,
        provide_context=True,
    )

    push_task >> pull_task
