from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import requests
from requests.auth import HTTPBasicAuth
import time
import os

# Parameters
DATA_DIRECTORY = os.environ.get("DATA_DIRECTORY", "/default/data/directory")
ENV_FILE = os.environ.get("ENV_FILE", "/default/path/to/granule-ingester.env")
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE", "default/repo/sdap-granule-ingester:latest")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "sdap-net")

CONTAINER_NAME_PREFIX = "granule-ingester"
RABBITMQ_QUEUE = "nexus"
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "user"
RABBITMQ_PASS = "bitnami"

MONITOR_INTERVAL = 10  # Seconds
FILES_PER_INSTANCE = 15

# Used for Airflow DAG args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": datetime(2024, 1, 1),
}


def get_rmq_queue_status() -> tuple[int, int]:
    """
    Fetch the RabbitMQ queue status.
    """
    url = f"http://{RABBITMQ_HOST}:15672/api/queues/%2f/{RABBITMQ_QUEUE}"
    response = requests.get(url, auth=HTTPBasicAuth(RABBITMQ_USER, RABBITMQ_PASS))
    if response.ok:
        data = response.json()
        return data["messages_ready"], data["messages_unacknowledged"]
    else:
        raise Exception(f"Failed to query RabbitMQ: {response.status_code} - {response.text}")


def calculate_required_instances(ti) -> int:
    """
    Calculate the number of granule-ingester instances needed.
    """
    messages_ready, messages_unacknowledged = get_rmq_queue_status()
    total_messages = messages_ready + messages_unacknowledged
    required_instances = (total_messages + FILES_PER_INSTANCE - 1) // FILES_PER_INSTANCE  # Ceiling division
    ti.xcom_push(key="required_instances", value=required_instances)
    return required_instances


def scale_containers(ti) -> None:
    """
    Scale granule-ingester containers dynamically.
    """
    required_instances = ti.xcom_pull(task_ids="calculate_instances", key="required_instances")
    running_containers = ti.xcom_pull(task_ids="track_containers", key="running_containers") or []

    # Determine scaling actions
    running_count = len(running_containers)
    if required_instances > running_count:
        # Scale up
        new_containers = []
        for i in range(running_count, required_instances):
            container_name = f"{CONTAINER_NAME_PREFIX}-{i}"
            subprocess.run(
                [
                    "docker", "run",
                    "--name", container_name,
                    "--network", DOCKER_NETWORK,
                    "-d",
                    "--env-file", ENV_FILE,
                    "-v", f"{DATA_DIRECTORY}:/data/granules/",
                    DOCKER_IMAGE,
                ],
                check=True,
            )
            new_containers.append(container_name)
        running_containers.extend(new_containers)
        print(f"Started {len(new_containers)} new containers.")
    elif required_instances < running_count:
        # Scale down
        excess_containers = running_containers[required_instances:]
        for container_name in excess_containers:
            subprocess.run(["docker", "stop", container_name], check=True)
            print(f"Stopped container: {container_name}")
        running_containers = running_containers[:required_instances]

    # Update XCom with the current running containers
    ti.xcom_push(key="running_containers", value=running_containers)


def monitor_and_stop_all_containers(ti) -> None:
    """
    Stop all containers when the queue is empty for a cooldown period.
    """
    running_containers = ti.xcom_pull(task_ids="scale_containers", key="running_containers") or []

    cooldown_start = None
    while True:
        messages_ready, messages_unacknowledged = get_rmq_queue_status()
        print(f"Queue Status: {messages_ready} ready, {messages_unacknowledged} in progress")

        if messages_ready == 0 and messages_unacknowledged == 0:
            if cooldown_start is None:
                cooldown_start = time.time()
            elif time.time() - cooldown_start >= MONITOR_INTERVAL:
                print("Queue is empty, stopping all containers.")
                for container_name in running_containers:
                    subprocess.run(["docker", "stop", container_name], check=True)
                break
        else:
            cooldown_start = None

        time.sleep(MONITOR_INTERVAL)


with DAG("granule_ingester_dynamic_scaling", default_args=default_args, schedule_interval="*/5 * * * *") as dag:
    # Task 1: Calculate Required Instances
    calculate_instances = PythonOperator(
        task_id="calculate_instances",
        python_callable=calculate_required_instances,
        provide_context=True,
    )

    # Task 2: Scale Containers
    scale_containers_task = PythonOperator(
        task_id="scale_containers",
        python_callable=scale_containers,
        provide_context=True,
    )

    # Task 3: Monitor Queue and Stop All Containers
    monitor_and_stop_task = PythonOperator(
        task_id="monitor_and_stop_containers",
        python_callable=monitor_and_stop_all_containers,
        provide_context=True,
    )

    calculate_instances >> scale_containers_task >> monitor_and_stop_task
