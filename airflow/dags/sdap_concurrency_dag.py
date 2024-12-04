from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from requests.auth import HTTPBasicAuth
import subprocess
import requests
import time
from datetime import datetime, timedelta
import os
from airflow.sensors.base import BaseSensorOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow-sdap',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}

doc_md_DAG = """
### Docker Images to be pulled

- Zookeeper
- Solr
- Solr init
- Cassandra
- RabbitMQ (RMQ)
- Granule Ingester
- Collection Manager
"""

# Parameters
DATA_DIRECTORY = os.environ.get("DATA_DIRECTORY")
GRANULE_INGESTER_PATHWAY = os.environ.get("GRANULE_INGESTER_PATHWAY")
ENV_FILE = f"{GRANULE_INGESTER_PATHWAY}/granule-ingester.env"
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "sdap-net")

REPO = os.environ.get("REPO")
GRANULE_INGESTER_VERSION = os.environ.get("GRANULE_INGESTER_VERSION")
DOCKER_IMAGE =  f"{REPO}/sdap-granule-ingester:{GRANULE_INGESTER_VERSION}"


CONTAINER_NAME_PREFIX = "granule-ingester"
RABBITMQ_QUEUE = "nexus"
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "user"
RABBITMQ_PASS = "bitnami"

MONITOR_INTERVAL = 10  # Seconds
FILES_PER_INSTANCE = 15


class ZookeeperReadySensor(BaseSensorOperator):
    """
    Custom sensor to check if Zookeeper is ready by polling its status endpoint.
    """
    def poke(self, context) -> bool:
        try:
            response = requests.get("http://localhost:2181", timeout=5)
            return response.status_code == 200
        except Exception:
            return False

def sleep_function() -> None:
    time.sleep(30)

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


def calculate_required_instances(ti) -> None:
    """
    Calculate the number of granule-ingester instances needed.
    """
    messages_ready, messages_unacknowledged = get_rmq_queue_status()
    total_messages = messages_ready + messages_unacknowledged
    required_instances = (total_messages + FILES_PER_INSTANCE - 1) // FILES_PER_INSTANCE  # Ceiling division
    ti.xcom_push(key="required_instances", value=required_instances)
    print(f"Pushed {required_instances} to XCom with key 'required_instances'")

    return required_instances


def scale_containers(ti) -> None:
    """
    Scale granule-ingester containers dynamically.
    """
    required_instances = ti.xcom_pull(dag_id="sdap_concurrency_dag", task_ids="granule_ingester_dynamic_scaling.calculate_instances", key="required_instances")
    running_containers = ti.xcom_pull(dag_id="sdap_concurrency_dag", task_ids="granule_ingester_dynamic_scaling.scale_containers", key="running_containers") or []
    print(f"Pulled required_instances: {required_instances} from Xcom with key 'required instances'")
    print("granule_ingester_dynamic_scaling.calculate_instances")
    print("dag_id")

    # Determine scaling actions
    running_count = len(running_containers)
    print(f"required instances: {required_instances}")
    print(f"running containers: {running_containers}")
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
            print(f"Container {container_name} created.")
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
        if running_containers == 0:
            print("Ingested all files from RMQ queue")
            return None

    # Update XCom with the current running containers
    ti.xcom_push(key="running_containers", value=running_containers)


def monitor_and_stop_all_containers(ti) -> None:
    """
    Stop all containers when the queue is empty for a cooldown period.
    """
    running_containers = ti.xcom_pull(task_ids="granule_ingester_dynamic_scaling.scale_containers", key="running_containers") or []

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


# Define the DAG
with DAG(
    'sdap_concurrency_dag',
    default_args=default_args,
    description='Deploy SDAP Nexus components in sequence',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1), # To be set at a date before current time
    catchup=False,
) as dag:
    # Task Group: Setup Environment
    with TaskGroup("setup_environment", tooltip="Handles creating docker network and pulling docker images") as setup_group:

        # Task: Source Directory Setup Script

        # setup_directory_task = BashOperator(
        #     task_id="setup_directory_task",
        #     bash_command=""" 
        #     set -a && source $SDAP_repo/helper_functions/DIR_SETUP_NEXUS_QUICKSTART.sh $SDAP_repo && set +a
        #     """,
        #     env=os.environ,
        # )

        create_docker_network = BashOperator(
            task_id="create_docker_network",
            bash_command="docker network create --driver bridge --attachable sdap-net || echo 'sdap-net already exists'",
            doc="Creates sdap-net docker network if the network does not exist already"
        )

        # Task: Pull Docker Images
        pull_docker_images = BashOperator(
            task_id='pull_docker_images',
            bash_command="""
            echo "Checking environment variables..."
            echo "ZK_VERSION=$ZK_VERSION"
            echo "SOLR_VERSION=$SOLR_VERSION"
            echo "SOLR_CLOUD_INIT_VERSION=$SOLR_CLOUD_INIT_VERSION"
            echo "CASSANDRA_VERSION=$CASSANDRA_VERSION"
            echo "RMQ_VERSION=$RMQ_VERSION"
            echo "GRANULE_INGESTER_VERSION=$GRANULE_INGESTER_VERSION"
            echo "COLLECTION_MANAGER_VERSION=$COLLECTION_MANAGER_VERSION"

            docker pull zookeeper:$ZK_VERSION &&
            docker pull apache/sdap-solr-cloud:$SOLR_VERSION &&
            docker pull apache/sdap-solr-cloud-init:$SOLR_CLOUD_INIT_VERSION &&
            docker pull bitnami/cassandra:$CASSANDRA_VERSION &&
            docker pull bitnami/rabbitmq:$RMQ_VERSION &&
            docker pull apache/sdap-granule-ingester:$GRANULE_INGESTER_VERSION &&
            docker pull apache/sdap-collection-manager:$COLLECTION_MANAGER_VERSION
            """,
            env=os.environ,
            doc_md=doc_md_DAG
        )

        # Task Group Dependencies
        # setup_directory_task >> create_docker_network >> pull_docker_images
        create_docker_network >> pull_docker_images

    # Task Group: Start Core Components
    with TaskGroup("start_core_components") as components_group:

        # Task: Start Zookeeper
        start_zookeeper = BashOperator(
            task_id='start_zookeeper',
            bash_command=f"docker run --name zookeeper -dp 2181:2181 zookeeper:{os.environ.get('ZK_VERSION')}",
        )

        # Task: Create ZNode for Solr
        create_solr_znode = BashOperator(
            task_id='create_solr_znode',
            bash_command="""
            docker exec zookeeper bash -c "bin/zkCli.sh create /solr"
            """,
        )

        # Task: Start Solr
        start_solr = BashOperator(
            task_id='start_solr',
            bash_command="""
            docker run --name solr --network sdap-net \
            -v ${SOLR_DATA}/:/bitnami \
            -p 8983:8983 \
            -e SOLR_ZK_HOSTS="host.docker.internal:2181" \
            -e SOLR_ENABLE_CLOUD_MODE="yes" \
            -d ${REPO}/sdap-solr-cloud:${SOLR_VERSION}
            """,
            env={
                "SOLR_DATA": os.environ.get("SOLR_DATA"),
                "REPO": os.environ.get("REPO"),
                "SOLR_VERSION": os.environ.get("SOLR_VERSION"),
            },
        )

        # Task: Initialize Solr
        initialize_solr_nexustiles = BashOperator(
            task_id='initialize_solr_nexustiles',
            bash_command="{{ 'bash /home/marc/Documents/Github/SDAP-AIRFLOW/Apache-SDAP-Airflow/helper_functions/monitor_initialize_solr.sh' }}",
        )

        # Task: Start Cassandra
        start_cassandra = BashOperator(
            task_id='start_cassandra',
            bash_command="""
            echo "CASSANDRA_DATA=${CASSANDRA_DATA}"
            echo "CASSANDRA_INIT=${CASSANDRA_INIT}"
            echo "CASSANDRA_VERSION=${CASSANDRA_VERSION}"

            docker run --name cassandra --network sdap-net \
            -p 9042:9042 \
            -v ${CASSANDRA_DATA}/cassandra/:/bitnami \
            -v "${CASSANDRA_INIT}/initdb.cql:/scripts/initdb.cql" \
            -d bitnami/cassandra:${CASSANDRA_VERSION}
            """,
            env={
                "CASSANDRA_DATA": os.environ.get("CASSANDRA_DATA"),
                "CASSANDRA_INIT": os.environ.get("CASSANDRA_INIT"),
                "CASSANDRA_VERSION": os.environ.get("CASSANDRA_VERSION"),
            },
        )


        # Task: Initialize Cassandra
        initialize_cassandra = BashOperator(
            task_id='initialize_cassandra',
            bash_command="""
            sleep 30
            for i in {1..5}; do
                echo "Attempt $i to initialize Cassandra..."
                docker exec cassandra bash -c "cqlsh -u cassandra -p cassandra -f /scripts/initdb.cql" && break
                echo "Initialization failed. Retrying in 30 seconds..."
                sleep 30
            done
            """,
        )

        # Task: Start RabbitMQ
        start_rabbitmq = BashOperator(
            task_id='start_rabbitmq',
            bash_command="""
            echo "RMQ_VERSION=${RMQ_VERSION}"
            docker run -dp 5672:5672 -p 15672:15672 --name rmq --network sdap-net bitnami/rabbitmq:${RMQ_VERSION}
            """,
            env={"RMQ_VERSION": os.environ.get("RMQ_VERSION")},
        )

        # Task that sleeps for 30 seconds
        delay = PythonOperator(
            task_id='delay',
            python_callable=sleep_function,
            doc="This task demonstrates a 30-second delay using Python."
        )

        start_collection_manager = BashOperator(
            task_id='start_collection_manager',
            bash_command="""
            docker run --name collection-manager --network sdap-net -d \
            -v ${DATA_DIRECTORY}:/data/granules/ \
            -v ${CONFIG_DIR}:/home/ingester/config/ \
            -e COLLECTIONS_PATH="/home/ingester/config/collectionConfig.yml" \
            -e HISTORY_URL="http://host.docker.internal:8983/" \
            -e RABBITMQ_HOST="host.docker.internal:5672" \
            -e RABBITMQ_USERNAME="user" \
            -e RABBITMQ_PASSWORD="bitnami" \
            ${REPO}/sdap-collection-manager:${COLLECTION_MANAGER_VERSION}
            """,
            env={
                "DATA_DIRECTORY": os.environ.get("DATA_DIRECTORY"),
                "CONFIG_DIR": os.environ.get("CONFIG_DIR"),
                "REPO": os.environ.get("REPO", "apache"),
                "COLLECTION_MANAGER_VERSION": os.environ.get("COLLECTION_MANAGER_VERSION", "1.4.0"),
            },
        )

        # Dependencies
        start_zookeeper >> create_solr_znode >> start_solr >> initialize_solr_nexustiles >> start_cassandra >> initialize_cassandra >> start_rabbitmq >> delay >> start_collection_manager

    # Task Group: Start the Ingester
    with TaskGroup("granule_ingester_dynamic_scaling") as ingester_group:
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

    with TaskGroup("stop-containers") as tear_down_group:
        stop_solr = BashOperator(
            task_id='start_collection_manager',
            bash_command="""
            docker exec solr /opt/bitnami/solr/bin/solr stop -p 8983
            """
        )

        stop_containers = BashOperator(
            task_id="stop_containers",
            bash_command="""
            docker stop zookeeper solr cassandra rmq collection-manager
            """
        )

        stop_solr >> stop_containers
        

    #  task group dependencies
    # setup_group >> components_group >> ingester_group >> tear_down_group
    setup_group >> components_group >> ingester_group

