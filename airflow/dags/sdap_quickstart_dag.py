from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from airflow.sensors.base import BaseSensorOperator
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow-sdap',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}


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


def set_docker_platform() -> None:
    os.environ['DOCKER_DEFAULT_PLATFORM'] = 'linux/amd64'

# Define the DAG
with DAG(
    'sdap_nexus_deployment',
    default_args=default_args,
    description='Deploy SDAP Nexus components in sequence',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1), # To be set at a date before current time
    catchup=False,
) as dag:

    # Task: Set Default Docker Platform
    set_docker_platform_task = PythonOperator(
        task_id='set_docker_platform',
        python_callable=set_docker_platform,
    )

    # Task Group: Setup Environment
    with TaskGroup("setup_environment") as setup_group:

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
            echo "CASSANDRA_DATA=${CASSANDRA_DATA}
            echo "CASSANDRA_INIT=${CASSANDRA_INIT}
            echo "CASSANDRA_VERSION=${CASSANDRA_VERSION}

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

        # Dependencies
        start_zookeeper >> create_solr_znode >> start_solr >> initialize_solr_nexustiles >> start_cassandra >> initialize_cassandra

    # Task Group: Start the Ingester
    with TaskGroup("start_ingester") as ingester_group:

        # Task: Start RabbitMQ
        start_rabbitmq = BashOperator(
            task_id='start_rabbitmq',
            bash_command="""
            echo "RMQ_VERSION=${RMQ_VERSION}"
            docker run -dp 5672:5672 -p 15672:15672 --name rmq --network sdap-net bitnami/rabbitmq:${RMQ_VERSION}
            """,
            env={"RMQ_VERSION": os.environ.get("RMQ_VERSION")},
        )


        # Task: Start Granule Ingester
        start_granule_ingester = BashOperator(
            task_id='start_granule_ingester',
            bash_command="""
            echo "GRANULE_INGESTER_PATHWAY=${GRANULE_INGESTER_PATHWAY}"
            echo "DATA_DIRECTORY=${DATA_DIRECTORY}"
            echo "REPO=${REPO}"
            echo "GRANULE_INGESTER_VERSION=${GRANULE_INGESTER_VERSION}"
            docker run --name granule-ingester --network sdap-net -d \
                --env-file ${GRANULE_INGESTER_PATHWAY}/granule-ingester.env \
                -v ${DATA_DIRECTORY}:/data/granules/ \
                ${REPO}/sdap-granule-ingester:${GRANULE_INGESTER_VERSION}
            """,
            env={
                "GRANULE_INGESTER_PATHWAY": os.environ.get("GRANULE_INGESTER_PATHWAY"),
                "DATA_DIRECTORY": os.environ.get("DATA_DIRECTORY"),
                "REPO": os.environ.get("REPO"),
                "GRANULE_INGESTER_VERSION": os.environ.get("GRANULE_INGESTER_VERSION"),
            },
        )



        # Task: Start Collection Manager
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



        # Dependencies within the ingester group
        start_rabbitmq >> start_granule_ingester >> start_collection_manager

    #  task group dependencies
    setup_group >> components_group >> ingester_group
