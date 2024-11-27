from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}

# Function to set the default Docker platform
def set_docker_platform():
    os.environ['DOCKER_DEFAULT_PLATFORM'] = 'linux/amd64'

# Define the DAG
with DAG(
    'sdap_nexus_deployment',
    default_args=default_args,
    description='Deploy SDAP Nexus components in sequence',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task: Set Default Docker Platform
    set_docker_platform_task = PythonOperator(
        task_id='set_docker_platform',
        python_callable=set_docker_platform,
    )

    # Task Group: Start Core Components
    with TaskGroup("start_core_components") as core_components_group:

        # Task: Start Zookeeper
        start_zookeeper = DockerOperator(
            task_id='start_zookeeper',
            image='zookeeper:3.5.5',
            container_name='zookeeper',
            api_version='auto',
            auto_remove=True,
            command="zkServer.sh start-foreground",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )

        # Task: Verify Zookeeper is running
        verify_zookeeper = HttpSensor(
            task_id='verify_zookeeper',
            http_conn_id='zookeeper_connection',
            endpoint='/',
            timeout=600,
            poke_interval=10,
        )

        # Task: Start Solr
        start_solr = DockerOperator(
            task_id='start_solr',
            image='apache/sdap-solr-cloud:1.4.0',
            container_name='solr',
            api_version='auto',
            auto_remove=True,
            command="solr start -f",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            environment={
                'SOLR_ZK_HOSTS': 'zookeeper:2181',
                'SOLR_ENABLE_CLOUD_MODE': 'yes',
            },
        )

        # Task: Initialize Solr
        initialize_solr = DockerOperator(
            task_id='initialize_solr',
            image='apache/sdap-solr-cloud-init:1.4.0',
            container_name='solr_init',
            api_version='auto',
            auto_remove=True,
            command="bash -c 'solr create -c nexustiles -shards 1 -replicationFactor 1'",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            environment={
                'SDAP_ZK_SOLR': 'zookeeper:2181/solr',
                'SDAP_SOLR_URL': 'http://solr:8983/solr/',
                'CREATE_COLLECTION_PARAMS': 'name=nexustiles&numShards=1&waitForFinalState=true',
            },
        )

        # Task: Start Cassandra
        start_cassandra = DockerOperator(
            task_id='start_cassandra',
            image='bitnami/cassandra:3.11.6-debian-10-r138',
            container_name='cassandra',
            api_version='auto',
            auto_remove=True,
            command="cassandra -f",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )

        # Task: Initialize Cassandra
        initialize_cassandra = DockerOperator(
            task_id='initialize_cassandra',
            image='bitnami/cassandra:3.11.6-debian-10-r138',
            container_name='cassandra_init',
            api_version='auto',
            auto_remove=True,
            command="bash -c 'cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS nexustiles WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }; CREATE TABLE IF NOT EXISTS nexustiles.sea_surface_temp (tile_id uuid PRIMARY KEY, tile_blob blob);\"'",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )

        # Define dependencies within the core components group
        start_zookeeper >> verify_zookeeper >> start_solr >> initialize_solr >> start_cassandra >> initialize_cassandra

    # Task Group: Start the Ingester
    with TaskGroup("start_ingester") as ingester_group:

        # Task 1: Export environment variables
        export_env_vars = BashOperator(
            task_id='export_env_vars',
            bash_command="""
            export DOCKER_DEFAULT_PLATFORM=linux/amd64 &&
            export RABBITMQ_HOST=rabbitmq &&
            export RABBITMQ_USERNAME=user &&
            export RABBITMQ_PASSWORD=bitnami &&
            export CASSANDRA_CONTACT_POINTS=cassandra &&
            export CASSANDRA_USERNAME=cassandra &&
            export CASSANDRA_PASSWORD=cassandra &&
            export SOLR_HOST_AND_PORT=http://solr:8983
            """,
        )

        # Task 2: Make required directories
        make_directories = BashOperator(
            task_id='make_directories',
            bash_command="""
            mkdir -p /data/cassandra &&
            mkdir -p /data/solr &&
            mkdir -p /data/rabbitmq
            """,
        )

        # Task 3: Pull required Docker images
        pull_docker_images = BashOperator(
            task_id='pull_docker_images',
            bash_command="""
            docker pull zookeeper:3.5.5 &&
            docker pull apache/sdap-solr-cloud:1.4.0 &&
            docker pull apache/sdap-solr-cloud-init:1.4.0 &&
            docker pull bitnami/cassandra:3.11.6-debian-10-r138 &&
            docker pull bitnami/rabbitmq:3.8.9-debian-10-r37 &&
            docker pull apache/sdap-granule-ingester:1.4.0
            """,
        )

        # Task: Start RabbitMQ
        start_rabbitmq = DockerOperator(
            task_id='start_rabbitmq',
            image='bitnami/rabbitmq:3.8.9-debian-10-r37',
            container_name='rabbitmq',
            api_version='auto',
            auto_remove=True,
            command="rabbitmq-server",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )

        # Task: Start Granule Ingester
        start_granule_ingester = DockerOperator(
            task_id='start_granule_ingester',
            image='apache/sdap-granule-ingester:1.4.0',
            container_name='granule_ingester',
            api_version='auto',
            auto_remove=True,
            command="python3 -m granule_ingester",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            environment={
                'RABBITMQ_HOST': 'rabbitmq:5672',
                'RABBITMQ_USERNAME': 'user',
                'RABBITMQ_PASSWORD': 'bitnami',
                'CASSANDRA_CONTACT_POINTS': 'cassandra',
                'CASSANDRA_USERNAME': 'cassandra',
                'CASSANDRA_PASSWORD': 'cassandra',
                'SOLR_HOST_AND_PORT': 'http://solr:8983',
            },
        )

        # Task: Start Collection Manager
        start_collection_manager = DockerOperator(
            task_id='start_collection_manager',
            image='apache/sdap-collection-manager:1.4.0'

        )

        # Task group dependencies
        setup_group >> core_components_group >> ingester_group >> webapp_group >> clean_up_group
