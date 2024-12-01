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

    # Task Group: Setup Environment
    with TaskGroup("setup_environment") as setup_group:

        # Task: Source Directory Setup Script

        setup_directory_task = BashOperator(
            task_id="setup_directory_task",
            bash_command="""
            set -a && source DIR_SETUP_NEXUS_QUICKSTART.sh /home/marc/Documents/Github/SDAP-AIRFLOW/Apache-SDAP-Airflow/ && set +a
            """,
        )

        create_docker_network = BashOperator(
            task_id="create_docker_network",
            bash_command="docker network create sdap-net || echo 'sdap-net already exists'",
        )

        # Task: Pull Docker Images
        pull_docker_images = BashOperator(
            task_id='pull_docker_images',
            bash_command="""
            docker pull zookeeper:$ZK_VERSION &&
            docker pull apache/sdap-solr-cloud:$SOLR_VERSION &&
            docker pull apache/sdap-solr-cloud-init:$SOLR_CLOUD_INIT_VERSION &&
            docker pull bitnami/cassandra:$CASSANDRA_VERSION &&
            docker pull bitnami/rabbitmq:$RMQ_VERSION &&
            docker pull apache/sdap-granule-ingester:$GRANULE_INGESTER_VERSION &&
            docker pull apache/sdap-collection-manager:$COLLECTION_MANAGER_VERSION
            """,
            env=os.environ,  # Pass sourced environment variables
        )

        # Task Group Dependencies
        setup_directory_task >> create_docker_network >> pull_docker_images

    # Task Group: Start Core Components
    with TaskGroup("start_core_components") as core_components_group:

        # Task: Start Zookeeper
        start_zookeeper = DockerOperator(
            task_id='start_zookeeper',
            image=f"zookeeper:{os.environ.get('ZK_VERSION', '3.5.5')}",
            container_name='zookeeper',
            api_version='auto',
            auto_remove=True,
            command="zkServer.sh start-foreground",
            docker_url='unix://var/run/docker.sock',
            network_mode='sdap-net',
        )

        # Task: Verify Zookeeper is running
        verify_zookeeper = HttpSensor(
            task_id='verify_zookeeper',
            http_conn_id='zookeeper_connection',
            endpoint='/',
            timeout=600,
            poke_interval=10,
        )

        # Task: Create ZNode for Solr
        create_solr_znode = BashOperator(
            task_id='create_solr_znode',
            bash_command="""
            docker exec zookeeper bash -c "bin/zkCli.sh create /solr"
            """,
        )

        # Task: Start Solr
        start_solr = DockerOperator( 
            task_id='start_solr',
            image=f"apache/sdap-solr-cloud:{os.environ.get('SOLR_VERSION', '1.4.0')}",
            container_name='solr',
            api_version='auto',
            auto_remove=True,
            command="solr start -f",
            docker_url='unix://var/run/docker.sock',
            network_mode='sdap-net',
            environment={
                'SOLR_ZK_HOSTS': 'zookeeper:2181',
                'SOLR_ENABLE_CLOUD_MODE': 'yes',
            },
        )

        # Task: Initialize Solr
        initialize_solr = BashOperator(
            task_id='initialize_solr',
            bash_command="/home/marc/Documents/Github/SDAP-AIRFLOW/Apache-SDAP-Airflow/helper_functions/monitor_initialize_solr.sh",
        )

        # Task: Start Cassandra
        start_cassandra = DockerOperator(
            task_id='start_cassandra',
            image=f"bitnami/cassandra:{os.environ.get('CASSANDRA_VERSION', '3.11.6-debian-10-r138')}",
            container_name='cassandra',
            api_version='auto',
            auto_remove=True,
            command="cassandra -f",
            docker_url='unix://var/run/docker.sock',
            network_mode='sdap-net',
            volumes=[
                f"{os.environ['CASSANDRA_DATA']}:/bitnami",
                f"{os.environ['CASSANDRA_INIT']}/initdb.cql:/scripts/initdb.cql"
            ],
            environment={
                'CASSANDRA_CLUSTER_NAME': 'cassandra',
                'CASSANDRA_PASSWORD_SEEDER': 'cassandra',
            },
        )

        # Task: Initialize Cassandra
        initialize_cassandra = BashOperator(
            task_id='initialize_cassandra',
            bash_command="""
            docker exec cassandra bash -c "cqlsh -u cassandra -p cassandra -f /scripts/initdb.cql"
            """,
        )


        # Define dependencies within the core components group
        start_zookeeper >> verify_zookeeper >> create_solr_znode >> start_solr >> initialize_solr >> start_cassandra >> initialize_cassandra

    # Task Group: Start the Ingester
    with TaskGroup("start_ingester") as ingester_group:

        # Task: Start RabbitMQ
        start_rabbitmq = DockerOperator(
            task_id='start_rabbitmq',
            image='bitnami/rabbitmq:3.8.9-debian-10-r37',
            container_name='rabbitmq',
            api_version='auto',
            auto_remove=True,
            command="rabbitmq-server",
            docker_url='unix://var/run/docker.sock',
            network_mode='sdap-net',
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
            network_mode='sdap-net',
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
            image='apache/sdap-collection-manager:1.4.0',
            container_name='collection_manager',
            api_version='auto',
            auto_remove=True,
            command="start-collection-manager.sh",
            docker_url='unix://var/run/docker.sock',
            network_mode='sdap-net',
        )

        # Define dependencies within the ingester group
        start_rabbitmq >> start_granule_ingester >> start_collection_manager

    # Define task group dependencies
    setup_group >> core_components_group >> ingester_group
