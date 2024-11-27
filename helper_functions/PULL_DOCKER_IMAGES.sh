#!/bin/bash
# Used to pull docker images for 1.4.0 SDAP Nexus QUICKSTART
# NOTE: Using 1.4.0 Version & Using Release Builds
# Usage: source ~/PULL_DOCKER_IMAGES.sh

# SET TAG VARIABLES
export CASSANDRA_VERSION=3.11.6-debian-10-r138
export RMQ_VERSION=3.8.9-debian-10-r37
export COLLECTION_MANAGER_VERSION=1.4.0
export GRANULE_INGESTER_VERSION=1.4.0
export WEBAPP_VERSION=1.4.0
export SOLR_VERSION=1.4.0
export SOLR_CLOUD_INIT_VERSION=1.4.0
export ZK_VERSION=3.5.5

export REPO=apache

docker pull bitnami/cassandra:${CASSANDRA_VERSION}
docker pull bitnami/rabbitmq:${RMQ_VERSION}
docker pull apache/sdap-collection-manager:${COLLECTION_MANAGER_VERSION}
docker pull apache/sdap-granule-ingester:${GRANULE_INGESTER_VERSION}
docker pull apache/sdap-nexus-webapp:${WEBAPP_VERSION}
docker pull apache/sdap-solr-cloud:${SOLR_VERSION}
docker pull apache/sdap-solr-cloud-init:${SOLR_CLOUD_INIT_VERSION}
docker pull zookeeper:${ZK_VERSION}

docker pull nexusjpl/jupyter:${JUPYTER_VERSION}