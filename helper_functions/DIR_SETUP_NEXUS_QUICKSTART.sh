#!/bin/bash
# Used to make the directories for 1.4.0 SDAP Nexus QUICKSTART
# NOTE: Using 1.4.0 Version & Using Release Builds
# Usage: source ~/DIR_SETUP_NEXUS_QUICKSTART.sh <DIRECTORY_ROOT>

if [ $# -eq 0 ]; then
    DIRECTORY_ROOT=${HOME}
    echo "Usage: Set a desired directory for SDAP Quickstart directories"
    echo "DIRECTORY_ROOT is defaulted to the home directory: ${DIRECTORY_ROOT}"
else
    DIRECTORY_ROOT="$1"
    echo "DIRECTORY_ROOT is set as: $1"
fi

# DEFAULT DOCKER PLATFORM
export DOCKER_DEFAULT_PLATFORM=linux/amd64

# custom
export GRANULE_INGESTER_PATHWAY=${DIRECTORY_ROOT}/nexus-quickstart

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

export JUPYTER_VERSION=1.0.0-rc2

# Release Builds
export REPO=apache

# SOLR
export SOLR_DATA=${DIRECTORY_ROOT}/nexus-quickstart/solr
mkdir -p ${SOLR_DATA}

# CASSANDRA
export CASSANDRA_INIT=${DIRECTORY_ROOT}/nexus-quickstart/init
export CASSANDRA_DATA=${DIRECTORY_ROOT}/nexus-quickstart/cassandra
mkdir -p ${CASSANDRA_INIT}
mkdir -p ${CASSANDRA_DATA}

cat << EOF >> ${CASSANDRA_INIT}/initdb.cql
CREATE KEYSPACE IF NOT EXISTS nexustiles WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS nexustiles.sea_surface_temp  (
tile_id       uuid PRIMARY KEY,
tile_blob     blob
);
EOF

# DATA DIRECTORY
export DATA_DIRECTORY=${DIRECTORY_ROOT}/nexus-quickstart/data/
mkdir -p ${DATA_DIRECTORY}

# GRANULE INGESTER/s
cat << EOF >> ${DIRECTORY_ROOT}/nexus-quickstart/granule-ingester.env
RABBITMQ_HOST=host.docker.internal:5672
RABBITMQ_USERNAME=user
RABBITMQ_PASSWORD=bitnami
CASSANDRA_CONTACT_POINTS=host.docker.internal
CASSANDRA_USERNAME=cassandra
CASSANDRA_PASSWORD=cassandra
SOLR_HOST_AND_PORT=http://host.docker.internal:8983
EOF

# SAMPLE DATA DIRECTORY
mkdir -p ${DATA_DIRECTORY}/avhrr-granules

# COLLECTION CONFIGURATION
export CONFIG_DIR=${DIRECTORY_ROOT}/nexus-quickstart/ingester/config
mkdir -p ${CONFIG_DIR}
cat << EOF >> ${CONFIG_DIR}/collectionConfig.yml
collections:
  - id: AVHRR_OI_L4_GHRSST_NCEI
    path: /data/granules/avhrr-granules/*AVHRR_OI-GLOB-v02.0-fv02.0.nc
    priority: 1
    forward-processing-priority: 5
    projection: Grid
    dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: analysed_sst
    slices:
      lat: 100
      lon: 100
      time: 1
EOF
