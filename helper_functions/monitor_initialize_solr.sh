#!/bin/bash
# Script to initialize Solr and terminate when complete.

# Start the Solr initialization process in the background
docker run -d -it --rm --name solr-init --network sdap-net -e SDAP_ZK_SOLR="host.docker.internal:2181/solr" -e SDAP_SOLR_URL="http://host.docker.internal:8983/solr/" -e CREATE_COLLECTION_PARAMS="name=nexustiles&numShards=1&waitForFinalState=true" ${REPO}/sdap-solr-cloud-init:${SOLR_CLOUD_INIT_VERSION}

# Monitor logs for the completion message
while true; do
    LOG_OUTPUT=$(docker logs solr-init 2>&1 | tail -n 10)
    echo "$LOG_OUTPUT"

    if echo "$LOG_OUTPUT" | grep -q "No such container: solr-init"; then
        docker run -d -it --rm --name solr-init --network sdap-net -e SDAP_ZK_SOLR="host.docker.internal:2181/solr" -e SDAP_SOLR_URL="http://host.docker.internal:8983/solr/" -e CREATE_COLLECTION_PARAMS="name=nexustiles&numShards=1&waitForFinalState=true" ${REPO}/sdap-solr-cloud-init:${SOLR_CLOUD_INIT_VERSION}
    fi 
    
    # Check for a specific log message indicating completion
    if echo "$LOG_OUTPUT" | grep -q "root - INFO - Done."; then
        echo "Initialization complete. Terminating process..."
        docker stop solr-init
        break
    fi

    sleep 5  # Check every 5 seconds
done
