#!/bin/bash
# Script to initialize Solr and terminate when complete.

# Start the Solr initialization process in the background
docker exec -it solr_init bash -c "solr create -c nexustiles -shards 1 -replicationFactor 1" &
INIT_PID=$!

# Monitor logs for the completion message
while true; do
    LOG_OUTPUT=$(docker logs solr_init 2>&1 | tail -n 10)
    echo "$LOG_OUTPUT"

    # Check for a specific log message indicating completion
    if echo "$LOG_OUTPUT" | grep -q "Collection nexustiles created successfully"; then
        echo "Initialization complete. Terminating process..."
        kill -SIGINT $INIT_PID  # Send Ctrl+C equivalent
        break
    fi

    sleep 5  # Check every 5 seconds
done
