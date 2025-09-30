#!/usr/bin/env bash
# Run the disaster recovery integration test in a container

echo "Running disaster recovery integration test in container..."

# Run the test in the synapse-dev container
podman run --rm \
    -v ".:/synapse" \
    -w /synapse \
    --entrypoint="" \
    localhost/synapse-dev:latest \
    bash -c "
        # Set up Python path
        export PYTHONPATH=/synapse
        
        # Run the integration test
        python test_disaster_recovery_integration.py $1
    "