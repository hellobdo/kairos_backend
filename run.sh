#!/bin/bash

# Build the Docker image
docker build -t kairos .

# Run the container with database volume mounted
docker run -it \
    --name kairos \
    -v "$(pwd)/data:/app/data" \
    -v "$(pwd)/kairos.db:/app/kairos.db" \
    kairos 