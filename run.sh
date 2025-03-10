#!/bin/bash

# Build the Docker image
docker build -t kairos .

# Run the container with current directory mounted
docker run -it --rm \
    -v "$(pwd):/app" \
    kairos 