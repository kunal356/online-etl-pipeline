#!/bin/bash

echo "Stopping and removing container..."
docker-compose down --volumes

echo "✅ Cleaned up spark-test-runner"
