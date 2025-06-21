#!/usr/bin/env bash

set -euo pipefail

JAR_PATH="target/my-flink-job-1.0-SNAPSHOT.jar"
mkdir -p Results
chmod 777 Results
echo "Caricamento  challenger"
docker image load -i data/gc25cdocker.tar
echo "Compose"
docker compose up --build -d
