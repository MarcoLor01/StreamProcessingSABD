#!/usr/bin/env bash

set -euo pipefail

JAR_PATH="target/my-flink-job-1.0-SNAPSHOT.jar"
mkdir -p Results
# Per poter effettuare la creazione di results
# sudo chown marty:marty Results
chmod 777 Results
echo "Caricamento  challenger"
docker image load -i data/gc25cdocker.tar
mvn clean package
echo "Compose"
docker compose up --build -d
echo "Aspetto che il JobManager sia pronto"
sleep 5
echo "Caricamento Jar"
docker cp target/StreamingApplicationSABD-1.0-SNAPSHOT.jar jobmanager:/opt/flink/lib/
docker cp target/StreamingApplicationSABD-1.0-SNAPSHOT.jar taskmanager:/opt/flink/lib/
echo "Esecuzione job flink"
docker exec -it jobmanager flink run -c flink.Main /opt/flink/lib/StreamingApplicationSABD-1.0-SNAPSHOT.jar