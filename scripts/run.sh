#!/usr/bin/env bash

docker image load -i data/gc25cdocker.tar

docker compose up --build

# TODO: setup Kafka (create broker)

# TODO: start flink