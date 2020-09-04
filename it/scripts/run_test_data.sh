#!/usr/bin/env bash

docker ps -f name=teststack_kafka

for id in $(docker ps -f "name=teststack_kafka" -q); do
  echo "Loading data on container $id"
  docker exec -it $id /bin/bash /run/secrets/test_data.sh
done
