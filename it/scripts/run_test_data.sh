#!/usr/bin/env bash

docker container ps

for id in $(docker container ps -f "name=teststack_kafka" --format "{{.ID}}"); do
  echo "Loading data on container $id"
  docker exec -it $id /bin/bash /run/secrets/test_data.sh
done
