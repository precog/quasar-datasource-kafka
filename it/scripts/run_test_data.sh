#!/usr/bin/env bash

for id in $(docker container ps -f "name=teststack_kafka" --format "{{.ID}}"); do
  docker exec -it $id /bin/bash /run/secrets/test_data.sh
done
