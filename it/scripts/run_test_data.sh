#!/usr/bin/env bash

docker node ls
docker node ps $(docker node ls -q)

for id in $(docker node ps -f "name=teststack_kafka" -q $(docker node ls -q)); do
  echo "Loading data on container $id"
  docker exec -it $id /bin/bash /run/secrets/test_data.sh
done
