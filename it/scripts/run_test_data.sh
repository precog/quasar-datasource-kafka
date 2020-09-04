#!/usr/bin/env bash

docker node ls

if [[ -x /tmp/docker-machine ]]; then
  /tmp/docker-machine ls
fi

for node in $(docker node ls -q); do
  echo "Node $node"
  if [[ -x /tmp/docker-machine ]]; then
    eval $(/tmp/docker-machine env "$node")
  else
    echo "Skipping docker-machine (not available)"
  fi
  docker node ps -f name=teststack_kafka "$node"
  for service in $(docker node ps --format="{{.Name}}.{{.ID}}" --no-trunc -f name=teststack_kafka "$node"); do
    echo "Loading test data on $service"
    docker exec -it "$service" /bin/bash /run/secrets/test_data.sh
  done
done
