#!/usr/bin/env bash

echo "Docker stack ps"
docker stack ps teststack

for id in $(docker stack ps teststack -f 'name=teststack_kafka' -q); do
  echo "Loading data on node $id"
done

echo "Docker service ps"
docker service ps

for service in kafka_ssh kafka_local; do
  echo "Service $service"
  id="$(docker service ps -f "name=teststack_$service.1" "teststack_$service" --no-trunc -q | head -n1)"
  echo "Loading data on container teststack_$service.1.$id"
  docker exec -it "teststack_$service.1.$id" /bin/bash /run/secrets/test_data.sh
done
