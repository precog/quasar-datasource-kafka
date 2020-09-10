#!/usr/bin/env bash

get_containers() {
  docker container ps
  mapfile -t < <(docker container ps -f "name=teststack_kafka" --format "{{.ID}}")
}

get_containers

COUNT=0
while [[ ${#MAPFILE[@]} -eq 0 ]]; do
  COUNT=$((COUNT + 1))
  if [[ $COUNT -gt 10 ]]; then
    echo >&2 "Unable to retrieve containers"
    exit 1
  fi
  echo "No containers found, waiting..."
  sleep 6
  get_containers
done

for id in "${MAPFILE[@]}"; do
  echo "Loading data on container $id"
  docker exec -it $id /bin/bash /run/secrets/test_data.sh
done
