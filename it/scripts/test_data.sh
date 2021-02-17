#!/usr/bin/env bash

echo "Creating topics"

COUNT=0
"${KAFKA_HOME}/bin/kafka-topics.sh" --zookeeper localhost --list
while [[ $? -ne 0 ]]; do
  COUNT=$((COUNT + 1))
  if [[ $COUNT -gt 12 ]]; then
    echo >&2 "Kafka not started. Giving up."
    exit 1
  fi
  echo "Kafka not started. Waiting."
  sleep 5
  "${KAFKA_HOME}/bin/kafka-topics.sh" --zookeeper localhost --list
done

for topic in empty keyOnly valueOnly offsetsAll; do
  "${KAFKA_HOME}/bin/kafka-topics.sh" --zookeeper localhost --create --topic "$topic" --partitions 1 --replication-factor 1
done

for topic in keyAndValue partitioned offsetsPartitioned; do
  "${KAFKA_HOME}/bin/kafka-topics.sh" --zookeeper localhost --create --topic "$topic" --partitions 5 --replication-factor 1
done

echo "Loading test data"

time "${KAFKA_HOME}/bin/kafka-console-producer.sh" --broker-list localhost:9092 --sync --topic keyOnly \
  --property "parse.key=true" \
  --property "key.separator=|" << EOF
false|
EOF

time "${KAFKA_HOME}/bin/kafka-console-producer.sh" --broker-list localhost:9092 --sync --topic valueOnly << EOF
{ "key" : "value" }
[1, 2, 3]
"string"
EOF

time "${KAFKA_HOME}/bin/kafka-console-producer.sh" --broker-list localhost:9092 --sync --topic keyAndValue \
  --property "parse.key=true" \
  --property "key.separator=|" << EOF
"key"|"value"
[1, 2, 3]|true
EOF

time "${KAFKA_HOME}/bin/kafka-console-producer.sh" --broker-list localhost:9092 --sync --topic partitioned \
  <<< "$(printf '{ "number": %d }' $(seq 1 50))"

if [[ "$ADVERTISED_HOST" == "kafka_ssh" ]]; then
  echo "Single broker topic and test data"
  "${KAFKA_HOME}/bin/kafka-topics.sh" --zookeeper localhost --create --topic sameServer --partitions 1 --replication-factor 1
  "${KAFKA_HOME}/bin/kafka-console-producer.sh" --broker-list localhost:9092 --sync --topic sameServer << EOF
"same"
"server"
EOF
fi
