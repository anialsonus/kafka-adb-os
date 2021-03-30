#!/usr/bin/env bash

# The container name where *Kafka* instance runs
CONTAINER=broker

COMMAND="docker exec -it $CONTAINER kafka-topics --bootstrap-server localhost:9092"


$COMMAND --create --topic kadb_fdw_test --partitions 4
$COMMAND --create --topic kadb_fdw_test_single_partition --partitions 1
$COMMAND --create --topic kadb_fdw_test_empty --partitions 1

$COMMAND --create --topic kadb_fdw_test_avro --partitions 1
