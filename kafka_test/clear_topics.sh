#!/usr/bin/env bash

# The container name where *Kafka* instance runs
CONTAINER=broker

COMMAND="docker exec -it $CONTAINER kafka-topics --bootstrap-server localhost:9092"


$COMMAND --delete --topic kadb_fdw_test
$COMMAND --delete --topic kadb_fdw_test_single_partition
$COMMAND --delete --topic kadb_fdw_test_empty
