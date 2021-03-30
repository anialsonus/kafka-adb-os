#!/usr/bin/env bash

BROKER="localhost:9092"


./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test -p 0
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test -p 0
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test -p 1
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test -p 3
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test -p 3
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test -p 3

./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test_single_partition -p 0
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test_single_partition -p 0
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test_single_partition -p 0
./producer.py -b $BROKER -s data/value_schema.json -d data/value_records.json -t kadb_fdw_test_single_partition -p 0

./producer.py -b $BROKER -s data/avro_schema.json -d data/avro_records.json -t kadb_fdw_test_avro
./producer.py -b $BROKER -s data/avro_schema.json -d data/avro_records.json -t kadb_fdw_test_avro
