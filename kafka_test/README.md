# Setup Kafka cluster to test the Kafka-ADB connector
This directory contains scripts and data to setup a Kafka cluster to test Kafka-ADB connector. These tests check how the connector interacts with Kafka and whether `librdkafka` works properly.

Docker and docker-compose are used to run a Kafka cluster. A [shortened version](./docker-compose.yml) of an [official docker-compose distribution of Kafka](https://github.com/confluentinc/cp-all-in-one) is used.

## Requirements
For automated CI run in a clean environment, a compound script `complete_ci_setup.sh` is provided. However, it makes the following assumptions about the environment:
* `python3` is installed and `pip3` is available
* `docker` and `docker-compose` are available
    * [`docker` installation instructions](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository)
    * [`docker-compose` installation instructions](https://docs.docker.com/compose/install/)

## Makefile
A `Makefile` for automated calls of test environment setup from the top-level Makefile is provided. It defines two targets:
* `all`: launches `complete_ci_setup.sh`
* `clean`: launches `clear_topics.sh`, then `clear_docker.sh`

## Files
Other entities in the current directory are:
* `producer.py`: a python3 script to produce AVRO-serialized data. It supports `--help`
    * `requirements.txt`: requirements to run `producer.py`
    * `data`: Contains data and schema used by `producer.py`
* `setup_topics.sh`: bash script to initialize topics *inside a running Docker container* with Kafka
* `clear_topics.sh`: bash script to drop topics created by `setup_topics.sh`
* `clear_docker.sh`: bash script to remove Docker containers created by `complete_ci_setup.sh`
* `setup_data.sh`: bash script running `producer.py` to fill the topics created by `setup_topics.sh`
