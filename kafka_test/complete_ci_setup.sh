#!/usr/bin/env bash

if ! command -v docker-compose &> /dev/null
then
    echo "docker-compose not found"
    exit 1
fi

if ! command -v pip3 &> /dev/null
then
    echo "pip3 not found"
    exit 1
fi


docker-compose up -d

echo "Sleeping for 10 seconds in order to wait while Kafka cluster starts..."
sleep 10
echo "Waking up"

./setup_topics.sh

pip3 install -r requirements.txt
./setup_data.sh
