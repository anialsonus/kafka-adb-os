#!/usr/bin/env python3

"""
Kafka producer for AVRO-serialized data.
"""

import argparse
import array
import datetime
import json

from io import BytesIO

from confluent_kafka import Producer

from fastavro import writer


def conversion_hook(json_dict):
    """
    A hook to convert some JSON-encoded types to Python types
    """
    for (key, value) in json_dict.items():
        if type(value) == str:
            try:
                key_date = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                unix_date = datetime.datetime(1970, 1, 1)
                json_dict[key] = (key_date - unix_date).days
            except (TypeError, ValueError) as e:
                pass
        if type(value) == list:
            json_dict[key] = array.array('B', value).tobytes()

    return json_dict

class MessageBytes(BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False

    def __len__(self):
        return len(self)

def serialized_records(schema, records):
    with MessageBytes() as message:
        writer(message, schema, records)
        return message.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print('Failed: {}'.format(err))
    else:
        print('Delivered to {}:{}'.format(msg.topic(), msg.partition()))

def get_arguments():
    parser = argparse.ArgumentParser(description="Kafka producer populating Kafka topic with test data serialized with test AVRO schema")
    kafka = parser.add_argument_group("Kafka", "Kafka settings")
    kafka.add_argument("-b", "--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers, separated by comma (default: %(default)s)")
    kafka.add_argument("-t", "--topic", required=True, help="Kafka topic")
    kafka.add_argument("-p", "--partition", type=int, default=0, help="Kafka partition")
    kafka.add_argument("-s", "--schema", help="AVRO schema (JSON)")
    kafka.add_argument("-d", "--data", help="Values (JSON)")
    return parser.parse_args()

def main():
    args = get_arguments()

    schema = json.loads(open(args.schema, "r").read())
    records = json.loads(open(args.data, "r").read(), object_hook=conversion_hook)

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    producer.poll(0)
    producer.produce(args.topic, serialized_records(schema, records), callback=delivery_report, partition=args.partition)
    producer.flush()


if __name__ == "__main__":
    main()
