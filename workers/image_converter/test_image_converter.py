import unittest
import os
import json
import time
import requests
from PIL import Image
import base64
import io
from main import greyscale_image, write_to_influxdb, process_image
from testcase import (
    REDIS_TESTCASES,
    GREYSCALE_TESTCASES,
    RABBITMQ_MESSAGES,
    PROCESS_IMAGE_TESTCASES,
    RECEIVE_AND_CONVERT_MESSAGES,
    WRITE_TO_INFLUXDB_TESTCASES,
    INFLUXDB_TESTCASES,
    start_consumer,
)
from pika import BasicProperties, BlockingConnection, ConnectionParameters, spec
from pika.adapters.blocking_connection import BlockingChannel
import redis
from dotenv import load_dotenv
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
IMAGE_QUEUE_NAME = os.getenv("RABBITMQ_TEST_IMAGE_QUEUE")
RESULT_QUEUE_NAME = os.getenv("RABBITMQ_TEST_RESULT_QUEUE")
TEST_QUEUE_NAME = os.getenv("RABBITMQ_TEST_QUEUE")

INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG")
INFLUXDB_TEST_BUCKET = os.environ.get("INFLUXDB_TEST_BUCKET")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL")
INFLUXDB_MEASUREMENT = os.environ.get("INFLUXDB_MEASUREMENT")

TIMEOUT = 100

redis_db = redis.StrictRedis(
    host=os.environ.get("REDIS_HOST"),
    port=os.environ.get("REDIS_PORT"),
    decode_responses=True,
)

influx_write_client = influxdb_client.InfluxDBClient(
    url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
)
write_api = influx_write_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_write_client.query_api()


class ImageConverterTestcase(unittest.TestCase):
    def test_rabbitmq(self):
        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        self.assertEqual(type(connection), BlockingConnection)
        channel = connection.channel()
        self.assertEqual(type(channel), BlockingChannel)
        channel.queue_purge(queue=RESULT_QUEUE_NAME)

        channel.queue_declare(queue=TEST_QUEUE_NAME, durable=True)

        for testcase in RABBITMQ_MESSAGES:
            channel.basic_publish(
                exchange="",
                routing_key=TEST_QUEUE_NAME,
                body=testcase["message"],
                properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
            )

        msg_list = start_consumer(channel=channel, queue_name=TEST_QUEUE_NAME)
        msg_list += start_consumer(channel=channel, queue_name=TEST_QUEUE_NAME)

        channel.close()

        self.assertEqual(len(msg_list), len(RABBITMQ_MESSAGES))
        for msg, testcase in zip(msg_list, RABBITMQ_MESSAGES):
            self.assertEqual(msg.decode("utf-8"), testcase["message"])

    def test_influxdb(self):
        for testcase in INFLUXDB_TESTCASES:
            point = Point(testcase["measurement"]).field(
                testcase["field"], testcase["value"]
            )
            write_api.write(bucket=testcase["bucket"], org=INFLUXDB_ORG, record=point)
            query = f"""from(bucket: "{testcase["bucket"]}")
                    |> range(start: -10m)
                    |> filter(fn: (r) => r._measurement == "{testcase["measurement"]}")"""
            tables = query_api.query(query, org=INFLUXDB_ORG)

            self.assertEqual(tables[0].records[-1]["_value"], testcase["value"])
            self.assertEqual(tables[0].records[-1]["_field"], testcase["field"])

    def test_write_to_influxdb(self):
        for testcase in WRITE_TO_INFLUXDB_TESTCASES:
            status = write_to_influxdb(
                values=testcase["values"],
                fields=testcase["fields"],
                bucket=testcase["bucket"],
                measurement=testcase["measurement"],
            )
            self.assertEqual(status, testcase["expected_status"])

            if status:
                query = f"""from(bucket: "{testcase["bucket"]}")
                        |> range(start: -10m)
                        |> filter(fn: (r) => r._measurement == "{testcase["measurement"]}")"""
                tables = query_api.query(query, org=INFLUXDB_ORG)

                for field, value in zip(testcase["fields"], testcase["values"]):
                    self.assertEqual(
                        tables[0].records[-len(testcase["values"])]["_value"], value
                    )
                    self.assertEqual(
                        tables[0].records[-len(testcase["values"])]["_field"], field
                    )

    def test_redis(self):
        for testcase in REDIS_TESTCASES:
            if "exp_time" in testcase:
                redis_db.set(testcase["key"], testcase["value"], testcase["exp_time"])
                res = redis_db.get(testcase["key"])
                self.assertEqual(res, testcase["value"])
                time.sleep(testcase["exp_time"])
                res = redis_db.get(testcase["key"])
                self.assertEqual(res, None)
            else:
                redis_db.set(testcase["key"], testcase["value"])
                res = redis_db.get(testcase["key"])
                self.assertEqual(res, testcase["value"])
                res = redis_db.get(testcase["not_exist_key"])
                self.assertEqual(res, None)

    # def test_greyscale_image(self):
    #     for testcase in GREYSCALE_TESTCASES:
    #         res = requests.get(testcase["url"], timeout=100)
    #         bytes_img = res.content
    #         img = Image.open(io.BytesIO(bytes_img))

    #         greyscaled_img = greyscale_image(img)
    #         greyscaled = base64.b64encode(greyscaled_img)
    #         self.assertEqual(greyscaled, testcase["expected_encode_img"])

    def test_process_image(self):
        for testcase in PROCESS_IMAGE_TESTCASES:
            encoded_body, _, finished = process_image(testcase["body"])
            # self.assertEqual(encoded_body["image"], testcase["expected_encode_img"])
            self.assertEqual(
                encoded_body["result_type"], testcase["expected_result_type"]
            )
            self.assertEqual(finished, testcase["expected_finished_status"])

    def test_message_receive_and_convert(self):
        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        self.assertEqual(type(connection), BlockingConnection)
        channel = connection.channel()
        self.assertEqual(type(channel), BlockingChannel)
        channel.queue_purge(queue=IMAGE_QUEUE_NAME)
        channel.queue_declare(queue=IMAGE_QUEUE_NAME, durable=True)
        channel.queue_purge(queue=RESULT_QUEUE_NAME)
        channel.queue_declare(queue=RESULT_QUEUE_NAME, durable=True)

        for testcase in RECEIVE_AND_CONVERT_MESSAGES:
            channel.basic_publish(
                exchange="",
                routing_key=IMAGE_QUEUE_NAME,
                body=testcase["message"],
                properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
            )

        msg_list = start_consumer(channel=channel, queue_name=RESULT_QUEUE_NAME)
        msg_list += start_consumer(channel=channel, queue_name=RESULT_QUEUE_NAME)

        self.assertEqual(len(msg_list), len(RECEIVE_AND_CONVERT_MESSAGES))

        # for msg, testcase in zip(msg_list, RECEIVE_AND_CONVERT_MESSAGES):
        #     self.assertEqual(msg.decode("utf-8"), testcase["message"])


if __name__ == "__main__":
    unittest.main()
