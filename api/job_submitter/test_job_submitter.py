import unittest
import os
from main import generate_job_id, check_if_valid_url, rabbitmq_publish
from testcase import (
    JOB_ID_TESTCASES,
    CONVERT_ENDPOINT_TESTCASES,
    VALID_URL_TESTCASES,
    RABBITMQ_MESSAGES,
    RABBITMQ_PUBLISH_FUNC_TESTCASES,
    start_consumer,
)
from pika import BasicProperties, BlockingConnection, ConnectionParameters, spec
from pika.adapters.blocking_connection import BlockingChannel
import requests
from dotenv import load_dotenv

load_dotenv()
APP_HOST = os.getenv("APP_HOST")
APP_PORT = os.getenv("APP_PORT")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
IMAGE_QUEUE_NAME = os.getenv("RABBITMQ_TEST_IMAGE_QUEUE")
TEST_QUEUE_NAME = os.getenv("RABBITMQ_TEST_QUEUE")


TIMEOUT = 100


class ResultCheckerTestcase(unittest.TestCase):
    def test_rabbitmq(self):
        # credentials = pika.PlainCredentials('username', 'password')
        # parameters = pika.ConnectionParameters(credentials=credentials)
        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        self.assertEqual(type(connection), BlockingConnection)
        channel = connection.channel()
        self.assertEqual(type(channel), BlockingChannel)
        channel.queue_purge(queue=TEST_QUEUE_NAME)

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

    def test_rabbitmq_publish(self):
        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel()
        channel.queue_purge(queue=IMAGE_QUEUE_NAME)
        channel.close()

        for testcase in RABBITMQ_PUBLISH_FUNC_TESTCASES:
            output = rabbitmq_publish(
                testcase["message"],
                host=testcase["host"],
                port=testcase["port"],
                queue=testcase["queue"],
            )
            self.assertEqual(output, testcase["expected_output"])

        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel()

        channel.queue_declare(queue=IMAGE_QUEUE_NAME, durable=True)

        expected_messages = [
            item["message"]
            for item in RABBITMQ_PUBLISH_FUNC_TESTCASES
            if item["expected_output"]
        ]
        msg_list = start_consumer(channel=channel, queue_name=IMAGE_QUEUE_NAME)

        for _ in range(len(expected_messages) - 1):
            msg_list += start_consumer(channel=channel, queue_name=IMAGE_QUEUE_NAME)
        channel.close()

        self.assertEqual(len(msg_list), len(expected_messages))
        for msg, expected_msg in zip(msg_list, expected_messages):
            self.assertEqual(msg.decode("utf-8"), expected_msg)

    def test_is_valid_url(self):
        for testcase in VALID_URL_TESTCASES:
            result = check_if_valid_url(testcase["url"])
            self.assertEqual(result, testcase["expected_output"])

    def test_gen_job_id(self):
        for testcase in JOB_ID_TESTCASES:
            result = generate_job_id(testcase["url"])
            self.assertEqual(type(result), int)
            self.assertGreaterEqual(result, 0)

    def test_healthcheck_endpoint(self):
        res = requests.get(f"http://{APP_HOST}:{APP_PORT}/healthcheck", timeout=TIMEOUT)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.ok, True)
        self.assertEqual(res.content, b'"Ok"')
        self.assertEqual(res.text, '"Ok"')

    def test_convert_endpoint(self):
        for testcase in CONVERT_ENDPOINT_TESTCASES:
            res = requests.post(
                f"http://{APP_HOST}:{APP_PORT}/convert",
                json=testcase["body"],
                headers={"x-api-key": testcase["api_key"]},
                timeout=TIMEOUT,
            )

            self.assertEqual(res.status_code, testcase["expected_status"])
            self.assertEqual(res.ok, testcase["expected_ok"])

            if "expected_content" in testcase:
                self.assertEqual(res.json()["detail"], testcase["expected_content"])


if __name__ == "__main__":
    unittest.main()
