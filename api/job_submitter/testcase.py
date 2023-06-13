import json
import os
from typing import List
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
IMAGE_QUEUE_NAME = os.getenv("RABBITMQ_TEST_IMAGE_QUEUE")

INVALIDMQ_HOST = "jj"
INVALIDMQ_PORT = 18

JSON_MESSAGE1 = json.dumps(
    {
        "url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg",
        "job_id": 123,
    }
)
JSON_MESSAGE2 = json.dumps(
    {
        "url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg",
        "job_id": 134,
    }
)


JOB_ID_TESTCASES = [
    {"url": "test_img/tmp_img1.jpeg"},
    {"url": "test_img/tmp_img2.jpeg"},
    {"url": "test_img/tmp_img3.jpeg"},
    {"url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg"},
]
VALID_URL_TESTCASES = [
    {"url": "test_img/tmp_img1.jpeg", "expected_output": False},
    {"url": "someString", "expected_output": False},
    {"url": 1233444, "expected_output": False},
    {
        "url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg",
        "expected_output": True,
    },
]

RABBITMQ_MESSAGES = [
    {"message": "hello1"},
    {"message": "hello2"},
]

RABBITMQ_PUBLISH_FUNC_TESTCASES = [
    {
        "message": JSON_MESSAGE1,
        "host": RABBITMQ_HOST,
        "port": RABBITMQ_PORT,
        "queue": IMAGE_QUEUE_NAME,
        "expected_output": True,
    },
    {
        "message": JSON_MESSAGE2,
        "host": RABBITMQ_HOST,
        "port": RABBITMQ_PORT,
        "queue": IMAGE_QUEUE_NAME,
        "expected_output": True,
    },
    {
        "message": JSON_MESSAGE2,
        "host": INVALIDMQ_HOST,
        "port": RABBITMQ_PORT,
        "queue": IMAGE_QUEUE_NAME,
        "expected_output": False,
    },
    {
        "message": JSON_MESSAGE2,
        "host": RABBITMQ_HOST,
        "port": INVALIDMQ_PORT,
        "queue": IMAGE_QUEUE_NAME,
        "expected_output": False,
    },
]

CONVERT_ENDPOINT_TESTCASES = [
    {
        "api_key": None,
        "body": {"url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg"},
        "expected_status": 401,
        "expected_ok": False,
        "expected_content": "Invalid or missing API Key",
    },
    {
        "api_key": "wrongKey",
        "body": {"url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg"},
        "expected_status": 401,
        "expected_ok": False,
        "expected_content": "Invalid or missing API Key",
    },
    {
        "api_key": "mond",
        "body": {"url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg"},
        "expected_status": 200,
        "expected_ok": True,
        "expected_content": "Job Submitted",
    },
    {
        "api_key": "1234",
        "body": {"url": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg"},
        "expected_status": 200,
        "expected_ok": True,
        "expected_content": "Job Submitted",
    },
    {
        "api_key": "1234",
        "body": {"url": "any string"},
        "expected_status": 400,
        "expected_ok": False,
        "expected_content": "URL in the body is not valid URL",
    },
    {
        "api_key": "1234",
        "body": {
            "some_field": "https://hatrabbits.com/wp-content/uploads/2017/01/random.jpg"
        },
        "expected_status": 422,
        "expected_ok": False,
    },
    {
        "api_key": "1234",
        "body": None,
        "expected_status": 422,
        "expected_ok": False,
    },
]


def start_consumer(channel, queue_name=IMAGE_QUEUE_NAME) -> List[str]:
    msg_list = []

    def callback(ch, method, properties, body):
        nonlocal msg_list
        msg_list.append(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    channel.basic_consume(
        queue=IMAGE_QUEUE_NAME,
        on_message_callback=callback,
    )
    channel.basic_qos(prefetch_count=1)
    channel.start_consuming()

    return msg_list
