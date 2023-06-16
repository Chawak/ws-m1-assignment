import os
import json
from typing import List
from dotenv import load_dotenv

load_dotenv()

TEST_QUEUE_NAME = os.getenv("TEST_QUEUE_NAME")
INFLUXDB_TESTCASES = [
    {
        "value": 1,
        "field": "field1",
        "bucket": "test",
        "measurement": "measurement1",
    },
    {
        "value": 2,
        "field": "field1",
        "bucket": "test",
        "measurement": "measurement1",
    },
]
WRITE_TO_INFLUXDB_TESTCASES = [
    {
        "values": [1],
        "fields": ["field1"],
        "bucket": "test",
        "measurement": "measurement1",
        "expected_status": True,
    },
    {
        "values": [1],
        "fields": ["field1"],
        "bucket": "NotExist",
        "measurement": "measurement1",
        "expected_status": False,
    },
    {
        "values": [2],
        "fields": ["field2"],
        "bucket": "NotExist",
        "measurement": "measurement1",
        "expected_status": False,
    },
]

REDIS_TESTCASES = [
    {"key": "key1", "value": "val1", "not_exist_key": "key1000"},
    {"key": "key2", "value": "val2", "exp_time": 2},
]

GREYSCALE_TESTCASES = [{"url": "tmp", "expected_encode_img": "tmp2"}]
PROCESS_IMAGE_TESTCASES = [
    {
        "body": json.dumps(
            {
                "url": "https://s3-ap-southeast-1.amazonaws.com/wisesight-images/saai/th/2023/02/09/original_1623585897492131841_146010221.jpg",
                "job_id": 0,
            }
        ),
        "expected_encode_img": "tmp",
        "expected_finished_status": True,
        "expected_result_type": "convert",
    },
    {
        "body": json.dumps(
            {
                "url": "https://s3-ap-southeast-1.amazonaws.com/wisesight-images/saai/th/2023/02/09/original_1623585897492131841_146010221.jpg",
                "job_id": 1,
            }
        ),
        "expected_encode_img": "tmp",
        "expected_finished_status": True,
        "expected_result_type": "cached",
    },
    {
        "body": json.dumps(
            {
                "url": "https://s3-ap-southeast-1.amazonaws.com/wisesight-images/saai/th/2023/02/09/original_1623585896271609856_111018367.jpg",
                "job_id": 0,
            }
        ),
        "expected_encode_img": "tmp",
        "expected_finished_status": True,
        "expected_result_type": "convert",
    },
]

RABBITMQ_MESSAGES = [
    {"message": "hello1"},
    {"message": "hello2"},
]

RECEIVE_AND_CONVERT_MESSAGES = [
    {
        "message": json.dumps(
            {
                "url": "https://s3-ap-southeast-1.amazonaws.com/wisesight-images/saai/th/2023/02/09/original_1623585897492131841_146010221.jpg",
                "job_id": 0,
            }
        )
    },
    {
        "message": json.dumps(
            {
                "url": "https://s3-ap-southeast-1.amazonaws.com/wisesight-images/saai/th/2023/02/09/original_1623585896271609856_111018367.jpg",
                "job_id": 1,
            }
        )
    },
]


def start_consumer(channel, queue_name=TEST_QUEUE_NAME) -> List[str]:
    msg_list = []

    def callback(ch, method, properties, body):
        nonlocal msg_list
        msg_list.append(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
    )
    channel.basic_qos(prefetch_count=1)
    channel.start_consuming()

    return msg_list
