import base64
import os
from typing import List
import json
from dotenv import load_dotenv
import requests


load_dotenv()
MONGO_TEST_DB_NAME = os.getenv("MONGO_TEST_DB_NAME")
MONGO_TEST_COL_NAME = os.getenv("MONGO_TEST_COL_NAME")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RESULT_QUEUE_NAME = os.getenv("RABBITMQ_TEST_RESULT_QUEUE")
TEST_QUEUE_NAME = os.getenv("RABBITMQ_TEST_QUEUE")

INVALIDMQ_HOST = "jj"
INVALIDMQ_PORT = 18

DUMMY_DATAS = [
    {
        "job_id": 0,
        "image": "https://onlinejpgtools.com/images/examples-onlinejpgtools/random-grid.jpg",
        "result_type": "cached",
    },
    {
        "job_id": 1,
        "image": "https://onlineimagetools.com/images/examples-onlineimagetools/color-grid.png",
        "result_type": "cached",
    },
    {
        "job_id": 2,
        "image": "https://onlineimagetools.com/images/examples-onlineimagetools/orange-shades.png",
        "result_type": "convert",
    },
]

for i, data in enumerate(DUMMY_DATAS):
    img = requests.get(data["image"], timeout=100)
    DUMMY_DATAS[i]["image"] = base64.b64encode(img.content).decode("ascii")

NOT_EXIST = "NotExistName"
UPDATE = "UPDATE"
QUERY = "QUERY"
DELETE = "DELETE"

MONGO_TESTCASES = [
    {"command": QUERY, "object": {"job_id": 0}, "ans": [DUMMY_DATAS[0]]},
    {
        "command": DELETE,
        "object": {"job_id": 0},
    },
    {"command": QUERY, "object": {"job_id": 0}, "ans": []},
    {"command": QUERY, "object": {"job_id": 1}, "ans": [DUMMY_DATAS[1]]},
    {
        "command": UPDATE,
        "object": {"job_id": 1},
        "new_object": {"$set": {"image": "thisIsImgButUpdated"}},
    },
    {
        "command": QUERY,
        "object": {"job_id": 1},
        "len_ans": 1,
        "ans": [{"job_id": 1, "image": "thisIsImgButUpdated", "result_type": "cached"}],
    },
]

RABBITMQ_MESSAGES = [
    {"message": "hello1"},
    {"message": "hello2"},
]


INSERT_FUNC_TESTCASES = [
    {
        "object": DUMMY_DATAS[0],
        "ans": [DUMMY_DATAS[0]],
        "len_cum_query": 1,
        "expected_ack": True,
    },
    {
        "object": DUMMY_DATAS[0],
        "ans": [DUMMY_DATAS[0]],
        "len_cum_query": 1,
        "expected_ack": False,
    },
    {
        "object": DUMMY_DATAS[1],
        "ans": [DUMMY_DATAS[1]],
        "len_cum_query": 2,
        "expected_ack": True,
    },
]

RECEIVE_AND_SAVE_MESSAGES = [
    {"message": json.dumps(DUMMY_DATAS[0]), "ans": [DUMMY_DATAS[0]]},
    {"message": json.dumps(DUMMY_DATAS[1]), "ans": [DUMMY_DATAS[1]]},
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
