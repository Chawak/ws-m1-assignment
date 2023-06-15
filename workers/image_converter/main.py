import io
import os
import json
import argparse
import base64
import requests
import redis
import imagehash
import time
from dotenv import load_dotenv
from pika import BlockingConnection, ConnectionParameters, BasicProperties, spec
from PIL import Image, ImageOps
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS

parser = argparse.ArgumentParser()
parser.add_argument("-test", action="store_true")
args = parser.parse_args()

load_dotenv()
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
IMG_REQUEST_TIMEOUT = os.getenv("IMG_REQUEST_TIMEOUT")

INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL")
INFLUXDB_MEASUREMENT = os.environ.get("INFLUXDB_MEASUREMENT")

if args.test:
    RABBITMQ_IMAGE_QUEUE = os.getenv("RABBITMQ_TEST_IMAGE_QUEUE")
    RABBITMQ_RESULT_QUEUE = os.getenv("RABBITMQ_TEST_RESULT_QUEUE")

else:
    RABBITMQ_IMAGE_QUEUE = os.getenv("RABBITMQ_IMAGE_QUEUE")
    RABBITMQ_RESULT_QUEUE = os.getenv("RABBITMQ_RESULT_QUEUE")

REDIS_EXP_TIME = int(os.environ.get("REDIS_EXP_TIME"))
REDIS_IMG_KEY_PREFIX = os.environ.get("REDIS_IMG_KEY_PREFIX")
REDIS_ATTEMPT_KEY_PREFIX = os.environ.get("REDIS_ATTEMPT_KEY_PREFIX")
MAX_CONVERT_ATTEMPT = int(os.environ.get("MAX_CONVERT_ATTEMPT"))

redis_db = redis.StrictRedis(
    host=os.environ.get("REDIS_HOST"),
    port=os.environ.get("REDIS_PORT"),
    decode_responses=True,
)


influx_write_client = influxdb_client.InfluxDBClient(
    url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
)
write_api = influx_write_client.write_api(write_options=SYNCHRONOUS)


def write_to_influxdb(value, field):
    point = Point(INFLUXDB_MEASUREMENT).field(field, value)
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)


def process_image(body) -> tuple[dict, float, bool]:
    body = json.loads(body)

    encoded_body = {"job_id": body.job_id}

    res = requests.get(body.url, timeout=IMG_REQUEST_TIMEOUT)

    if not res.ok:
        key = REDIS_ATTEMPT_KEY_PREFIX + hash(body.url)
        attempt_count = redis_db.get(key)

        if not attempt_count:
            attempt_count = 0

        if attempt_count < MAX_CONVERT_ATTEMPT:
            finished = False
            redis_db.set(key, int(attempt_count) + 1, REDIS_EXP_TIME)
        else:
            finished = True

        encoded_body["image"] = "ImageNotFound"
        encoded_body["result_type"] = "None"
        return encoded_body, 0, finished

    bytes_img = res.content
    img = Image.open(io.BytesIO(bytes_img))
    hashed_img = imagehash.phash(img)

    start_time = time.perf_counter()

    cached_img = redis_db.get(REDIS_IMG_KEY_PREFIX + hashed_img)
    if cached_img:
        encoded_body["image"] = cached_img
        encoded_body["result_type"] = "cached"
    else:
        converted_img = greyscale_image(img)
        encoded_img = base64.b64encode(converted_img).decode("ascii")
        encoded_body["image"] = encoded_img
        encoded_body["result_type"] = "convert"
        redis_db.set(REDIS_IMG_KEY_PREFIX + hashed_img, encoded_img, ex=REDIS_EXP_TIME)

    process_time = time.perf_counter() - start_time

    return encoded_body, process_time, True


def greyscale_image(img) -> str:
    greyscaled_img = ImageOps.grayscale(img)
    greyscaled_bytes = io.BytesIO()
    greyscaled_img.save(greyscaled_bytes, format="PNG")
    greyscaled_bytes = greyscaled_bytes.getvalue()
    return greyscaled_bytes


def main():
    mq_connection = BlockingConnection(
        ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )

    mq_channel = mq_connection.channel()
    mq_channel.queue_declare(queue=RABBITMQ_IMAGE_QUEUE, durable=True)
    mq_channel.queue_declare(queue=RABBITMQ_RESULT_QUEUE, durable=True)

    def callback(ch, method, properties, body):
        encoded_body, process_time, finish = process_image(body)

        write_to_influxdb(value=process_time, field="process_time")

        write_to_influxdb(value=encoded_body["is_cached"], field="is_cached")

        if finish:
            ch.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_publish(
            exchange="",
            routing_key=RABBITMQ_RESULT_QUEUE,
            body=json.dumps(encoded_body),
            properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
        )

    mq_channel.basic_consume(queue=RABBITMQ_IMAGE_QUEUE, on_message_callback=callback)
    mq_channel.basic_qos(prefetch_count=1)

    mq_channel.start_consuming()


if __name__ == "__main__":
    print("Running a saver worker...")
    print("Message receive from queue:", RABBITMQ_IMAGE_QUEUE)
    print("Message send to queue:", RABBITMQ_RESULT_QUEUE)
    main()
