import os
import json
import pymongo
from dotenv import load_dotenv
from pika import BasicProperties, BlockingConnection, ConnectionParameters, spec
from pika.exceptions import AMQPConnectionError

load_dotenv()
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COL_NAME = os.getenv("MONGO_COL_NAME")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_RESULT_QUEUE = os.getenv("RABBITMQ_RESULT_QUEUE")


def main():
    mongodb = pymongo.MongoClient(MONGO_HOST)

    mq_connection = BlockingConnection(
        ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )

    mq_channel = mq_connection.channel()

    def callback(ch, method, properties, body):
        res = mongodb[MONGO_DB_NAME][MONGO_COL_NAME].insert_one(json.loads(body))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    mq_channel.basic_consume(queue=RABBITMQ_RESULT_QUEUE, on_message_callback=callback)
    mq_channel.basic_qos(prefetch_count=1)

    mq_channel.start_consuming()
