import os
import json
import pymongo
from dotenv import load_dotenv
from pika import BlockingConnection, ConnectionParameters
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-test", action="store_true")
args = parser.parse_args()

load_dotenv()
MONGO_HOST = os.getenv("MONGO_HOST")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")

if args.test:
    RABBITMQ_RESULT_QUEUE = os.getenv("RABBITMQ_TEST_RESULT_QUEUE")
    MONGO_DB_NAME = os.getenv("MONGO_TEST_DB_NAME")
    MONGO_COL_NAME = os.getenv("MONGO_TEST_COL_NAME")
else:
    RABBITMQ_RESULT_QUEUE = os.getenv("RABBITMQ_RESULT_QUEUE")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
    MONGO_COL_NAME = os.getenv("MONGO_COL_NAME")

mongo_db = pymongo.MongoClient(MONGO_HOST)


def mongo_insert(
    db_name: str = MONGO_DB_NAME,
    col_name: str = MONGO_COL_NAME,
    obj: dict = {},
) -> bool:
    try:
        result = mongo_db[db_name][col_name].insert_one(obj)

        return result.acknowledged
    except pymongo.errors.DuplicateKeyError:
        return False


def main():
    mq_connection = BlockingConnection(
        ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )

    mq_channel = mq_connection.channel()
    mq_channel.queue_declare(queue=RABBITMQ_RESULT_QUEUE, durable=True)

    def callback(ch, method, properties, body):
        write_ack = mongo_insert(
            db_name=MONGO_DB_NAME, col_name=MONGO_COL_NAME, obj=json.loads(body)
        )

        if write_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    mq_channel.basic_consume(queue=RABBITMQ_RESULT_QUEUE, on_message_callback=callback)
    mq_channel.basic_qos(prefetch_count=1)

    mq_channel.start_consuming()


if __name__ == "__main__":
    print("Running a saver worker...")
    print("Message receive from queue:", RABBITMQ_RESULT_QUEUE)
    main()
