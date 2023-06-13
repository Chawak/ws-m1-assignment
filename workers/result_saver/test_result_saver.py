import unittest
import os
import json
from main import mongo_insert
from testcase import (
    MONGO_TESTCASES,
    DUMMY_DATAS,
    RABBITMQ_MESSAGES,
    INSERT_FUNC_TESTCASES,
    RECEIVE_AND_SAVE_MESSAGES,
    start_consumer,
)
from pika import BasicProperties, BlockingConnection, ConnectionParameters, spec
from pika.adapters.blocking_connection import BlockingChannel
import requests
import pymongo
from dotenv import load_dotenv

load_dotenv()
APP_HOST = os.getenv("APP_HOST")
APP_PORT = os.getenv("APP_PORT")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RESULT_QUEUE_NAME = os.getenv("RABBITMQ_TEST_RESULT_QUEUE")

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_TEST_DB_NAME = os.getenv("MONGO_TEST_DB_NAME")
MONGO_TEST_COL_NAME = os.getenv("MONGO_TEST_COL_NAME")


TIMEOUT = 100

mongo_db = pymongo.MongoClient(MONGO_HOST)


class ResultSaverTestcase(unittest.TestCase):
    def test_rabbitmq(self):
        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        self.assertEqual(type(connection), BlockingConnection)
        channel = connection.channel()
        self.assertEqual(type(channel), BlockingChannel)
        channel.queue_delete(queue=RESULT_QUEUE_NAME)

        channel.queue_declare(queue=RESULT_QUEUE_NAME, durable=True)

        for testcase in RABBITMQ_MESSAGES:
            channel.basic_publish(
                exchange="",
                routing_key=RESULT_QUEUE_NAME,
                body=testcase["message"],
                properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
            )

        msg_list = start_consumer(channel=channel)
        msg_list += start_consumer(channel=channel)

        channel.close()

        self.assertEqual(len(msg_list), len(RABBITMQ_MESSAGES))
        for msg, testcase in zip(msg_list, RABBITMQ_MESSAGES):
            self.assertEqual(msg.decode("utf-8"), testcase["message"])

    def test_mongodb(self):
        collection = mongo_db[MONGO_TEST_DB_NAME][MONGO_TEST_COL_NAME]
        collection.delete_many({})
        collection.insert_many(DUMMY_DATAS)

        for testcase in MONGO_TESTCASES:
            if testcase["command"] == "QUERY":
                res = collection.find(testcase["object"])
                res_list = [data for data in res]

                self.assertEqual(len(res_list), len(testcase["ans"]))
                res_list = sorted(res_list, key=lambda d: d["job_id"])

                for i in range(len(testcase["ans"])):
                    for field in testcase["ans"][i]:
                        self.assertEqual(res_list[i][field], testcase["ans"][i][field])

            elif testcase["command"] == "DELETE":
                collection.delete_one(testcase["object"])
            elif testcase["command"] == "UPDATE":
                collection.update_one(testcase["object"], testcase["new_object"])
        collection.delete_many({})

    def test_mongo_insert(self):
        collection = mongo_db[MONGO_TEST_DB_NAME][MONGO_TEST_COL_NAME]

        RANDOM_FIELD = list(INSERT_FUNC_TESTCASES[0]["ans"].keys())[0]
        collection.drop_index(RANDOM_FIELD)
        collection.create_index(RANDOM_FIELD, unique=True)

        collection.delete_many({})

        for testcase in INSERT_FUNC_TESTCASES:
            insert_ack = mongo_insert(
                db_name=MONGO_TEST_DB_NAME,
                col_name=MONGO_TEST_COL_NAME,
                obj=testcase["object"],
            )

            self.assertEqual(insert_ack, testcase["expected_ack"])

            query_all_res = [elem for elem in collection.find({})]
            query_one_res = [elem for elem in collection.find(testcase["object"])]
            self.assertEqual(len(query_all_res), testcase["len_cum_query"])
            self.assertEqual(len(query_one_res), len(testcase["ans"]))

            for res, ans in zip(query_one_res, testcase["ans"]):
                for field in ans:
                    self.assertEqual(res[field], ans[field])
        collection.delete_many({})
        collection.drop_index(RANDOM_FIELD)

    def test_message_receive_and_save(self):
        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        self.assertEqual(type(connection), BlockingConnection)
        channel = connection.channel()
        self.assertEqual(type(channel), BlockingChannel)
        channel.queue_delete(queue=RESULT_QUEUE_NAME)

        channel.queue_declare(queue=RESULT_QUEUE_NAME, durable=True)

        for testcase in RECEIVE_AND_SAVE_MESSAGES:
            channel.basic_publish(
                exchange="",
                routing_key=RESULT_QUEUE_NAME,
                body=testcase["message"],
                properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
            )

        for testcase in RECEIVE_AND_SAVE_MESSAGES:
            result = [
                elem
                for elem in mongo_db[MONGO_TEST_DB_NAME][MONGO_TEST_COL_NAME].find(
                    json.loads(testcase["message"])
                )
            ]

            self.assertEqual(len(result), len(testcase["ans"]))
            for res, ans in zip(result, testcase["ans"]):
                for field in ans:
                    self.assertEqual(res[field], ans[field])


if __name__ == "__main__":
    unittest.main()
