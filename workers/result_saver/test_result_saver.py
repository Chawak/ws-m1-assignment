import unittest
import os
import json
import time
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
import pymongo
from dotenv import load_dotenv

load_dotenv()
APP_HOST = os.getenv("APP_HOST")
APP_PORT = os.getenv("APP_PORT")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RESULT_QUEUE_NAME = os.getenv("RABBITMQ_TEST_RESULT_QUEUE")
TEST_QUEUE_NAME = os.getenv("RABBITMQ_TEST_QUEUE")

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_TEST_DB_NAME = os.getenv("MONGO_TEST_DB_NAME")
MONGO_TEST_COL_NAME = os.getenv("MONGO_TEST_COL_NAME")


TIMEOUT = 100

mongo_db = pymongo.MongoClient(MONGO_HOST)


class ResultSaverTestcase(unittest.TestCase):
    def wait_for_document_insertion(
        self, collection, query, max_attempts=3, interval_seconds=1
    ):
        for _ in range(max_attempts):
            query_result = [elem for elem in collection.find(query)]
            if len(query_result) != 0:
                return query_result

            time.sleep(interval_seconds)

        return []

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

    def test_mongodb(self):
        collection = mongo_db[MONGO_TEST_DB_NAME][MONGO_TEST_COL_NAME]
        collection.delete_many({})
        collection.insert_many(DUMMY_DATAS)

        for testcase in MONGO_TESTCASES:
            if testcase["command"] == "QUERY":
                res_list = self.wait_for_document_insertion(
                    collection=collection, query=testcase["object"]
                )

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

        INDEX_FIELD = "job_id"

        if f"{INDEX_FIELD}_1" in collection.index_information():
            collection.drop_index(f"{INDEX_FIELD}_1")

        collection.create_index(INDEX_FIELD, unique=True)
        collection.delete_many({})

        for testcase in INSERT_FUNC_TESTCASES:
            insert_ack = mongo_insert(
                db_name=MONGO_TEST_DB_NAME,
                col_name=MONGO_TEST_COL_NAME,
                obj=testcase["object"].copy(),
            )

            self.assertEqual(insert_ack, testcase["expected_ack"])

            query_all_res = self.wait_for_document_insertion(
                collection=collection, query={}
            )

            query_one_res = self.wait_for_document_insertion(
                collection=collection, query=testcase["object"]
            )

            self.assertEqual(len(query_all_res), testcase["len_cum_query"])
            self.assertEqual(len(query_one_res), len(testcase["ans"]))

            for res, ans in zip(query_one_res, testcase["ans"]):
                for field in ans:
                    self.assertEqual(res[field], ans[field])

        collection.delete_many({})
        if f"{INDEX_FIELD}_1" in collection.index_information():
            collection.drop_index(f"{INDEX_FIELD}_1")

    def test_message_receive_and_save(self):
        collection = mongo_db[MONGO_TEST_DB_NAME][MONGO_TEST_COL_NAME]
        collection.delete_many({})

        connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        self.assertEqual(type(connection), BlockingConnection)
        channel = connection.channel()
        self.assertEqual(type(channel), BlockingChannel)
        channel.queue_purge(queue=RESULT_QUEUE_NAME)
        channel.queue_declare(queue=RESULT_QUEUE_NAME, durable=True)

        for testcase in RECEIVE_AND_SAVE_MESSAGES:
            channel.basic_publish(
                exchange="",
                routing_key=RESULT_QUEUE_NAME,
                body=testcase["message"],
                properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
            )

        for testcase in RECEIVE_AND_SAVE_MESSAGES:
            result = self.wait_for_document_insertion(
                collection=collection, query=json.loads(testcase["message"])
            )

            self.assertEqual(len(result), len(testcase["ans"]))
            for res, ans in zip(result, testcase["ans"]):
                for field in ans:
                    self.assertEqual(res[field], ans[field])

        collection.delete_many({})


if __name__ == "__main__":
    unittest.main()
