import unittest
import os
import base64
import time
from main import decode, mongo_query
from testcase import (
    QUERY_FUNC_TESTCASES,
    DUMMY_DATAS,
    MONGO_TESTCASES,
    DECODE_TESTCASES,
    GET_ENDPOINT_TESTCASES,
)
import requests
from dotenv import load_dotenv
import pymongo

load_dotenv()
APP_HOST = os.getenv("APP_HOST")
APP_PORT = os.getenv("APP_PORT")
MONGO_TEST_DB_NAME = os.getenv("MONGO_TEST_DB_NAME")
MONGO_TEST_COL_NAME = os.getenv("MONGO_TEST_COL_NAME")
TIMEOUT = 100

mongo_db = pymongo.MongoClient(os.getenv("MONGO_HOST"))
collection = mongo_db[MONGO_TEST_DB_NAME][MONGO_TEST_COL_NAME]


class ResultCheckerTestcase(unittest.TestCase):
    def wait_for_document_insertion(
        self, collection, query, max_attempts=3, interval_seconds=1
    ):
        for _ in range(max_attempts):
            query_result = [elem for elem in collection.find(query)]
            if len(query_result) != 0:
                return query_result

            time.sleep(interval_seconds)

        return []

    def test_mongodb(self):
        collection.delete_many({})
        collection.insert_many(DUMMY_DATAS)

        for testcase in MONGO_TESTCASES:
            if testcase["command"] == "QUERY":
                res = self.wait_for_document_insertion(
                    collection, query=testcase["object"]
                )
                self.assertEqual(len(res), len(testcase["ans"]))
                res = sorted(res, key=lambda d: d["job_id"])

                for i in range(len(testcase["ans"])):
                    for field in testcase["ans"][i]:
                        self.assertEqual(res[i][field], testcase["ans"][i][field])

            elif testcase["command"] == "DELETE":
                collection.delete_one(testcase["object"])
            elif testcase["command"] == "UPDATE":
                collection.update_one(testcase["object"], testcase["new_object"])
        collection.delete_many({})

    def test_decoding(self):
        for testcase in DECODE_TESTCASES:
            res = requests.get(testcase["url"], timeout=TIMEOUT)
            encoded_content = base64.b64encode(res.content)
            decoded_content = decode(encoded_content)
            self.assertEqual(decoded_content, res.content)

    def test_healthcheck_endpoint(self):
        res = requests.get(f"http://{APP_HOST}:{APP_PORT}/healthcheck", timeout=TIMEOUT)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.ok, True)
        self.assertEqual(res.content, b'"Ok"')
        self.assertEqual(res.text, '"Ok"')

    def test_get_endpoint(self):
        collection.delete_many({})
        collection.insert_many(DUMMY_DATAS)

        for testcase in GET_ENDPOINT_TESTCASES:
            res = requests.post(
                f"http://{APP_HOST}:{APP_PORT}/get",
                json=testcase["body"],
                headers={"x-api-key": testcase["api_key"]},
                timeout=TIMEOUT,
            )

            if "expected_headers" in testcase:
                self.assertEqual(
                    res.headers["Content-Type"],
                    testcase["expected_headers"]["Content-Type"],
                )
                self.assertEqual(
                    res.headers["Content-Disposition"],
                    testcase["expected_headers"]["Content-Disposition"],
                )

            self.assertEqual(res.status_code, testcase["expected_status"])
            self.assertEqual(res.ok, testcase["expected_ok"])

            if "expected_content" in testcase:
                self.assertEqual(res.content, testcase["expected_content"])
        collection.delete_many({})


if __name__ == "__main__":
    unittest.main()
