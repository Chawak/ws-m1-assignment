import unittest
import os
import base64
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
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COL_NAME = os.getenv("MONGO_COL_NAME")
TIMEOUT = 100

mongo_db = pymongo.MongoClient(os.getenv("MONGO_HOST"))


class ResultCheckerTestcase(unittest.TestCase):
    def test_mongodb(self):
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].delete_many({})
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].insert_many(DUMMY_DATAS)

        for testcase in MONGO_TESTCASES:
            if testcase["command"] == "QUERY":
                res = mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].find(testcase["object"])
                res_list = [data for data in res]

                self.assertEqual(len(res_list), len(testcase["ans"]))
                res_list = sorted(res_list, key=lambda d: d["job_id"])

                for i in range(len(testcase["ans"])):
                    for field in testcase["ans"][i]:
                        self.assertEqual(res_list[i][field], testcase["ans"][i][field])

            elif testcase["command"] == "DELETE":
                mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].delete_one(testcase["object"])
            elif testcase["command"] == "UPDATE":
                mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].update_one(
                    testcase["object"], testcase["new_object"]
                )
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].delete_many({})

    def test_mongo_query(self):
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].delete_many({})
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].insert_many(DUMMY_DATAS)

        for testcase in QUERY_FUNC_TESTCASES:
            res = mongo_query(
                db_name=testcase["db"],
                col_name=testcase["col"],
                query=testcase["query"],
            )
            res_list = [data for data in res]
            self.assertEqual(len(res_list), len(testcase["ans"]))
            res_list = sorted(res_list, key=lambda d: d["job_id"])

            for i in range(len(testcase["ans"])):
                for field in testcase["ans"][i]:
                    self.assertEqual(res_list[i][field], testcase["ans"][i][field])

        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].delete_many({})

    def test_decoding(self):
        for testcase in DECODE_TESTCASES:
            with open(testcase["filename"], "rb") as image:
                original_content = image.read()
            encoded_content = base64.b64encode(original_content)
            decoded_content = decode(encoded_content)
            self.assertEqual(decoded_content, original_content)

    def test_healthcheck_endpoint(self):
        res = requests.get(f"http://{APP_HOST}:{APP_PORT}/healthcheck", timeout=TIMEOUT)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.ok, True)
        self.assertEqual(res.content, b'"Ok"')
        self.assertEqual(res.text, '"Ok"')

    def test_get_endpoint(self):
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].delete_many({})
        mongo_db[MONGO_DB_NAME][MONGO_COL_NAME].insert_many(DUMMY_DATAS)

        for testcase in GET_ENDPOINT_TESTCASES:
            res = requests.post(
                f"http://{APP_HOST}:{APP_PORT}/get",
                json={"job_id": testcase["job_id"]},
                headers={"x-api-key": testcase["api_key"]},
                timeout=TIMEOUT,
            )
            if testcase["expected_headers"]:
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
            self.assertEqual(res.content, testcase["expected_content"])


if __name__ == "__main__":
    unittest.main()
