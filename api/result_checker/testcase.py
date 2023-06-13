import base64
import os
from dotenv import load_dotenv
import requests

load_dotenv()
MONGO_TEST_DB_NAME = os.getenv("MONGO_TEST_DB_NAME")
MONGO_TEST_COL_NAME = os.getenv("MONGO_TEST_COL_NAME")

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
    DUMMY_DATAS[i]["image"] = base64.b64encode(img.content)


NOT_EXIST = "NotExistName"
UPDATE = "UPDATE"
QUERY = "QUERY"
DELETE = "DELETE"

DECODE_TESTCASES = [
    {
        "url": "https://onlinejpgtools.com/images/examples-onlinejpgtools/random-grid.jpg"
    },
    {
        "url": "https://onlineimagetools.com/images/examples-onlineimagetools/color-grid.png"
    },
    {
        "url": "https://onlineimagetools.com/images/examples-onlineimagetools/orange-shades.png"
    },
]

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

QUERY_FUNC_TESTCASES = [
    {
        "query": {},
        "db": MONGO_TEST_DB_NAME,
        "col": MONGO_TEST_COL_NAME,
        "ans": DUMMY_DATAS,
    },
    {
        "query": {"job_id": 0},
        "db": MONGO_TEST_DB_NAME,
        "col": MONGO_TEST_COL_NAME,
        "ans": [DUMMY_DATAS[0]],
    },
    {
        "query": {"job_id": 1, "image": DUMMY_DATAS[1]["image"]},
        "db": MONGO_TEST_DB_NAME,
        "col": MONGO_TEST_COL_NAME,
        "ans": [DUMMY_DATAS[1]],
    },
    {
        "query": {"job_id": 1, "image": "thisIsImg0"},
        "db": MONGO_TEST_DB_NAME,
        "col": MONGO_TEST_COL_NAME,
        "ans": [],
    },
    {
        "query": {"job_id": 4},
        "db": MONGO_TEST_DB_NAME,
        "col": MONGO_TEST_COL_NAME,
        "ans": [],
    },
    {"query": {}, "db": NOT_EXIST, "col": NOT_EXIST, "ans": []},
    {"query": {}, "db": NOT_EXIST, "col": MONGO_TEST_COL_NAME, "ans": []},
    {"query": {}, "db": MONGO_TEST_DB_NAME, "col": NOT_EXIST, "ans": []},
]

GET_ENDPOINT_TESTCASES = [
    {
        "api_key": None,
        "body": {"job_id": 0},
        "expected_status": 401,
        "expected_ok": False,
        "expected_content": b'{"detail":"Invalid or missing API Key"}',
    },
    {
        "api_key": "wrongKey",
        "body": {"job_id": 0},
        "expected_status": 401,
        "expected_ok": False,
        "expected_content": b'{"detail":"Invalid or missing API Key"}',
    },
    {
        "api_key": "mond",
        "body": {"job_id": 0},
        "expected_status": 200,
        "expected_ok": True,
        "expected_content": base64.b64decode(DUMMY_DATAS[0]["image"]),
        "expected_headers": {
            "Content-Type": "image/png",
            "Content-Disposition": "attachment; filename=image.png",
        },
    },
    {
        "api_key": "1234",
        "body": {"job_id": 1},
        "expected_status": 200,
        "expected_ok": True,
        "expected_content": base64.b64decode(DUMMY_DATAS[1]["image"]),
        "expected_headers": {
            "Content-Type": "image/png",
            "Content-Disposition": "attachment; filename=image.png",
        },
    },
    {
        "api_key": "mond",
        "body": {"job_id": 18},
        "expected_status": 404,
        "expected_ok": False,
        "expected_content": b'{"detail":"No such result with job_id (The job may being processed or no job with this id was submitted)"}',
    },
    {
        "api_key": "mond",
        "body": {"unk_field": 18},
        "expected_status": 422,
        "expected_ok": False,
    },
    {
        "api_key": "mond",
        "body": None,
        "expected_status": 422,
        "expected_ok": False,
    },
]
