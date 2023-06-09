import base64
from typing import List
import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Security
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import pymongo

load_dotenv()
API_KEYS = os.getenv("API_KEYS").split("<key_sep>")  # pylint: disable=eval-used

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def get_api_key(
    api_key_header: str = Security(api_key_header),
) -> str:
    if api_key_header in API_KEYS:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key",
    )


MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COL_NAME = os.getenv("MONGO_COL_NAME")

mongo_db = pymongo.MongoClient(os.getenv("MONGO_HOST"))


def mongo_query(
    db_name: str = MONGO_DB_NAME,
    col_name: str = MONGO_COL_NAME,
    query: object = {},
) -> List:
    result = mongo_db[db_name][col_name].find(query)

    return [obj for obj in result]


def decode(b64img: str):
    return base64.b64decode(b64img)


app = FastAPI()


class Body(BaseModel):
    job_id: int


@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK, content="Ok")


@app.post("/get")
def get_job_result(body: Body, api_key: str = Security(get_api_key)):
    job_id = body.job_id

    result = [
        elem
        for elem in mongo_query(
            db_name=MONGO_DB_NAME, col_name=MONGO_COL_NAME, query={"job_id": job_id}
        )
    ]
    if len(result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No such result with job_id (The job may being processed or no job with this id was submitted)",
        )

    image_bytes = decode(result[0]["image"])
    response = Response(status_code=status.HTTP_200_OK, content=image_bytes)
    response.headers["Content-Type"] = "image/png"
    response.headers["Content-Disposition"] = "attachment; filename=image.png"
    return response
