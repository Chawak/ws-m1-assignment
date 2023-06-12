import json
import os
from socket import gaierror
import bcrypt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Security
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pika import BasicProperties, BlockingConnection, ConnectionParameters, spec
from pika.exceptions import AMQPConnectionError
import validators
from pydantic import BaseModel

load_dotenv()
API_KEYS = os.getenv("API_KEYS").split("<key_sep>")

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


app = FastAPI()


class Body(BaseModel):
    url: str


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
IMAGE_QUEUE_NAME = os.getenv("RABBITMQ_IMAGE_QUEUE")


def check_if_valid_url(url: str) -> bool:
    try:
        is_valid = bool(validators.url(url))
    except TypeError:
        return False

    return is_valid


def generate_job_id(url) -> int:
    return hash(url + str(bcrypt.gensalt()))


def rabbitmq_publish(
    message,
    host: str = RABBITMQ_HOST,
    port: str = RABBITMQ_PORT,
    queue: str = IMAGE_QUEUE_NAME,
) -> bool:
    try:
        mq_connection = BlockingConnection(ConnectionParameters(host=host, port=port))
        mq_channel = mq_connection.channel()

        mq_channel.queue_declare(queue=queue, durable=True)
        mq_channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=message,
            properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
        )
        mq_connection.close()

        return True
    except (gaierror, AMQPConnectionError):
        return False


@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK, content="Ok")


@app.post("/convert")
def get_job_result(body: Body, api_key: str = Security(get_api_key)):
    if not body or not body.url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Please specify body and input URL in the field "url" of the body',
        )

    is_valid_url = check_if_valid_url(body.url)

    if not is_valid_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL in the body is not valid URL",
        )

    job_id = generate_job_id(body.url)
    message = {"url": body.url, "job_id": job_id}

    message_sent = rabbitmq_publish(json.dumps(message))

    if message_sent:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"detail": "Job Submitted", "job_id": job_id},
        )

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Error occured with rabbitmq",
    )
