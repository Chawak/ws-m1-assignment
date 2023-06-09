import json
import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Security
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pika import BasicProperties, BlockingConnection, ConnectionParameters, spec
from pydantic import BaseModel

load_dotenv()
API_KEYS = eval(os.getenv("API_KEYS"))

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


def rabbitmq_publish(
    message,
    host: str = RABBITMQ_HOST,
    port: str = RABBITMQ_PORT,
    queue: str = IMAGE_QUEUE_NAME,
):
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

    except:
        return False


@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK, content="Ok")


@app.post("/convert")
def get_job_result(body: Body, api_key: str = Security(get_api_key)):
    message = {"url": body.url, "job_id": hash(body.url)}

    message_sent = rabbitmq_publish(json.dumps(message))

    if message_sent:
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"message": "Job Submitted"}
        )

    return JSONResponse(
        status_code=status.HTTP_500_OK,
        content={"message": "Error occured with rabbitmq"},
    )
