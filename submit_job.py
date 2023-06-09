from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Security
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import json
from pika import (
    BasicProperties,
    BlockingConnection, 
    ConnectionParameters, 
    spec)
from pydantic import BaseModel
import os

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

app =FastAPI()

class Body(BaseModel):
    url : str

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
IMAGE_QUEUE_NAME = os.getenv("RABBITMQ_IMAGE_QUEUE")

@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK)

@app.post("/convert")
def get_job_result(body : Body, api_key: str = Security(get_api_key)) :
    
    message = {"url": body.url, "job_id":hash(body.url)}

    mq_connection = BlockingConnection(ConnectionParameters(host=RABBITMQ_HOST,port = RABBITMQ_PORT))
    mq_channel = mq_connection.channel()

    mq_channel.queue_declare(queue=IMAGE_QUEUE_NAME,durable=True)
    mq_channel.basic_publish(exchange="",routing_key=IMAGE_QUEUE_NAME,
                                body=json.dumps(message),
                                properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE))
    mq_connection.close()

    return JSONResponse(status_code=status.HTTP_200_OK,content={
        "message":"Job Submitted"})
