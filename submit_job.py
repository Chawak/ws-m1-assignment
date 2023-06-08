from dotenv import load_dotenv
from fastapi import FastAPI,status
from fastapi.responses import JSONResponse
import json
from pika import (
    BasicProperties,
    BlockingConnection, 
    ConnectionParameters, 
    spec)
from pydantic import BaseModel
import os

load_dotenv()

app =FastAPI()

class Body(BaseModel):
    url : str

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
IMAGE_QUEUE_NAME = os.getenv("RABBITMQ_IMAGE_QUEUE")

@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK)

@app.post("/convert")
def get_job_result(body : Body ) :
    
    message = {"url": body.url, "job_id":hash(body.url)}

    try: 
        mq_connection = BlockingConnection(ConnectionParameters(host=RABBITMQ_HOST,port = RABBITMQ_PORT))
        mq_channel = mq_connection.channel()

        mq_channel.queue_declare(queue=IMAGE_QUEUE_NAME,durable=True)
        mq_channel.basic_publish(exchange="",routing_key=IMAGE_QUEUE_NAME,
                                 body=json.dumps(message),
                                 properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE))
        
        return JSONResponse(status_code=status.HTTP_200_OK,content={
            "message":"Job Submitted"})
    except :
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,content={
            "message":"Internal Servel Error"})
