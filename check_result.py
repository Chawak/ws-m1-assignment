import base64
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Security
from fastapi.responses import JSONResponse , Response
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import pymongo
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

mongo_db = pymongo.MongoClient(os.getenv("MONGO_HOST"))

app =FastAPI()

class Body(BaseModel):
    job_id : int

@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK,content="Ok")

@app.post("/get")
def get_job_result(body : Body, api_key: str = Security(get_api_key) ) :
    job_id = body.job_id

    result = [elem for elem in mongo_db["jobs"]["grayscale-images"].find({"job_id":job_id})]
    if len(result) == 0 :

        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND,content={
            "message":"No such result with job_id (The job may being processed or no job with this id was submitted)"})

    else :
        image_bytes = base64.b64decode(result["image"])
        response = Response(status_code=status.HTTP_200_OK,content=image_bytes)
        response.headers["Content-Type"] = "image/png"  
        response.headers["Content-Disposition"] = "attachment; filename=image.png" 
        return response
