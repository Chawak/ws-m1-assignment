import base64
from dotenv import load_dotenv
from fastapi import FastAPI,status
from fastapi.responses import JSONResponse , Response
from pydantic import BaseModel
import pymongo
import os

load_dotenv()

mongo_db = pymongo.MongoClient(os.getenv("MONGO_HOST"))

app =FastAPI()

class Body(BaseModel):
    job_id : int

@app.get("/healthcheck")
def healthcheck():
    return JSONResponse(status_code=status.HTTP_200_OK)

@app.post("/get")
def get_job_result(body : Body ) :
    job_id = body.job_id

    result = [elem for elem in mongo_db["jobs"]["grayscale-images"].find({"job_id":job_id})]
    if len(result) == 0 :

        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND,content={
            "message":"No such result with job_id (The job may being processed or no job with this id was submitted)"})

    else :
        image_bytes = base64.b64decode(result["image"])
        response = Response(status_code=status.HTTP_200_OK,)
        response.headers["Content-Type"] = "image/png"  
        response.headers["Content-Disposition"] = "attachment; filename=image.png" 
        response.content = image_bytes
        return response