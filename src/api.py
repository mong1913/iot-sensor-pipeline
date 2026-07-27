from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os
from dotenv import load_dotenv

app = FastAPI(title="IoT_Sensor_API")

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client['sensor_data_db']

@app.get("/api/v1/topics/{topic}/sensors/{sensor_id}")
def get_sensor_data(topic: str, sensor_id: str):
    sensor_data = db[topic].find_one(
        {"sensor_id": sensor_id},
        {"_id": 0}
    )

    if not sensor_data:
        raise HTTPException(status_code=404, detail="Sensor not found")

    return sensor_data