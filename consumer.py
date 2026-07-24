from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv
from processor import SensorProcessor

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client['sensor_data_db']

processor = SensorProcessor()

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

consumer.subscribe(['environment', 'traffic', 'weather', 'light', 'radiation', 'pollution', 'gps', 'power', 'noise', 'others'])

for message in consumer:
    data = message.value
    topic = message.topic

    avg = processor.add_and_get_average(data)
    print(f"[{topic}] Sensor {data['sensor_id']} average: {avg: .2f}")
    try:
        db[topic].update_one(
            {
                'sensor_id': data['sensor_id'],
                'timestamp': data['timestamp']
            },
            {'$set': data},
            upsert=True
        )
        print(f"Stored data from {topic} for sensor {data['sensor_id']}")
    except Exception as e:
        print(f"MongoDB write failed: {e}")