from kafka import KafkaConsumer
from pymongo import MongoClient
import os, json
from dotenv import load_dotenv

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client['sensor_data_db']
alert_collection = db["alert_action"]

kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
consumer = KafkaConsumer(
    bootstrap_servers=kafka_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

consumer.subscribe(['alert_logs'])

for alert_log in consumer:
    data = alert_log.value

    try:
        alert_collection.update_one(
            {'uni_key': data['uni_key']},
            {'$set': data},
            upsert=True
        )
        print(f"Stored alert for {data}")
    except Exception as e:
            print(f"MongoDB write alert log failed: {e}")

