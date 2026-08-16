from kafka import KafkaConsumer, KafkaProducer
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

kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
producer = KafkaProducer(
    bootstrap_servers=kafka_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
consumer = KafkaConsumer(
    bootstrap_servers=kafka_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)
consumer.subscribe(['environment', 'traffic', 'weather', 'light', 'radiation', 
                    'pollution', 'gps', 'power', 'noise', 'others'])

def get_alert_rule(data):
    label = data.get('label')
    alert_rule = db['alert_rules'].find_one(
        {"label": label}
    )
    return alert_rule

for message in consumer:
    data = message.value
    topic = message.topic
    sensor_id = data.get('sensor_id')
    label = data.get('label')
    rule = get_alert_rule(data)

    avg = processor.add_and_get_average(data)
    print(f"[{topic}] Sensor {sensor_id} average: {avg if avg is None else f'{avg: .2f}'}")

    try:
        db[topic].update_one(
            {
                'sensor_id': sensor_id,
                'timestamp': data['timestamp']
            },
            {'$set': data},
            upsert=True
        )
        print(f"Stored data from {topic} - {label} for sensor {data['sensor_id']}")
    except Exception as e:
        print(f"MongoDB write failed: {e}")

    # Send alert to kafka if the average value exceeds the threshold
    if rule and avg is not None and avg > rule['threshold']:
        alert_data = {
            'uni_key': f"{rule['rule_id']}:{sensor_id}:{data['timestamp']}",
            'rule_id': rule['rule_id'],
            'sensor_id': sensor_id,
            'threshold': rule['threshold'],
            'value': avg,
            'severity': rule['severity'],
            'event_timestamp': data['timestamp']
        }
        producer.send("alert_logs", value=alert_data).get(timeout=10)
        print(f"{sensor_id}: {avg}")

