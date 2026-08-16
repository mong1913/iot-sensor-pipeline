import json, os, re
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client['sensor_data_db']
rules_collection = db["alert_rules"]
rules_collection.create_index("rule_id", unique=True)

with open("src/alert_rules.json") as f:
    raw = f.read()

filled = re.sub(r"\$\{(\w+)\}", lambda m: os.getenv(m.group(1), ""), raw)
rules = json.loads(filled)["rules"]

for rule in rules:
    rules_collection.update_one({"rule_id": rule["rule_id"]}, {"$set": rule}, upsert=True)
    print(f"upserted {rule['rule_id']}")