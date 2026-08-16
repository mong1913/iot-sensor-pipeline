import requests
#from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaProducer
import json, os
import time
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
producer = KafkaProducer(
    bootstrap_servers=kafka_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_topic_by_sensor(title):
    title = title.lower()

    mapping = [
        (['lighning distance', 'lightnings distance'], ('environment', 'environment.lightning_distance')),
        (['lightning counter'], ('environment', 'environment.lightning_count')),
        (['lightning energy'], ('environment', 'environment.lightning_energy')),

        (['overtaking manoeuvre', 'overtaking distance'], ('traffic', 'car.overtaking_distance')),
        (['fahrzeugdichte'], ('traffic', 'car.density')),
        (['fahrzeuge', 'fahrzeugzähler'], ('traffic', 'car.count')),
        (['speed'], ('traffic', 'car.speed')),

        (['wassertemperatur'], ('weather', 'water.water_temperature')),
        (['wassertemperatur 1,5m'], ('weather', 'water.water_temperature_1.5m')),
        (['wassertemperatur 1m'], ('weather', 'water.water_temperature_1m')),
        (['temp', 'Temperatur'], ('weather', 'atm.temperature')),
        (['feucht', 'humid'], ('weather', 'atm.humidity')),
        (['druck', 'pressure'], ('weather', 'atm.air_pressure')),
        (['dew point'], ('weather', 'atm.dew_point')),
        (['windgeschwindigkeit'], ('weather', 'atm.wind_speed')),
        (['windrichtung'], ('weather', 'atm.wind_direction')),
        (['rain'], ('weather', 'atm.rain')),
        (['altitude'], ('weather', 'atm.altitude')),

        (['uv-index'], ('light', 'uv.index')),
        (['uv-a', 'uva'], ('light', 'uv.a')),
        (['uv-b', 'uvb'], ('light', 'uv.b')),
        (['uv-c'], ('light', 'uv.c')),
        (['uv-i', 'uvi'], ('light', 'uv.i')),
        (['beleuchtung', 'light', 'licht', 'lux', 'visible'], ('light', 'visible_light.visible_light')),
        (['uv'], ('light', 'uv.intensity')),
        (['ir', 'infra'], ('light', 'ir.infrared')),
 
        (['radiation', 'radioactivity', 'radioaktivity'], ('radiation', 'radiation.level')),
        (['radiation - count'], ('radiation', 'radiation.count')),
        (['radiation - cpm - count per minute'], ('radiation', 'radiation.frequency_cpm')),
        (['radiation - µsv/h - micro sievert / hour'], ('radiation', 'radiation.frequency_µsvph')),
        (['radiation - µsv/h - error of micro sievert / hour'], ('radiation', 'radiation.frequency_error_µsvph')),
        (['radiation - cpm - count per minute'], ('radiation', 'radiation.frequency_cpm')),

        (['voc', 'volatile organic compounds'], ('pollution', 'gas.volatile_organic_compounds')),
        (['ammonia'], ('pollution', 'gas.ammonia')),
        (['carbon monoxide'], ('pollution', 'gas.carbon_monoxide')),
        (['co2', 'co₂', 'carbon dioxide'], ('pollution', 'gas.carbon_dioxide')),
        (['ethanol'], ('pollution', 'gas.ethanol')),
        (['pm10', 'pm 10'], ('pollution', 'particle.pm10')),
        (['pm2.5', 'pm 2,5', 'PM2.5'], ('pollution', 'particle.pm2.5')),
        (['pm1', 'pm 1'], ('pollution', 'particle.pm1')),
        (['pm10', 'pm10.0'], ('pollution', 'particle.pm10')),
        (['pm4'], ('pollution', 'particle.pm4')),
        (['pm25'], ('pollution', 'particle.pm25')),
        (['formaldehyd'], ('pollution', 'gas.formaldehyd')),
        (['oxygen'], ('pollution', 'gas.oxygen')),
        (['hydrogen'], ('pollution', 'gas.hydrogen')),
        (['methane'], ('pollution', 'gas.methane')),
        (['ethanol'], ('pollution', 'gas.ethanol')),
        (['butane'], ('pollution', 'gas.butane')),
        (['propane'], ('pollution', 'gas.propane')),
        (['nitrogen dioxide'], ('pollution', 'gas.nitrogen_dioxide')),

        (['gps - angle'], ('gps', 'location.angle')),
        (['gps - fix'], ('gps', 'location.fix')),
        (['gps - latitude'], ('gps', 'location.latitude')),
        (['gps - longitude'], ('gps', 'location.longitude')),
        (['gps - quality'], ('gps', 'location.quality')),
        (['gps - satellites'], ('gps', 'location.satellites')),
        (['gps - speed'], ('gps', 'location.speed')),

        (['spannung', 'lipo - v'], ('power', 'power.voltage')),
        (['batteriestand', 'lipo - %'], ('power', 'power.battery_level')),
        (['lipo - rest h'], ('power', 'power,battery_rest_hour')),
        (['lipo - w'], ('power', 'power.powerwatt')),

        (['noise', 'laut', 'loudness'], ('noise', 'environment.noise_level'))
    ]

    for keywords, (topic, label) in mapping:
        if any(key in title for key in keywords):
            return topic, label
    return 'others', 'unknown'

def fetch_data():

    # Munich city sensor data
    bbox = "11.4, 48, 11.7, 48.3"
    url_bbox = f"https://api.opensensemap.org/boxes?minimal=true&bbox={bbox}"

    try:
        response = requests.get(url_bbox)
        response.raise_for_status()
        boxes = response.json()
        box_ids = [box["_id"] for box in boxes]
        print(f"get {len(box_ids)} sensor ids")
        print(box_ids[:10])
    except Exception as e:
        print(e)

    while True:
        logging.info("Start new round data fetching.")
        for box_id in box_ids:
            try:
                url_boxid = f"https://api.opensensemap.org/boxes/{box_id}"
                response = requests.get(url_boxid)
                response.raise_for_status()
                data = response.json()

                for sensor in data.get('sensors', []):
                    lastMeasurement = sensor.get('lastMeasurement', {}) or {}
                    title = sensor.get('title', '')
                    topic, label = get_topic_by_sensor(title)
                    de_timezone = ZoneInfo("Europe/Berlin")
                    data = {
                        "box_id": box_id,
                        "sensor_id": sensor.get('_id'),
                        "topic": topic,
                        "type": title,
                        "label": label,
                        "value": lastMeasurement.get('value'),
                        "unit": sensor.get('unit'),
                        "timestamp": lastMeasurement.get('createdAt'),
                        "processedtime": datetime.now(de_timezone).isoformat()
                    }

                    if data['value'] is not None:
                        producer.send(topic, value=data)
                        print(f"{topic}: {data}")        
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                logging.error(f"Box {box_id} request failed: {e}")
                continue
            except Exception as e:
                logging.error(f"Unknown error: {e}")
                continue
            
        time.sleep(60)

    producer.flush()

if __name__ == "__main__":
    fetch_data()