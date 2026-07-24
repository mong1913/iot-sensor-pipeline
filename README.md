<h1 align="center"> IoT Sensor Data Pipeline </h1>

** Note: This project is currently in progress.**

A near real-time data streaming pipeline that ingests city sensor data, processes it, stores it, and serves it via a RESTful API.

## Tech Stack
* **Data Processing:** Python
* **Message Broker:** Apache Kafka
* **Database:** MongoDB
* **Infrastructure:** Docker Compose
* **Backend API:** FastAPI

## Key Features
* **Data Ingestion:** Fetches city data (weather, pollution, traffic, etc.) from the OpenSenseMap API and publishes it to Kafka topics.
* **Stream Processing:** Consumes Kafka messages and calculates a rolling average.
* **Storage:** Stores the processed data into a local Dockerized MongoDB instance.
* **API Serving:** Exposes a FastAPI endpoint to query specific sensor data dynamically.