# Project Setup

1. Create and activate virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```
3. Install dependencies
```bash
pip install -r requirements.txt
```
## Start Kafka

Make sure Docker is running, then start Kafka services:
```bash
docker-compose up -d
```
## Run the System (Use 3 Terminals)
Terminal 1 – Start Consumer
```bash
python3 consumer.py
```

Terminal 2 – Start DLQ Consumer
```bash
python3 dlq_consumer.py
```

Terminal 3 – Start Producer
```bash
python3 producer.py
```
