Project Setup
1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

2. Install dependencies
pip install -r requirements.txt

Start Kafka

Make sure Docker is running, then start Kafka services:

docker-compose up -d

Run the System (Use 3 Terminals)
Terminal 1 – Start Consumer
python3 consumer.py

Terminal 2 – Start DLQ Consumer
python3 dlq_consumer.py

Terminal 3 – Start Producer
python3 producer.py