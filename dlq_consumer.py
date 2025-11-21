import json
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
DLQ_TOPIC = os.getenv('DLQ_TOPIC', 'orders-dlq')
GROUP_ID = os.getenv('DLQ_GROUP_ID', 'dlq-consumer-group')

class DLQMonitor:
    def __init__(self):
        self.message_count = 0
    
    def process_message(self, data):
        self.message_count += 1
        timestamp = datetime.fromtimestamp(data.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n{'='*60}")
        print(f"DLQ Message #{self.message_count}")
        print(f"{'='*60}")
        print(f"Timestamp:  {timestamp}")
        print(f"Order ID:   {data.get('orderId', 'N/A')}")
        print(f"Product:    {data.get('product', 'N/A')}")
        print(f"Price:      ${data.get('price', 0):.2f}")
        print(f"Error:      {data.get('error', 'N/A')}")
        print(f"Retries:    {data.get('retries', 0)}")
        print(f"{'='*60}")
    
    def get_count(self):
        return self.message_count

def main():
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([DLQ_TOPIC])
    
    monitor = DLQMonitor()
    
    print(f"DLQ Consumer started | Broker: {KAFKA_BROKER} | Topic: {DLQ_TOPIC}")
    print("Monitoring for failed messages...\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                monitor.process_message(data)
            except json.JSONDecodeError as e:
                print(f"Failed to parse message: {e}")
                
    except KeyboardInterrupt:
        print("\n\nShutting down DLQ consumer...")
    finally:
        print(f"\nTotal DLQ messages received: {monitor.get_count()}")
        consumer.close()
        print("DLQ Consumer stopped")

if __name__ == '__main__':
    main()