import time
import random
import json
import os
from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import Producer, KafkaError

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
TOPIC = os.getenv('KAFKA_TOPIC', 'orders')
DLQ_TOPIC = os.getenv('DLQ_TOPIC', 'orders-dlq')
GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'order-consumer-group')

MAX_RETRIES = 3
RETRY_DELAY = 2
FAILURE_RATE = 0.2

class OrderProcessor:
    def __init__(self, dlq_producer):
        self.total_price = 0.0
        self.order_count = 0
        self.failed_count = 0
        self.dlq_producer = dlq_producer
    
    def calculate_running_average(self, price):
        self.order_count += 1
        self.total_price += price
        return self.total_price / self.order_count
    
    def process_order(self, order, retry_count=0):
        try:
            if random.random() < FAILURE_RATE:
                raise Exception("Processing error")
            
            avg_price = self.calculate_running_average(order['price'])
            
            print(f"✓ Processed: Order {order['orderId']} | {order['product']} | "
                  f"${order['price']:.2f} | Avg: ${avg_price:.2f} | Total: {self.order_count}")
            
            return True
            
        except Exception as e:
            if retry_count < MAX_RETRIES:
                print(f"✗ Retry {retry_count + 1}/{MAX_RETRIES}: Order {order['orderId']}")
                time.sleep(RETRY_DELAY)
                return self.process_order(order, retry_count + 1)
            else:
                print(f"✗ Failed permanently: Order {order['orderId']} → DLQ")
                self.send_to_dlq(order, str(e))
                return False
    
    def send_to_dlq(self, order, error):
        dlq_message = {
            'orderId': order['orderId'],
            'product': order['product'],
            'price': order['price'],
            'error': error,
            'timestamp': time.time(),
            'retries': MAX_RETRIES
        }
        
        self.dlq_producer.produce(
            topic=DLQ_TOPIC,
            key=order['orderId'].encode('utf-8'),
            value=json.dumps(dlq_message).encode('utf-8')
        )
        self.dlq_producer.flush()
        self.failed_count += 1
    
    def get_stats(self):
        return {
            'processed': self.order_count,
            'average_price': self.total_price / self.order_count if self.order_count > 0 else 0,
            'failed': self.failed_count
        }

def main():
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe([TOPIC])
    
    processor = OrderProcessor(dlq_producer)
    
    print(f"Consumer started | Broker: {KAFKA_BROKER} | Topic: {TOPIC} | Group: {GROUP_ID}")
    print(f"DLQ: {DLQ_TOPIC} | Max Retries: {MAX_RETRIES} | Retry Delay: {RETRY_DELAY}s")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {msg.error()}")
                continue
            
            order = msg.value()
            if order:
                processor.process_order(order)
                
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        stats = processor.get_stats()
        print(f"\nStatistics:")
        print(f"  Processed: {stats['processed']}")
        print(f"  Average Price: ${stats['average_price']:.2f}")
        print(f"  Failed (DLQ): {stats['failed']}")
        consumer.close()
        print("Consumer stopped")

if __name__ == '__main__':
    main()