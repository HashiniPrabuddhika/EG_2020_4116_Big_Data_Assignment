import time
import random
import os
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
TOPIC = os.getenv('KAFKA_TOPIC', 'orders')
SCHEMA_FILE = 'schemas/order.avsc'

PRODUCTS = [f"Item{i}" for i in range(1, 11)]
MIN_PRICE = 10.0
MAX_PRICE = 1000.0
PRODUCE_INTERVAL = 1

def load_schema(schema_file):
    key_schema = avro.loads('{"type": "string"}')
    with open(schema_file, 'rb') as f:
        value_schema = avro.loads(f.read())
    return key_schema, value_schema

def generate_order(order_id):
    return {
        'orderId': str(order_id),
        'product': random.choice(PRODUCTS),
        'price': round(random.uniform(MIN_PRICE, MAX_PRICE), 2)
    }

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Order {msg.key().decode('utf-8')} delivered to {msg.topic()} [partition {msg.partition()}]")

def main():
    key_schema, value_schema = load_schema(SCHEMA_FILE)
    
    producer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'on_delivery': delivery_report
    }
    
    producer = AvroProducer(producer_config, 
                           default_key_schema=key_schema, 
                           default_value_schema=value_schema)
    
    print(f"Producer started | Broker: {KAFKA_BROKER} | Topic: {TOPIC}")
    
    order_id = 1001
    
    try:
        while True:
            order = generate_order(order_id)
            producer.produce(topic=TOPIC, key=str(order_id), value=order)
            print(f"Produced: Order {order['orderId']} | {order['product']} | ${order['price']:.2f}")
            producer.poll(0)
            order_id += 1
            time.sleep(PRODUCE_INTERVAL)
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        print("Producer stopped")

if __name__ == '__main__':
    main()