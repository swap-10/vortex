from confluent_kafka import Producer
import random
import time
import json
import argparse

parser = argparse.ArgumentParser(description='Generate and stream dummy events to kafka')
parser.add_argument('--bootstrap-servers', required=False, default="localhost:9092", help='Kafka bootstrap servers (e.g., localhost:9092)')
parser.add_argument('--topic', required=False, default="vortex-events", help='Kafka topic to produce events to')
parser.add_argument('--num-events', type=int, required=False, default=1000, help='Number of events to generate')
parser.add_argument('--num-products', type=int, required=False, default=10, help='Number of unique products')
parser.add_argument('--num_users', type=int, required=False, default=10, help='Number of unique users')

args = parser.parse_args()

# Define kafka configuration
conf = {
    'bootstrap.servers': args.bootstrap_servers,
    'client.id': 'vortex-data-producer'
}


# Create a kafka producer instance
producer = Producer(conf)

# Generate and send dummy events

for _ in range(args.num_events):
    event = {
        'user': f'user_{random.randint(1, args.num_users)}',
        'product': f'product_{random.randint(1, args.num_products)}',
        'action': random.choice(['view', 'add_to_cart', 'purchase']),
        'timestamp': time.time()
    }

    # Serialize the event as a json obj
    event_json = json.dumps(event)

    # send the event to the kafka topic
    producer.produce(args.topic, value=event_json)

    print(f"Produced event: {event_json}")

    time.sleep(0.5)

producer.flush()