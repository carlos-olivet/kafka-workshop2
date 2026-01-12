from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import sys

# Configuration
conf = {
    'bootstrap.servers': 'localhost:9092', # Connects from your host
    'group.id': 'python_dashboard_group',
    'auto.offset.reset': 'earliest'
}

TOPIC = 'store_revenue_output'

def start_consumer():
    consumer = Consumer(conf)
    
    try:
        consumer.subscribe([TOPIC])
        print(f"🎧 Listening to Flink output on topic: {TOPIC}...")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Parse JSON message from Flink
            try:
                # Flink JSON format is usually pure bytes
                value = json.loads(msg.value().decode('utf-8'))
                
                print("\n💰 --- REAL-TIME REVENUE ALERT ---")
                print(f"Store ID:   {value.get('store_id')}")
                print(f"Time Window: {value.get('window_end')}")
                print(f"Revenue:     ${value.get('total_revenue')}")
                print("-----------------------------------")
            except Exception as e:
                print(f"Error decoding: {e}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()