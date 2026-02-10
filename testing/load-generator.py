import requests
import random
import uuid
import time
import argparse
from datetime import datetime

API_URL = "http://localhost:8000/sales/"

def generate_payload(malformed=False):
    if malformed:
        return {"user_id": 123} # Missing fields -> 422 Error

    return {
        "user_id": f"user_{random.randint(1, 100)}",
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "store_id": f"store_LDN_{random.randint(1, 5)}",
        "total_amount": round(random.uniform(10.0, 500.0), 2),
        "items": [
            {
                "item_id": f"item_{random.randint(1, 50)}",
                "name": "Widget",
                "price": 10.0,
                "quantity": random.randint(1, 5)
            }
        ]
    }

def run_load(count, defective_count):
    print(f"--- Starting Load: {count} valid, {defective_count} defective ---")
    
    for i in range(count):
        try:
            resp = requests.post(API_URL, json=generate_payload())
            print(f"Valid [{i+1}/{count}]: {resp.status_code}")
        except Exception as e:
            print(f"Error: {e}")
            
    for i in range(defective_count):
        try:
            resp = requests.post(API_URL, json=generate_payload(malformed=True))
            print(f"Defective [{i+1}/{defective_count}]: {resp.status_code}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1, help="Number of valid events")
    parser.add_argument("--defective", type=int, default=0, help="Number of bad events")
    args = parser.parse_args()

    run_load(args.count, args.defective)