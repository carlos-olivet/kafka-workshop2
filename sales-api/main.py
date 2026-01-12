from fastapi import FastAPI, HTTPException
from models import PurchaseEvent
from producer import KafkaSalesProducer
import os
import asyncio

app = FastAPI()

# Initialize Producer (Singleton pattern recommended for prod)
producer = KafkaSalesProducer(
    bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
    schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL")
)

@app.post("/sales/")
async def create_sale(event: PurchaseEvent):
    try:
        # Convert Pydantic to Dict and send
        await producer.send_event("raw_sale_events_topic", event.model_dump())
        return {"status": "accepted", "transaction_id": event.transaction_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))