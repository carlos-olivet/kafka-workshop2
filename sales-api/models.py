from pydantic import BaseModel, Field, conlist
from typing import List, Optional

class Item(BaseModel):
    item_id: str
    name: str
    price: float
    quantity: int

class PurchaseEvent(BaseModel):
    user_id: str
    transaction_id: str
    timestamp: str
    store_id: str
    items: conlist(Item, min_length=1)
    total_amount: float