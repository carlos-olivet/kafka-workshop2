from pydantic import BaseModel, Field, conlist
from typing import List, Optional

class Item(BaseModel):
    item_id: str
    name: str
    price: float
    quantity: int

class PurchaseEvent(BaseModel):
    user_id: str
    # # Uncomment on part 2
    # # New field added as backwards compatible
    # promotion_code: Optional[str] = None
    transaction_id: str
    timestamp: str
    store_id: str
    items: conlist(Item, min_length=1)
    total_amount: float