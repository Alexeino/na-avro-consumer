from pydantic import BaseModel, Field, computed_field
from typing import List
from enum import Enum

class OrderStatus(Enum):
    new = "NEW"
    accepted = "ACCEPTED"
    preparing = "PREPARING"
    out_for_delivery = "OUT_FOR_DELIVERY"
    delivered = "DELIVERED"

class ItemSchema(BaseModel):
    item_name: str = Field(min_length=3)
    quantity: int = Field(gt=0)
    price_per_item: float = Field(gt=0)

class OrderSchema(BaseModel):
    customer_name: str = Field(min_length=3)
    items: List[ItemSchema]
    order_status: OrderStatus = OrderStatus.new
    
    @computed_field
    def order_value(self)-> float:
        items = self.items
        order_value = sum(item.quantity * item.price_per_item for item in items)
        return order_value
    
    @computed_field
    def order_id(self) -> str:
        name_prefix = self.customer_name[:3].upper()
        order_id = f"FOO_{name_prefix}_{int(self.order_value)}"

        return order_id