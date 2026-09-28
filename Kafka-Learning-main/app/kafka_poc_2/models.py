from pydantic import BaseModel, Field
from typing import Literal


class ValidOrderRequest(BaseModel):
    order_id: str = Field(..., example="ORD-7001", description="Unique Order ID")
    customer_id: str = Field(..., example="CUST-303", description="Customer ID")
    amount: float = Field(..., example=25.50, gt=0, description="Order amount (must be positive > 0)")
    item_name: str = Field(..., example="Burger + Fries Combo", description="Item description")


class PoisonPillSimulateRequest(BaseModel):
    pill_type: Literal["corrupt_json", "negative_amount", "missing_order_id"] = Field(
        ...,
        example="negative_amount",
        description="Poison pill type to simulate: 'corrupt_json' (malformed string), 'negative_amount' (amount <= 0), or 'missing_order_id' (null ID)",
    )
    custom_payload: str | None = Field(
        None,
        example="RAW_POISON_PILL_STRING_BAD_JSON{",
        description="Optional custom raw bad string to publish when pill_type='corrupt_json'",
    )


class OrderResponse(BaseModel):
    message_id: str
    status: str
    topic: str
    partition: int
    offset: int
    timestamp: str
