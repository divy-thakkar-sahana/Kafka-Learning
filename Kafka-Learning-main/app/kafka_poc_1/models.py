from pydantic import BaseModel, Field


class OrderDeliveryRequest(BaseModel):
    order_id: str = Field(..., example="ORD-7890", description="Unique Order ID")
    customer_id: str = Field(..., example="CUST-102", description="Customer ID")
    delivery_partner_id: str = Field(..., example="DRIVER-45", description="Assigned Delivery Partner/Driver ID")
    item_name: str = Field(..., example="Paneer Butter Masala + Butter Naan", description="Ordered food/item description")
    use_key: bool = Field(True, description="If True, customer_id is passed as Kafka partition key for hashing")
    explicit_partition: int | None = Field(
        None,
        ge=0,
        le=2,
        example=2,
        description="Optional: Explicit partition ID (0, 1, or 2) chosen directly by Producer. Overrides key hashing if provided.",
    )


class OrderDeliveryResponse(BaseModel):
    message_id: str
    order_id: str
    customer_id: str
    delivery_partner_id: str
    item_name: str
    routing_mode: str  # "explicit_partition", "customer_key_hash", or "unkeyed_auto"
    partition_key: str | None
    topic: str
    partition: int
    offset: int
    timestamp: str
