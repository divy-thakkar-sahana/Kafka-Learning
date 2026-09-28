from fastapi import APIRouter, HTTPException
from app.kafka_poc_1.models import OrderDeliveryRequest, OrderDeliveryResponse
from app.kafka_poc_1.producer import publish_delivery_event
from app.kafka_poc_1.consumer import get_buffered_orders

router = APIRouter()


@router.post("/send", response_model=OrderDeliveryResponse, summary="Send Order Message (Producer)")
def send_order_event_endpoint(request: OrderDeliveryRequest):
    """
    Publish an order delivery message to Kafka.
    """
    try:
        return publish_delivery_event(request)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka order publish failed: {exc}")


@router.get("/orders", summary="View Consumed Orders (Consumer)")
def get_orders_endpoint(limit: int = 50):
    """
    Retrieve messages received and acknowledged by the Kafka consumer.
    """
    orders = get_buffered_orders(limit)
    return {
        "count": len(orders),
        "orders": orders,
    }
