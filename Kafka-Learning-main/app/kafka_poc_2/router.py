from fastapi import APIRouter, HTTPException
from app.kafka_poc_2.models import ValidOrderRequest, PoisonPillSimulateRequest, OrderResponse
from app.kafka_poc_2.producer import publish_valid_order, publish_poison_pill
from app.kafka_poc_2.consumer import get_processed_orders, get_dlq_orders

router = APIRouter()


@router.post("/send-valid-order", response_model=OrderResponse, summary="Send Valid Order (Normal Processing)")
def send_valid_order_endpoint(request: ValidOrderRequest):
    """Publish a normal valid order to topic 'poc-2-orders'."""
    try:
        return publish_valid_order(request)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka publish failed: {exc}")


@router.post("/send-poison-pill", response_model=OrderResponse, summary="Send Poison Pill Message (Simulate Corrupt Data)")
def send_poison_pill_endpoint(request: PoisonPillSimulateRequest):
    """
    Publish a Poison Pill payload to 'poc-2-orders'.
    Types:
    - `corrupt_json`: Malformed raw string
    - `negative_amount`: Invalid negative payment amount ($ -50.00)
    - `missing_order_id`: Payload lacking mandatory 'order_id'
    """
    try:
        return publish_poison_pill(request)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka publish failed: {exc}")


@router.get("/processed-orders", summary="View Successfully Processed Orders")
def get_processed_orders_endpoint(limit: int = 50):
    """View orders that passed validation and were successfully committed."""
    orders = get_processed_orders(limit)
    return {
        "status": "success",
        "count": len(orders),
        "orders": orders,
    }


@router.get("/dlq-orders", summary="View Poison Pills Diverted to DLQ")
def get_dlq_orders_endpoint(limit: int = 50):
    """
    View Poison Pill messages caught by the consumer and redirected to Dead Letter Queue ('poc-2-dlq').
    Shows error reasons and original partition/offset details.
    """
    dlq_records = get_dlq_orders(limit)
    return {
        "status": "dead_letter_queue",
        "count": len(dlq_records),
        "dlq_topic": "poc-2-dlq",
        "records": dlq_records,
    }
