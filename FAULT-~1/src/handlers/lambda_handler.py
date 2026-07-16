"""
AWS Lambda handler for event processing.

Triggered by:
- S3 event notifications (object created in events/ prefix)
- API Gateway HTTP requests
- CloudWatch scheduled rules
"""

import json
import time
import logging
import os

from src.models.event import Event, EventType
from src.handlers.event_processor import EventProcessor
from src.utils.dynamo_client import DynamoDBClient
from src.utils.s3_client import S3Client
from src.utils.cloudwatch_client import CloudWatchClient

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Initialize clients outside handler for Lambda container reuse
dynamo_client = DynamoDBClient()
s3_client = S3Client()
cloudwatch_client = CloudWatchClient()
processor = EventProcessor(dynamo_client, s3_client, cloudwatch_client)


def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda entry point.
    Routes to appropriate handler based on event source.
    """
    logger.info(f"Received event: {json.dumps(event)[:500]}")

    try:
        # Determine event source and route
        if _is_s3_event(event):
            return _handle_s3_event(event, context)
        elif _is_api_gateway_event(event):
            return _handle_api_gateway_event(event, context)
        elif _is_scheduled_event(event):
            return _handle_scheduled_event(event, context)
        else:
            return _handle_direct_invocation(event, context)

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {e}", exc_info=True)
        return _error_response(500, str(e))


def _is_s3_event(event: dict) -> bool:
    """Check if the event is from S3."""
    records = event.get("Records", [])
    return bool(records and records[0].get("eventSource") == "aws:s3")


def _is_api_gateway_event(event: dict) -> bool:
    """Check if the event is from API Gateway."""
    return "httpMethod" in event or "requestContext" in event


def _is_scheduled_event(event: dict) -> bool:
    """Check if the event is from CloudWatch Events/EventBridge."""
    return event.get("source") == "aws.events"


def _handle_s3_event(event: dict, context) -> dict:
    """Handle S3 object creation events."""
    results = []
    s3_records = s3_client.parse_s3_event(event)

    for record in s3_records:
        processing_event = Event(
            event_type=EventType.S3_OBJECT_CREATED.value,
            source="s3",
            s3_bucket=record["bucket"],
            s3_key=record["key"],
            payload=record,
        )
        result = processor.process_event(processing_event)
        results.append(result)

    return {
        "statusCode": 200,
        "body": json.dumps({"results": results, "count": len(results)}),
    }


def _handle_api_gateway_event(event: dict, context) -> dict:
    """Handle API Gateway requests."""
    http_method = event.get("httpMethod", "POST")
    path = event.get("path", "/")

    # Route API requests
    if path == "/events" and http_method == "POST":
        return _create_event_from_api(event)
    elif path == "/events" and http_method == "GET":
        return _get_events_status()
    elif path == "/health":
        return _health_check()
    elif path == "/metrics":
        return _get_metrics()
    elif path == "/reprocess" and http_method == "POST":
        return _reprocess_failed()
    else:
        return _error_response(404, f"Not found: {http_method} {path}")


def _create_event_from_api(event: dict) -> dict:
    """Create and process an event from an API request."""
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return _error_response(400, "Invalid JSON body")

    processing_event = Event(
        event_type=body.get("event_type", EventType.API_REQUEST.value),
        source="api-gateway",
        payload=body.get("payload", {}),
        idempotency_key=body.get("idempotency_key"),
    )

    result = processor.process_event(processing_event)

    status_code = 200 if result["status"] in ("completed", "deduplicated") else 500
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),
    }


def _get_events_status() -> dict:
    """Get event processing status summary."""
    counts = dynamo_client.get_event_count_by_status()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"event_counts": counts}),
    }


def _handle_scheduled_event(event: dict, context) -> dict:
    """Handle CloudWatch scheduled events for reprocessing failures."""
    result = processor.reprocess_failed_events()
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }


def _handle_direct_invocation(event: dict, context) -> dict:
    """Handle direct Lambda invocations."""
    processing_event = Event(
        event_type=event.get("event_type", EventType.CUSTOM.value),
        source="direct-invocation",
        payload=event.get("payload", event),
    )
    result = processor.process_event(processing_event)
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }


def _health_check() -> dict:
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "healthy", "timestamp": time.time()}),
    }


def _get_metrics() -> dict:
    metrics = cloudwatch_client.get_metrics_summary()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(metrics),
    }


def _reprocess_failed() -> dict:
    result = processor.reprocess_failed_events()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),
    }


def _error_response(status_code: int, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}),
    }
