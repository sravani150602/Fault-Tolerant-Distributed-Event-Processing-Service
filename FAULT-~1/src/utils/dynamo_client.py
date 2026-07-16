"""DynamoDB client for event state tracking with optimized partition key schema."""

import os
import time
import logging
from typing import Optional, Dict, Any, List
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from src.models.event import Event, EventStatus

logger = logging.getLogger(__name__)


class DynamoDBClient:
    """
    DynamoDB-backed event state tracking with optimized partition key schema.

    Partition key: event_type#source (distributes load across partitions)
    Sort key: event_id (unique identifier for each event)
    GSI: idempotency-index on idempotency_key for deduplication lookups
    GSI: status-index on status#updated_at for status-based queries
    """

    def __init__(self, table_name: Optional[str] = None, region: Optional[str] = None):
        self.table_name = table_name or os.environ.get("EVENTS_TABLE", "event-processing-table")
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region)
        self.table = self.dynamodb.Table(self.table_name)
        self.client = boto3.client("dynamodb", region_name=self.region)

    def create_table_if_not_exists(self):
        """Create the DynamoDB table with optimized schema if it doesn't exist."""
        try:
            self.client.describe_table(TableName=self.table_name)
            logger.info(f"Table {self.table_name} already exists")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise

        logger.info(f"Creating table {self.table_name}")
        self.client.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "idempotency_key", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "updated_at", "AttributeType": "N"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "idempotency-index",
                    "KeySchema": [
                        {"AttributeName": "idempotency_key", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 10,
                        "WriteCapacityUnits": 10,
                    },
                },
                {
                    "IndexName": "status-index",
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "updated_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 10,
                        "WriteCapacityUnits": 10,
                    },
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 25,
                "WriteCapacityUnits": 25,
            },
        )

        waiter = self.client.get_waiter("table_exists")
        waiter.wait(TableName=self.table_name)
        logger.info(f"Table {self.table_name} created successfully")

    def _build_partition_key(self, event: Event) -> str:
        """Build optimized partition key: event_type#source."""
        return f"{event.event_type}#{event.source}"

    def _convert_floats(self, obj: Any) -> Any:
        """Convert floats to Decimal for DynamoDB compatibility."""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self._convert_floats(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_floats(i) for i in obj]
        return obj

    def _convert_decimals(self, obj: Any) -> Any:
        """Convert Decimals back to floats for application use."""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals(i) for i in obj]
        return obj

    def put_event(self, event: Event) -> bool:
        """
        Store an event in DynamoDB with conditional write for idempotency.
        Returns True if stored, False if duplicate detected.
        """
        item = self._convert_floats(event.to_dict())
        item["pk"] = self._build_partition_key(event)
        item["sk"] = event.event_id

        try:
            self.table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
            )
            logger.info(f"Stored event {event.event_id}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(f"Duplicate event detected: {event.event_id}")
                return False
            raise

    def check_idempotency(self, idempotency_key: str) -> Optional[Event]:
        """Check if an event with the given idempotency key already exists."""
        try:
            response = self.table.query(
                IndexName="idempotency-index",
                KeyConditionExpression="idempotency_key = :key",
                ExpressionAttributeValues={":key": idempotency_key},
                Limit=1,
            )
            items = response.get("Items", [])
            if items:
                item = self._convert_decimals(items[0])
                return Event.from_dict(item)
            return None
        except ClientError as e:
            logger.error(f"Error checking idempotency: {e}")
            raise

    def update_event_status(self, event: Event):
        """Update the status of an existing event."""
        self.table.update_item(
            Key={
                "pk": self._build_partition_key(event),
                "sk": event.event_id,
            },
            UpdateExpression=(
                "SET #status = :status, updated_at = :updated_at, "
                "retry_count = :retry_count, error_message = :error_message, "
                "processed_at = :processed_at, processing_duration_ms = :duration"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": event.status,
                ":updated_at": Decimal(str(event.updated_at)),
                ":retry_count": event.retry_count,
                ":error_message": event.error_message,
                ":processed_at": Decimal(str(event.processed_at)) if event.processed_at else None,
                ":duration": Decimal(str(event.processing_duration_ms)) if event.processing_duration_ms else None,
            },
        )

    def get_event(self, event_type: str, source: str, event_id: str) -> Optional[Event]:
        """Retrieve a specific event."""
        pk = f"{event_type}#{source}"
        try:
            response = self.table.get_item(Key={"pk": pk, "sk": event_id})
            item = response.get("Item")
            if item:
                return Event.from_dict(self._convert_decimals(item))
            return None
        except ClientError as e:
            logger.error(f"Error getting event: {e}")
            raise

    def get_events_by_status(self, status: str, limit: int = 100) -> List[Event]:
        """Get events filtered by status using the GSI."""
        try:
            response = self.table.query(
                IndexName="status-index",
                KeyConditionExpression="#status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": status},
                Limit=limit,
                ScanIndexForward=False,
            )
            return [
                Event.from_dict(self._convert_decimals(item))
                for item in response.get("Items", [])
            ]
        except ClientError as e:
            logger.error(f"Error querying events by status: {e}")
            raise

    def get_failed_events_for_retry(self, limit: int = 50) -> List[Event]:
        """Get failed events that are eligible for retry."""
        events = self.get_events_by_status(EventStatus.FAILED.value, limit=limit)
        return [e for e in events if e.can_retry()]

    def get_event_count_by_status(self) -> Dict[str, int]:
        """Get count of events grouped by status."""
        counts = {}
        for status in EventStatus:
            response = self.table.query(
                IndexName="status-index",
                KeyConditionExpression="#status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": status.value},
                Select="COUNT",
            )
            counts[status.value] = response.get("Count", 0)
        return counts
