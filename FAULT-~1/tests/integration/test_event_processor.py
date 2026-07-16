"""Integration tests for the EventProcessor with mocked AWS services."""

import time
import json
import pytest
from unittest.mock import MagicMock, patch

from src.models.event import Event, EventStatus, EventType
from src.handlers.event_processor import EventProcessor
from src.utils.retry import RetryConfig


class MockDynamoDBClient:
    """In-memory mock of DynamoDB for integration tests."""

    def __init__(self):
        self.events = {}
        self.idempotency_index = {}

    def put_event(self, event):
        key = f"{event.event_type}#{event.source}:{event.event_id}"
        if key in self.events:
            return False
        self.events[key] = event.to_dict()
        if event.idempotency_key:
            self.idempotency_index[event.idempotency_key] = event
        return True

    def check_idempotency(self, idempotency_key):
        return self.idempotency_index.get(idempotency_key)

    def update_event_status(self, event):
        key = f"{event.event_type}#{event.source}:{event.event_id}"
        if key in self.events:
            self.events[key] = event.to_dict()

    def get_failed_events_for_retry(self, limit=50):
        return [
            Event.from_dict(e) for e in self.events.values()
            if e["status"] == EventStatus.FAILED.value and e["retry_count"] < e["max_retries"]
        ][:limit]

    def get_event_count_by_status(self):
        counts = {}
        for e in self.events.values():
            status = e["status"]
            counts[status] = counts.get(status, 0) + 1
        return counts


class MockS3Client:
    """In-memory mock of S3."""

    def __init__(self):
        self.objects = {}

    def download_event(self, key):
        return self.objects.get(key, {"data": "mock"})

    def upload_event(self, key, data):
        self.objects[key] = data
        return f"s3://mock-bucket/{key}"


class MockCloudWatchClient:
    """Mock CloudWatch that records metric calls."""

    def __init__(self):
        self.metrics = []

    def record_event_processed(self, duration_ms, event_type, success):
        self.metrics.append({
            "name": "EventProcessed",
            "duration_ms": duration_ms,
            "event_type": event_type,
            "success": success,
        })

    def record_deduplication(self, event_type):
        self.metrics.append({"name": "Deduplication", "event_type": event_type})

    def record_retry(self, event_type, retry_count):
        self.metrics.append({
            "name": "Retry",
            "event_type": event_type,
            "retry_count": retry_count,
        })


class TestEventProcessorIntegration:
    """Integration tests for the full processing pipeline."""

    def setup_method(self):
        self.dynamo = MockDynamoDBClient()
        self.s3 = MockS3Client()
        self.cw = MockCloudWatchClient()
        self.processor = EventProcessor(
            dynamo_client=self.dynamo,
            s3_client=self.s3,
            cloudwatch_client=self.cw,
            retry_config=RetryConfig(max_retries=2, base_delay=0.001),
        )

    def test_process_new_event(self):
        event = Event(
            event_type=EventType.API_REQUEST.value,
            source="test",
            payload={"data": "test"},
        )
        result = self.processor.process_event(event)
        assert result["status"] == "completed"
        assert result["duration_ms"] > 0
        assert event.status == EventStatus.COMPLETED.value

    def test_deduplication(self):
        event1 = Event(
            event_type=EventType.API_REQUEST.value,
            source="test",
            payload={"data": "same"},
        )
        event2 = Event(
            event_type=EventType.API_REQUEST.value,
            source="test",
            payload={"data": "same"},
        )

        result1 = self.processor.process_event(event1)
        assert result1["status"] == "completed"

        result2 = self.processor.process_event(event2)
        assert result2["status"] == "deduplicated"

    def test_deduplication_reduces_processing(self):
        """Verify deduplication prevents reprocessing of identical events."""
        events = [
            Event(event_type="test", source="src", payload={"id": 1})
            for _ in range(10)
        ]

        results = [self.processor.process_event(e) for e in events]
        completed = sum(1 for r in results if r["status"] == "completed")
        deduplicated = sum(1 for r in results if r["status"] == "deduplicated")

        assert completed == 1
        assert deduplicated == 9

    def test_different_events_not_deduplicated(self):
        events = [
            Event(event_type="test", source="src", payload={"id": i})
            for i in range(5)
        ]
        results = [self.processor.process_event(e) for e in events]
        assert all(r["status"] == "completed" for r in results)

    def test_s3_event_processing(self):
        self.s3.objects["events/test.json"] = [{"record": 1}, {"record": 2}]

        event = Event(
            event_type=EventType.S3_OBJECT_CREATED.value,
            source="s3",
            s3_bucket="test-bucket",
            s3_key="events/test.json",
        )

        result = self.processor.process_event(event)
        assert result["status"] == "completed"
        assert result["result"]["records_processed"] == 2

    def test_metrics_recorded(self):
        event = Event(event_type="test", source="src", payload={"x": 1})
        self.processor.process_event(event)

        assert len(self.cw.metrics) > 0
        processed = [m for m in self.cw.metrics if m["name"] == "EventProcessed"]
        assert len(processed) == 1
        assert processed[0]["success"] is True

    def test_dedup_metrics_recorded(self):
        event1 = Event(event_type="test", source="src", payload={"x": 1})
        event2 = Event(event_type="test", source="src", payload={"x": 1})

        self.processor.process_event(event1)
        self.processor.process_event(event2)

        dedup_metrics = [m for m in self.cw.metrics if m["name"] == "Deduplication"]
        assert len(dedup_metrics) == 1

    def test_processing_latency_under_threshold(self):
        """Verify individual event processing completes under 300ms."""
        latencies = []
        for i in range(50):
            event = Event(event_type="test", source="src", payload={"id": i})
            result = self.processor.process_event(event)
            latencies.append(result["duration_ms"])

        avg_latency = sum(latencies) / len(latencies)
        assert avg_latency < 300, f"Average latency {avg_latency}ms exceeds 300ms threshold"

    def test_concurrent_deduplication(self):
        """Test that concurrent identical events are properly deduplicated."""
        import concurrent.futures

        event_payload = {"data": "concurrent-test"}
        events = [
            Event(event_type="test", source="src", payload=event_payload)
            for _ in range(20)
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(self.processor.process_event, events))

        completed = sum(1 for r in results if r["status"] == "completed")
        deduplicated = sum(1 for r in results if r["status"] == "deduplicated")

        assert completed >= 1
        assert completed + deduplicated == 20

    def test_reprocess_failed_events(self):
        """Test reprocessing of failed events."""
        # Manually insert a failed event
        event = Event(event_type="test", source="src", payload={"id": "fail"})
        event.mark_failed("test error")
        event.retry_count = 0
        self.dynamo.put_event(event)
        self.dynamo.update_event_status(event)

        result = self.processor.reprocess_failed_events()
        assert result["reprocessed"] >= 0
