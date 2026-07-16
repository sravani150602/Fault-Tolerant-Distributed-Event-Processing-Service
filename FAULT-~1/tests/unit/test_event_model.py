"""Unit tests for the Event model."""

import time
import json
import pytest
from src.models.event import Event, EventStatus, EventType, EventMetrics


class TestEvent:
    """Tests for the Event dataclass."""

    def test_create_event_defaults(self):
        event = Event()
        assert event.event_id is not None
        assert event.status == EventStatus.RECEIVED.value
        assert event.retry_count == 0
        assert event.max_retries == 3

    def test_create_event_with_params(self):
        event = Event(
            event_type=EventType.S3_OBJECT_CREATED.value,
            source="test-source",
            payload={"key": "value"},
        )
        assert event.event_type == "s3:ObjectCreated"
        assert event.source == "test-source"
        assert event.payload == {"key": "value"}

    def test_idempotency_key_generated(self):
        event = Event(event_type="test", source="src", payload={"data": "test"})
        assert event.idempotency_key is not None
        assert len(event.idempotency_key) == 32

    def test_idempotency_key_deterministic(self):
        """Same content should produce the same idempotency key."""
        event1 = Event(event_type="test", source="src", payload={"a": 1})
        event2 = Event(event_type="test", source="src", payload={"a": 1})
        assert event1.idempotency_key == event2.idempotency_key

    def test_idempotency_key_differs_for_different_content(self):
        event1 = Event(event_type="test", source="src", payload={"a": 1})
        event2 = Event(event_type="test", source="src", payload={"a": 2})
        assert event1.idempotency_key != event2.idempotency_key

    def test_custom_idempotency_key(self):
        event = Event(idempotency_key="custom-key-123")
        assert event.idempotency_key == "custom-key-123"

    def test_mark_processing(self):
        event = Event()
        event.mark_processing()
        assert event.status == EventStatus.PROCESSING.value

    def test_mark_completed(self):
        event = Event()
        event.mark_completed(duration_ms=150.5)
        assert event.status == EventStatus.COMPLETED.value
        assert event.processing_duration_ms == 150.5
        assert event.processed_at is not None

    def test_mark_failed(self):
        event = Event()
        event.mark_failed("Connection timeout")
        assert event.status == EventStatus.FAILED.value
        assert event.error_message == "Connection timeout"

    def test_mark_retrying(self):
        event = Event()
        event.mark_retrying()
        assert event.status == EventStatus.RETRYING.value
        assert event.retry_count == 1

    def test_can_retry_within_limit(self):
        event = Event(max_retries=3)
        assert event.can_retry() is True
        event.retry_count = 2
        assert event.can_retry() is True

    def test_cannot_retry_at_limit(self):
        event = Event(max_retries=3)
        event.retry_count = 3
        assert event.can_retry() is False

    def test_to_dict(self):
        event = Event(event_type="test", source="src")
        d = event.to_dict()
        assert isinstance(d, dict)
        assert d["event_type"] == "test"
        assert d["source"] == "src"
        assert "event_id" in d

    def test_from_dict(self):
        original = Event(event_type="test", source="src", payload={"x": 1})
        d = original.to_dict()
        restored = Event.from_dict(d)
        assert restored.event_type == original.event_type
        assert restored.source == original.source
        assert restored.payload == original.payload
        assert restored.event_id == original.event_id

    def test_from_dict_ignores_extra_fields(self):
        d = {"event_type": "test", "source": "src", "extra_field": "ignored"}
        event = Event.from_dict(d)
        assert event.event_type == "test"

    def test_s3_event_fields(self):
        event = Event(
            event_type=EventType.S3_OBJECT_CREATED.value,
            s3_bucket="my-bucket",
            s3_key="events/test.json",
        )
        assert event.s3_bucket == "my-bucket"
        assert event.s3_key == "events/test.json"


class TestEventMetrics:
    def test_metrics_defaults(self):
        m = EventMetrics()
        assert m.total_events == 0
        assert m.avg_latency_ms == 0.0

    def test_metrics_to_dict(self):
        m = EventMetrics(total_events=100, successful_events=95)
        d = m.to_dict()
        assert d["total_events"] == 100
        assert d["successful_events"] == 95
