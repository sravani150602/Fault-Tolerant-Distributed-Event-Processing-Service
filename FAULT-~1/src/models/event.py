"""Event data models for the distributed event processing service."""

import uuid
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Dict, Any


class EventStatus(Enum):
    """Status of an event in the processing pipeline."""
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEDUPLICATED = "DEDUPLICATED"


class EventType(Enum):
    """Types of events supported by the service."""
    S3_OBJECT_CREATED = "s3:ObjectCreated"
    S3_OBJECT_REMOVED = "s3:ObjectRemoved"
    API_REQUEST = "api:Request"
    SCHEDULED = "scheduled:Trigger"
    CUSTOM = "custom:Event"


@dataclass
class Event:
    """Represents a processing event in the system."""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = EventType.CUSTOM.value
    source: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    status: str = EventStatus.RECEIVED.value
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    processed_at: Optional[float] = None
    idempotency_key: Optional[str] = None
    error_message: Optional[str] = None
    processing_duration_ms: Optional[float] = None
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None

    def __post_init__(self):
        if self.idempotency_key is None:
            self.idempotency_key = self._generate_idempotency_key()

    def _generate_idempotency_key(self) -> str:
        """Generate a deterministic idempotency key based on event content."""
        import hashlib
        import json
        key_data = {
            "event_type": self.event_type,
            "source": self.source,
            "payload": json.dumps(self.payload, sort_keys=True),
            "s3_bucket": self.s3_bucket,
            "s3_key": self.s3_key,
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for DynamoDB storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Create an Event from a dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def mark_processing(self):
        self.status = EventStatus.PROCESSING.value
        self.updated_at = time.time()

    def mark_completed(self, duration_ms: float):
        self.status = EventStatus.COMPLETED.value
        self.processed_at = time.time()
        self.updated_at = time.time()
        self.processing_duration_ms = duration_ms

    def mark_failed(self, error: str):
        self.status = EventStatus.FAILED.value
        self.error_message = error
        self.updated_at = time.time()

    def mark_retrying(self):
        self.status = EventStatus.RETRYING.value
        self.retry_count += 1
        self.updated_at = time.time()

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries


@dataclass
class EventMetrics:
    """Metrics for event processing."""
    total_events: int = 0
    successful_events: int = 0
    failed_events: int = 0
    deduplicated_events: int = 0
    retried_events: int = 0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    throughput_per_second: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
