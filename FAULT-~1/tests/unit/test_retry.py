"""Unit tests for retry logic."""

import time
import pytest
from unittest.mock import MagicMock, patch

from src.utils.retry import (
    RetryConfig,
    RetryExhaustedError,
    calculate_backoff,
    with_retry,
    retry_event_processing,
)
from src.models.event import Event


class TestRetryConfig:
    def test_default_config(self):
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 0.1
        assert config.jitter is True

    def test_custom_config(self):
        config = RetryConfig(max_retries=5, base_delay=0.5, jitter=False)
        assert config.max_retries == 5
        assert config.base_delay == 0.5
        assert config.jitter is False


class TestCalculateBackoff:
    def test_exponential_growth(self):
        config = RetryConfig(base_delay=1.0, jitter=False)
        assert calculate_backoff(0, config) == 1.0
        assert calculate_backoff(1, config) == 2.0
        assert calculate_backoff(2, config) == 4.0

    def test_respects_max_delay(self):
        config = RetryConfig(base_delay=1.0, max_delay=5.0, jitter=False)
        assert calculate_backoff(10, config) == 5.0

    def test_jitter_within_range(self):
        config = RetryConfig(base_delay=1.0, jitter=True)
        for _ in range(100):
            delay = calculate_backoff(0, config)
            assert 0.5 <= delay <= 1.0


class TestWithRetryDecorator:
    def test_succeeds_first_try(self):
        call_count = 0

        @with_retry(RetryConfig(max_retries=3, base_delay=0.001))
        def success():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = success()
        assert result == "ok"
        assert call_count == 1

    def test_succeeds_after_retries(self):
        call_count = 0

        @with_retry(RetryConfig(max_retries=3, base_delay=0.001))
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient error")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 3

    def test_exhausts_retries(self):
        @with_retry(RetryConfig(max_retries=2, base_delay=0.001))
        def always_fails():
            raise ValueError("permanent error")

        with pytest.raises(RetryExhaustedError) as exc_info:
            always_fails()
        assert exc_info.value.attempts == 3
        assert "permanent error" in str(exc_info.value.last_exception)

    def test_only_retries_specified_exceptions(self):
        config = RetryConfig(
            max_retries=3,
            base_delay=0.001,
            retryable_exceptions=(ConnectionError,),
        )

        @with_retry(config)
        def wrong_error():
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            wrong_error()


class TestRetryEventProcessing:
    def test_successful_processing(self):
        event = Event(max_retries=3)
        func = MagicMock(return_value={"status": "ok"})

        result = retry_event_processing(
            func=func,
            event=event,
            config=RetryConfig(max_retries=3, base_delay=0.001),
        )
        assert result == {"status": "ok"}
        func.assert_called_once()

    def test_retry_updates_event(self):
        event = Event(max_retries=3)
        call_count = 0

        def flaky(e):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("transient")
            return {"status": "ok"}

        on_retry = MagicMock()
        retry_event_processing(
            func=flaky,
            event=event,
            config=RetryConfig(max_retries=3, base_delay=0.001),
            on_retry=on_retry,
        )
        assert event.retry_count == 1
        on_retry.assert_called_once()

    def test_failure_calls_on_failure(self):
        event = Event(max_retries=1)
        on_failure = MagicMock()

        def always_fails(e):
            raise ValueError("permanent")

        with pytest.raises(RetryExhaustedError):
            retry_event_processing(
                func=always_fails,
                event=event,
                config=RetryConfig(max_retries=1, base_delay=0.001),
                on_failure=on_failure,
            )
        on_failure.assert_called_once()
        assert event.status == "FAILED"
