"""Retry logic with exponential backoff and jitter."""

import time
import random
import logging
import functools
from typing import Callable, Optional, Tuple, Type

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or (Exception,)


class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, attempts: int, last_exception: Exception):
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"Retry exhausted after {attempts} attempts. Last error: {last_exception}"
        )


def calculate_backoff(attempt: int, config: RetryConfig) -> float:
    """Calculate delay with exponential backoff and optional jitter."""
    delay = config.base_delay * (config.exponential_base ** attempt)
    delay = min(delay, config.max_delay)

    if config.jitter:
        delay = delay * (0.5 + random.random() * 0.5)

    return delay


def with_retry(config: Optional[RetryConfig] = None):
    """
    Decorator for adding retry logic with exponential backoff.

    Usage:
        @with_retry(RetryConfig(max_retries=3))
        def process_event(event):
            ...
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e
                    if attempt < config.max_retries:
                        delay = calculate_backoff(attempt, config)
                        logger.warning(
                            f"Attempt {attempt + 1}/{config.max_retries + 1} failed for "
                            f"{func.__name__}: {e}. Retrying in {delay:.2f}s"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {config.max_retries + 1} attempts failed for {func.__name__}"
                        )
            raise RetryExhaustedError(config.max_retries + 1, last_exception)

        return wrapper
    return decorator


def retry_event_processing(
    func: Callable,
    event,
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable] = None,
    on_failure: Optional[Callable] = None,
):
    """
    Execute a function with retry logic, tracking retry state on the event.

    Args:
        func: The processing function to execute
        event: The event being processed (must have mark_retrying, mark_failed, can_retry methods)
        config: Retry configuration
        on_retry: Callback invoked on each retry (receives event and attempt number)
        on_failure: Callback invoked on final failure (receives event and exception)
    """
    if config is None:
        config = RetryConfig(max_retries=event.max_retries)

    last_exception = None
    for attempt in range(config.max_retries + 1):
        try:
            result = func(event)
            return result
        except config.retryable_exceptions as e:
            last_exception = e
            if attempt < config.max_retries and event.can_retry():
                delay = calculate_backoff(attempt, config)
                event.mark_retrying()
                logger.warning(
                    f"Event {event.event_id} attempt {attempt + 1} failed: {e}. "
                    f"Retry {event.retry_count}/{event.max_retries} in {delay:.2f}s"
                )
                if on_retry:
                    on_retry(event, attempt + 1)
                time.sleep(delay)
            else:
                event.mark_failed(str(e))
                logger.error(f"Event {event.event_id} permanently failed: {e}")
                if on_failure:
                    on_failure(event, e)
                raise RetryExhaustedError(attempt + 1, e)

    event.mark_failed(str(last_exception))
    if on_failure:
        on_failure(event, last_exception)
    raise RetryExhaustedError(config.max_retries + 1, last_exception)
