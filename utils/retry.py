"""Retry utilities for network/IO operations.

This module provides a retry decorator that is instance-aware (will use
`self.logger` if available), supports exponential backoff, jitter, and
raises a custom exception when exhausted.
"""
from __future__ import annotations

import time
import random
import logging
from functools import wraps
from typing import Callable, Tuple, Type, Any

from googleapiclient.errors import HttpError

from .exceptions import MaxRetriesExceeded


def retry_on_exception(
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    exceptions: Tuple[Type[BaseException], ...] = (HttpError,),
    jitter: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to retry a function on specified exceptions.

    Args:
        max_retries: number of retries after the initial attempt.
        backoff_factor: base multiplier for exponential backoff. Wait time
            for attempt n is backoff_factor * (2 ** (n - 1)).
        exceptions: tuple of exception classes to catch and retry on.
        jitter: whether to add uniform jitter to wait time to avoid thundering herd.

    Behavior:
        - Uses `self.logger` if available on the first positional arg.
        - Re-raises non-specified exceptions immediately.
        - Raises MaxRetriesExceeded when retries are exhausted.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # try to use instance logger if present
            logger = None
            if args:
                possible_self = args[0]
                logger = getattr(possible_self, "logger", None)
            if logger is None:
                logger = logging.getLogger(func.__module__)

            attempt = 0
            while True:
                try:
                    attempt += 1
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt > max_retries:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__,
                            attempt - 1,
                            exc,
                        )
                        raise MaxRetriesExceeded(
                            f"{func.__name__} failed after {max_retries} retries"
                        ) from exc

                    # compute backoff: exponential with optional jitter
                    wait = backoff_factor * (2 ** (attempt - 1))
                    if jitter:
                        wait = wait * (1 + random.uniform(-0.1, 0.1))

                    logger.warning(
                        "[RETRY] %s raised %s. Retrying in %.2fs (attempt %d/%d)",
                        func.__name__,
                        exc,
                        wait,
                        attempt,
                        max_retries,
                    )
                    time.sleep(wait)
                except Exception:
                    # non-retryable: immediately propagate
                    raise

        return wrapper

    return decorator
