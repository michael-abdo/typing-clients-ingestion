#!/usr/bin/env python3
"""
Minimal rate limiter replacement for dry test.
"""
import time
from functools import wraps


def rate_limit(service: str):
    """Minimal rate limiting decorator."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Just add a small delay
            time.sleep(0.1)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def wait_for_rate_limit(service: str):
    """Wait for rate limit - minimal implementation."""
    time.sleep(0.1)