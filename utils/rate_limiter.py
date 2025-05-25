#!/usr/bin/env python3
"""
Centralized rate limiting for API calls and web requests.
Prevents getting blocked by services due to excessive requests.
"""
import time
import threading
from typing import Dict, Optional, Callable
from functools import wraps
from collections import defaultdict

try:
    from config import get_config
    from logging_config import get_logger
except ImportError:
    from .config import get_config
    from .logging_config import get_logger

# Setup module logger
logger = get_logger(__name__)

# Get configuration
config = get_config()


class RateLimiter:
    """Thread-safe rate limiter for controlling request frequency"""
    
    def __init__(self, rate: float = None, burst: int = None):
        """
        Initialize rate limiter.
        
        Args:
            rate: Maximum requests per second (defaults from config)
            burst: Maximum burst size (defaults to rate)
        """
        self.rate = rate if rate is not None else config.get('rate_limiting.default_rate', 2.0)
        self.burst = burst if burst is not None else int(self.rate)
        self.tokens = float(self.burst)
        self.last_update = time.time()
        self.lock = threading.Lock()
        
        logger.debug(f"Rate limiter initialized: {self.rate} req/s, burst={self.burst}")
    
    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the rate limiter.
        
        Args:
            tokens: Number of tokens to acquire
            blocking: Whether to block until tokens are available
            timeout: Maximum time to wait (None for infinite)
        
        Returns:
            True if tokens were acquired, False otherwise
        """
        if tokens > self.burst:
            raise ValueError(f"Cannot acquire {tokens} tokens, burst size is {self.burst}")
        
        start_time = time.time()
        
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                
                # Refill tokens based on elapsed time
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                # Try to acquire tokens
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
            
            if not blocking:
                return False
            
            # Calculate wait time
            wait_time = (tokens - self.tokens) / self.rate
            
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed + wait_time > timeout:
                    return False
                wait_time = min(wait_time, timeout - elapsed)
            
            # Wait before retrying
            time.sleep(min(wait_time, 0.1))


class ServiceRateLimiter:
    """Manages rate limits for different services"""
    
    def __init__(self):
        self.limiters: Dict[str, RateLimiter] = {}
        self.lock = threading.Lock()
        
        # Initialize service-specific rate limiters from config
        self._init_service_limits()
    
    def _init_service_limits(self):
        """Initialize rate limiters for known services"""
        services = config.get('rate_limiting.services', {})
        
        # Default service configurations
        default_services = {
            'youtube': {'rate': 2.0, 'burst': 5},
            'google_drive': {'rate': 3.0, 'burst': 10},
            'google_docs': {'rate': 1.0, 'burst': 3},
            'selenium': {'rate': 0.5, 'burst': 2}
        }
        
        # Merge with config
        for service, limits in default_services.items():
            config_limits = services.get(service, {})
            rate = config_limits.get('rate', limits['rate'])
            burst = config_limits.get('burst', limits['burst'])
            
            self.limiters[service] = RateLimiter(rate=rate, burst=burst)
            logger.info(f"Rate limiter for {service}: {rate} req/s, burst={burst}")
    
    def get_limiter(self, service: str) -> RateLimiter:
        """Get or create a rate limiter for a service"""
        with self.lock:
            if service not in self.limiters:
                # Create default limiter for unknown services
                self.limiters[service] = RateLimiter()
                logger.warning(f"Using default rate limiter for unknown service: {service}")
            
            return self.limiters[service]
    
    def acquire(self, service: str, tokens: int = 1, **kwargs) -> bool:
        """Acquire tokens for a specific service"""
        limiter = self.get_limiter(service)
        return limiter.acquire(tokens, **kwargs)
    
    def wait(self, service: str, tokens: int = 1):
        """Wait until tokens are available for a service"""
        limiter = self.get_limiter(service)
        limiter.acquire(tokens, blocking=True)


# Global service rate limiter instance
_service_limiter = None


def get_service_limiter() -> ServiceRateLimiter:
    """Get the global service rate limiter instance"""
    global _service_limiter
    if _service_limiter is None:
        _service_limiter = ServiceRateLimiter()
    return _service_limiter


def rate_limit(service: str, tokens: int = 1):
    """
    Decorator to rate limit function calls.
    
    Args:
        service: Service name for rate limiting
        tokens: Number of tokens to consume per call
    
    Example:
        @rate_limit('youtube')
        def download_video(url):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter = get_service_limiter()
            
            # Log when we're waiting
            start_time = time.time()
            limiter.wait(service, tokens)
            wait_time = time.time() - start_time
            
            if wait_time > 0.1:
                logger.debug(f"Rate limit wait for {service}: {wait_time:.2f}s")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


class URLRateLimiter:
    """Rate limiter based on URL domains"""
    
    def __init__(self):
        self.domain_limiters: Dict[str, RateLimiter] = defaultdict(lambda: RateLimiter())
        self.service_limiter = get_service_limiter()
    
    def get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Map domains to services
        if 'youtube.com' in domain or 'youtu.be' in domain:
            return 'youtube'
        elif 'drive.google.com' in domain:
            return 'google_drive'
        elif 'docs.google.com' in domain:
            return 'google_docs'
        else:
            return domain
    
    def acquire(self, url: str, tokens: int = 1, **kwargs) -> bool:
        """Acquire tokens for a URL"""
        service = self.get_domain(url)
        return self.service_limiter.acquire(service, tokens, **kwargs)
    
    def wait(self, url: str, tokens: int = 1):
        """Wait for tokens for a URL"""
        service = self.get_domain(url)
        self.service_limiter.wait(service, tokens)


# Convenience functions
def wait_for_rate_limit(service: str, tokens: int = 1):
    """Wait for rate limit tokens"""
    limiter = get_service_limiter()
    limiter.wait(service, tokens)


def check_rate_limit(service: str, tokens: int = 1) -> bool:
    """Check if rate limit allows request (non-blocking)"""
    limiter = get_service_limiter()
    return limiter.acquire(service, tokens, blocking=False)


if __name__ == "__main__":
    # Test the rate limiting system
    logger.info("Testing rate limiting system...")
    
    # Test basic rate limiter
    limiter = RateLimiter(rate=2.0, burst=3)
    
    logger.info("Testing burst capability...")
    for i in range(5):
        acquired = limiter.acquire(blocking=False)
        logger.info(f"Request {i+1}: {'Allowed' if acquired else 'Blocked'}")
    
    # Test service rate limiter
    logger.info("\nTesting service rate limiter...")
    service_limiter = get_service_limiter()
    
    # Simulate YouTube requests
    logger.info("Simulating YouTube requests...")
    for i in range(5):
        start = time.time()
        service_limiter.wait('youtube')
        elapsed = time.time() - start
        logger.info(f"YouTube request {i+1}: waited {elapsed:.2f}s")
    
    # Test decorator
    @rate_limit('youtube')
    def mock_download(video_id):
        logger.info(f"Downloading video: {video_id}")
        return True
    
    logger.info("\nTesting rate limit decorator...")
    for i in range(3):
        mock_download(f"video_{i}")
    
    logger.info("Rate limiting tests completed!")