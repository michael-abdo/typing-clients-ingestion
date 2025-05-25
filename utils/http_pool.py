"""HTTP connection pooling for improved performance."""
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Optional, Dict, Any
import logging

try:
    from config import get_config, is_ssl_verify_enabled
except ImportError:
    from .config import get_config, is_ssl_verify_enabled

# Get configuration
config = get_config()

# Global session instance
_session = None

class HTTPPool:
    """HTTP connection pool with retry logic and configuration."""
    
    def __init__(self, 
                 pool_connections: int = 10,
                 pool_maxsize: int = 10,
                 max_retries: int = 3,
                 backoff_factor: float = 1.0,
                 status_forcelist: Optional[list] = None,
                 verify_ssl: Optional[bool] = None):
        """
        Initialize HTTP connection pool.
        
        Args:
            pool_connections: Number of connection pools to cache
            pool_maxsize: Maximum number of connections to save in the pool
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff factor for retries
            status_forcelist: HTTP status codes to retry
            verify_ssl: Whether to verify SSL certificates
        """
        self.session = requests.Session()
        
        # Get retry configuration from config
        retry_config = config.get_section('retry')
        self.max_retries = max_retries or retry_config.get('max_attempts', 3)
        self.backoff_factor = backoff_factor or retry_config.get('base_delay', 1.0)
        
        # Default status codes to retry
        if status_forcelist is None:
            status_forcelist = [408, 429, 500, 502, 503, 504]
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"],
            raise_on_status=False
        )
        
        # Configure connection pooling
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=retry_strategy
        )
        
        # Mount adapter for both http and https
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        headers = {
            'User-Agent': config.get('web_scraping.user_agent', 
                                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
            'Accept': config.get('web_scraping.accept_header', 
                               'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
            'Accept-Language': config.get('web_scraping.accept_language', 'en-US,en;q=0.5'),
        }
        self.session.headers.update(headers)
        
        # SSL verification
        if verify_ssl is None:
            verify_ssl = is_ssl_verify_enabled()
        self.session.verify = verify_ssl
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.get(url, timeout=timeout, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.post(url, timeout=timeout, **kwargs)
    
    def put(self, url: str, **kwargs) -> requests.Response:
        """Make PUT request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.put(url, timeout=timeout, **kwargs)
    
    def delete(self, url: str, **kwargs) -> requests.Response:
        """Make DELETE request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.delete(url, timeout=timeout, **kwargs)
    
    def head(self, url: str, **kwargs) -> requests.Response:
        """Make HEAD request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.head(url, timeout=timeout, **kwargs)
    
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make generic request with connection pooling."""
        timeout = kwargs.pop('timeout', config.get('timeouts.http_request', 60.0))
        return self.session.request(method, url, timeout=timeout, **kwargs)
    
    def close(self):
        """Close the session and all connections."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_http_pool() -> HTTPPool:
    """
    Get singleton HTTP pool instance.
    
    Returns:
        HTTPPool instance
    """
    global _session
    
    if _session is None:
        _session = HTTPPool()
    
    return _session


def close_http_pool():
    """Close the global HTTP pool."""
    global _session
    
    if _session is not None:
        _session.close()
        _session = None


# Convenience functions that use the global pool
def get(url: str, **kwargs) -> requests.Response:
    """Make GET request using global connection pool."""
    return get_http_pool().get(url, **kwargs)


def post(url: str, **kwargs) -> requests.Response:
    """Make POST request using global connection pool."""
    return get_http_pool().post(url, **kwargs)


def put(url: str, **kwargs) -> requests.Response:
    """Make PUT request using global connection pool."""
    return get_http_pool().put(url, **kwargs)


def delete(url: str, **kwargs) -> requests.Response:
    """Make DELETE request using global connection pool."""
    return get_http_pool().delete(url, **kwargs)


def head(url: str, **kwargs) -> requests.Response:
    """Make HEAD request using global connection pool."""
    return get_http_pool().head(url, **kwargs)


# Register cleanup on exit
import atexit
atexit.register(close_http_pool)


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Testing HTTP connection pooling...")
    
    # Test with global pool
    response = get("https://httpbin.org/get")
    print(f"Status: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    
    # Test with context manager
    with HTTPPool() as pool:
        response = pool.get("https://httpbin.org/delay/1")
        print(f"\nDelayed response status: {response.status_code}")
        
        # Test multiple requests (connection reuse)
        for i in range(3):
            response = pool.get(f"https://httpbin.org/get?request={i}")
            print(f"Request {i+1} status: {response.status_code}")
    
    print("\n✓ HTTP connection pooling test complete!")