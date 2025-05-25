"""Custom exceptions for better error handling and propagation"""


class PipelineError(Exception):
    """Base exception for all pipeline errors"""
    pass


class NetworkError(PipelineError):
    """Raised when network operations fail"""
    pass


class DownloadError(PipelineError):
    """Raised when download operations fail"""
    pass


class ScrapingError(PipelineError):
    """Raised when web scraping fails"""
    pass


class DataError(PipelineError):
    """Raised when data processing fails"""
    pass


class ConfigError(PipelineError):
    """Raised when configuration is invalid"""
    pass


class AuthenticationError(PipelineError):
    """Raised when authentication fails"""
    pass


def handle_error(logger, error_msg, exception=None, raise_error=True):
    """
    Centralized error handling function
    
    Args:
        logger: Logger instance
        error_msg: Error message to log
        exception: Original exception (if any)
        raise_error: Whether to raise an exception after logging
    
    Raises:
        Appropriate PipelineError subclass based on exception type
    """
    # Log the error
    if exception:
        logger.error(f"{error_msg}: {str(exception)}")
    else:
        logger.error(error_msg)
    
    if raise_error:
        # Determine appropriate exception type
        if exception:
            if isinstance(exception, (ConnectionError, TimeoutError)):
                raise NetworkError(error_msg) from exception
            elif isinstance(exception, IOError):
                raise DownloadError(error_msg) from exception
            elif isinstance(exception, (KeyError, ValueError, IndexError)):
                raise DataError(error_msg) from exception
            else:
                # Generic pipeline error
                raise PipelineError(error_msg) from exception
        else:
            raise PipelineError(error_msg)