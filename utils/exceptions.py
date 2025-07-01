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


# Error handling moved to utils/error_handling.py for comprehensive categorization and recovery