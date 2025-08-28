"""Custom exceptions for the project."""

class MaxRetriesExceeded(Exception):
    """Raised when a retry loop exceeded the allowed number of attempts."""
    pass
