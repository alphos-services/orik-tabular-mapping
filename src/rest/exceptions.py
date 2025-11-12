class OrikClientError(Exception):
    """Base exception for all client errors."""


class OrikRateLimitError(OrikClientError):
    """Raised when API returns 429 Too Many Requests."""


class OrikValidationError(OrikClientError):
    """Raised when the API response format is invalid or malformed."""


class OrikHTTPError(OrikClientError):
    """Raised for unexpected non-2xx HTTP responses."""
