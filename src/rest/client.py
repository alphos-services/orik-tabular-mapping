from typing import Union, Dict, Any

import httpx

from .exceptions import OrikClientError, OrikHTTPError, OrikRateLimitError, OrikValidationError
from .models import ValidateMappingRequest, ValidateMappingResponse


class OrikTabularClient:
    """
    Client for interacting with the Orik Tabular Mapping API of the ORIK Platform.

    Example:
        >>> from src.rest.client import OrikTabularClient
        >>> client = OrikTabularClient(base_url="https://api.alphos-services.com/v1")
        >>> response = client.validate_mapping({
        ...     "mapping": {...},
        ...     "sample_data": {...}
        ... })
    """

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    def validate_mapping(self, request: Union[ValidateMappingRequest, Dict[str, Any]]) -> ValidateMappingResponse:
        url = f"{self.base_url}/otm/validate"
        try:
            if isinstance(request, dict):
                req = ValidateMappingRequest(**request)
                req = req.model_dump()
            else:
                req = request.model_dump()
            response = self._client.post(url, json=req)

            if response.status_code == 429:
                raise OrikRateLimitError("Rate limit exceeded. Please retry later.")

            if not response.is_success:
                raise OrikHTTPError(f"Unexpected status code: {response.status_code}")

            try:
                data = response.json()
                return ValidateMappingResponse(**data)
            except Exception as e:
                raise OrikValidationError(f"Invalid response format: {e}")

        except httpx.RequestError as e:
            raise OrikClientError(f"Request failed: {e}") from e

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
