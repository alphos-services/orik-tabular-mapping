from typing import Dict, Any, Optional, List, Union
from pydantic import BaseModel


class ValidateMappingRequest(BaseModel):
    mapping: Dict[str, Any]
    sample_data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None
    sample_is_batched: bool = False


class ValidateMappingResponse(BaseModel):
    is_valid: bool
    errors: Optional[List[str]] = None
    sample_result: Optional[List[Dict[str, Any]]] = None
    sample_is_valid: Optional[bool] = None
    sample_error: Optional[str] = None
