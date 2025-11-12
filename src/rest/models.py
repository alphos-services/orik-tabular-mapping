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


class UploadDataRequest(BaseModel):
    auth_token: str
    mapping_uuid: str
    data: Union[Dict[str, Any], List[Dict[str, Any]]]
    is_batched: Optional[bool] = False


class UploadDataResponse(BaseModel):
    success: bool
    mapping_uuid: Optional[str] = None
    duration: int
    message: Optional[str] = None
    processed_records: Optional[int] = None
