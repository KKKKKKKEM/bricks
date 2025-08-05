from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Request(_message.Message):
    __slots__ = ("method", "data", "request_id")
    METHOD_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    method: str
    data: str
    request_id: str
    def __init__(self, method: _Optional[str] = ..., data: _Optional[str] = ..., request_id: _Optional[str] = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ("data", "message", "code", "request_id")
    DATA_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    data: str
    message: str
    code: int
    request_id: str
    def __init__(self, data: _Optional[str] = ..., message: _Optional[str] = ..., code: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...
