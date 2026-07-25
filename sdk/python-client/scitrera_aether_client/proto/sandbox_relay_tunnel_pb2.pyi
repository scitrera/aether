import aether_pb2 as _aether_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TunnelFrame(_message.Message):
    __slots__ = ("up", "down", "hello")
    UP_FIELD_NUMBER: _ClassVar[int]
    DOWN_FIELD_NUMBER: _ClassVar[int]
    HELLO_FIELD_NUMBER: _ClassVar[int]
    up: _aether_pb2.UpstreamMessage
    down: _aether_pb2.DownstreamMessage
    hello: TunnelHello
    def __init__(self, up: _Optional[_Union[_aether_pb2.UpstreamMessage, _Mapping]] = ..., down: _Optional[_Union[_aether_pb2.DownstreamMessage, _Mapping]] = ..., hello: _Optional[_Union[TunnelHello, _Mapping]] = ...) -> None: ...

class TunnelHello(_message.Message):
    __slots__ = ("tenant",)
    TENANT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    def __init__(self, tenant: _Optional[str] = ...) -> None: ...

class WatchTenantsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TenantEvent(_message.Message):
    __slots__ = ("tenant", "online", "snapshot_complete")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ONLINE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    online: bool
    snapshot_complete: bool
    def __init__(self, tenant: _Optional[str] = ..., online: _Optional[bool] = ..., snapshot_complete: _Optional[bool] = ...) -> None: ...
