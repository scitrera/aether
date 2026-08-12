from unittest.mock import AsyncMock, MagicMock

import pytest

from scitrera_aether_client.client import BaseAetherClient
from scitrera_aether_client.client_async import BaseAsyncAetherClient
from scitrera_aether_client.proto import aether_pb2


def _request(correlation_id: str = "call-1") -> aether_pb2.ResourceAccessRequest:
    return aether_pb2.ResourceAccessRequest(
        resource_type="tool-catalog/entry",
        resource_id="provider-1/tool-1",
        operation="invoke",
        workspace="workspace-1",
        required_access_level=20,
        correlation_id=correlation_id,
    )


def test_sync_check_access_correlates_and_returns_denial_receipt():
    client = BaseAetherClient(auto_reconnect=False)
    response = aether_pb2.AccessCheckResponse(
        success=True,
        decision=aether_pb2.AccessDecisionReceipt(
            allowed=False, denial_code="access_denied", request=_request()
        ),
    )
    client._send_sync_op = MagicMock(return_value=response)

    receipt = client.check_access(_request())

    assert receipt.allowed is False
    assert receipt.denial_code == "access_denied"
    upstream, request_id, timeout = client._send_sync_op.call_args.args
    assert upstream.access_check.request_id == request_id
    assert upstream.access_check.access.correlation_id == "call-1"
    assert timeout == 10.0


def test_sync_checked_send_wires_authority_and_access_request():
    client = BaseAetherClient(auto_reconnect=False)
    authorization = aether_pb2.AuthorizationContext(
        authority_mode="on_behalf_of",
        subject=aether_pb2.PrincipalRef(principal_type="user", principal_id="user-1"),
        grant_id="grant-1",
    )

    client.send_checked_message(
        "sv::tools", b"payload", _request(),
        authorization=authorization, forward_authorization=True,
    )

    upstream = client.request_queue.get_nowait()
    assert upstream.send.checked_access.resource_id == "provider-1/tool-1"
    assert upstream.send.authorization.grant_id == "grant-1"
    assert upstream.send.forward_authorization is True


@pytest.mark.asyncio
async def test_async_batch_check_access_returns_ordered_receipts():
    client = BaseAsyncAetherClient(auto_reconnect=False)
    response = aether_pb2.BatchAccessCheckResponse(
        success=True,
        decisions=[
            aether_pb2.AccessDecisionReceipt(allowed=True, request=_request("one")),
            aether_pb2.AccessDecisionReceipt(allowed=False, request=_request("two")),
        ],
    )
    client._send_sync_op = AsyncMock(return_value=response)

    receipts = await client.batch_check_access([_request("one"), _request("two")])

    assert [item.request.correlation_id for item in receipts] == ["one", "two"]
    upstream, request_id, timeout = client._send_sync_op.call_args.args
    assert upstream.batch_access_check.request_id == request_id
    assert len(upstream.batch_access_check.access) == 2
    assert timeout == 10.0
