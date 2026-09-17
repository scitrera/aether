"""Real gRPC image-sized response, plus the old receive limit as a control."""
import grpc
import pytest
from scitrera_aether_client.client_async import AsyncServiceClient
from scitrera_aether_client.grpc_limits import GRPC_CHANNEL_OPTIONS
from scitrera_aether_client.proto import aether_pb2 as pb, aether_pb2_grpc as rpc

class Echo(rpc.AetherGatewayServicer):
    async def Connect(self, requests, context):
        async for request in requests:
            yield pb.DownstreamMessage(msg=pb.IncomingMessage(payload=request.send.payload))

@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['async_sdk', 'sync_sdk', 'legacy_control'])
async def test_original_image_exceeds_legacy_receive_limit(kind):
    server = grpc.aio.server(options=[('grpc.max_receive_message_length', 9 * 1024 * 1024)])
    rpc.add_AetherGatewayServicer_to_server(Echo(), server)
    port = server.add_insecure_port('127.0.0.1:0')
    await server.start()
    client = AsyncServiceClient(implementation='test', specifier='image')
    options = client._channel_options if kind == 'async_sdk' else GRPC_CHANNEL_OPTIONS
    if kind == 'legacy_control':
        options = [('grpc.max_receive_message_length', 4 * 1024 * 1024)]
    payload = b'x' * (7 * 1024 * 1024)
    try:
        async with grpc.aio.insecure_channel(f'127.0.0.1:{port}', options=options) as channel:
            stream = rpc.AetherGatewayStub(channel).Connect(timeout=10)
            await stream.write(pb.UpstreamMessage(send=pb.SendMessage(payload=payload)))
            if kind == 'legacy_control':
                with pytest.raises(grpc.aio.AioRpcError) as failure:
                    await stream.read()
                # grpcio may report the oversized bidirectional receive as a
                # cancelled stream; in either case no image was delivered.
                assert failure.value.code() in (grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.CANCELLED)
            else:
                assert (await stream.read()).msg.payload == payload
            if kind != 'legacy_control':
                await stream.done_writing()
    finally:
        await server.stop(0)
