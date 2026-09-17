"""Bounded receive ceiling matching the gateway's outbound gRPC frame cap."""

GRPC_CHANNEL_OPTIONS = (("grpc.max_receive_message_length", 16 * 1024 * 1024),)
