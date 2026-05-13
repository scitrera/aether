"""Metric builder for typed metric publishing.

All entries are interpreted as additive deltas; negative qty values require
the ``capability/metric_credit`` ACL permission on the sender.
"""
from __future__ import annotations
from typing import Optional
from .proto import aether_pb2


class MetricBuilder:
    """Small fluent helper for constructing aether_pb2.Metric."""

    def __init__(self) -> None:
        self._m = aether_pb2.Metric()

    def trace(self, trace_id: str) -> "MetricBuilder":
        self._m.trace_id = trace_id
        return self

    def add(self, name: str, kind: str = "", qty: float = 0.0) -> "MetricBuilder":
        entry = self._m.entries.add()
        entry.name = name
        entry.kind = kind
        entry.qty = qty
        return self

    def tag(self, key: str, value: str) -> "MetricBuilder":
        if value is not None:
            self._m.metadata[key] = value
        return self

    def client_timestamp_ms(self, ts: int) -> "MetricBuilder":
        self._m.client_timestamp_ms = ts
        return self

    def build(self) -> aether_pb2.Metric:
        return self._m


def new_metric() -> MetricBuilder:
    """Return a fresh MetricBuilder."""
    return MetricBuilder()
