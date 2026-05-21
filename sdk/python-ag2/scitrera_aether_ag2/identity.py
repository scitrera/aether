"""Aether identity helper for ag2 agents (workspace / impl / spec ↔ ag2 name)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AetherIdentity:
    """Canonical Aether agent identity: ``ag::{workspace}::{implementation}::{specifier}``."""

    workspace: str
    implementation: str
    specifier: str

    def to_topic(self) -> str:
        return f"ag::{self.workspace}::{self.implementation}::{self.specifier}"

    @classmethod
    def from_topic(cls, topic: str) -> "AetherIdentity":
        parts = topic.split("::", 3)
        if len(parts) != 4 or parts[0] != "ag":
            raise ValueError(f"not an agent topic: {topic!r}")
        _, workspace, implementation, specifier = parts
        return cls(workspace=workspace, implementation=implementation, specifier=specifier)
