"""Unit tests for workspace identity resolver (mock-only, no aetherlite required)."""

from __future__ import annotations

import pytest

from scitrera_aether_ag2.features.workspace import (
    WorkspaceResolverConfig,
    resolve_identity,
    resolve_identity_from_env,
)
from scitrera_aether_ag2.identity import AetherIdentity


def test_resolve_identity_defaults():
    identity = resolve_identity("alice")
    assert identity == AetherIdentity(workspace="default", implementation="alice", specifier="default")


def test_resolve_identity_custom_workspace():
    config = WorkspaceResolverConfig(default_workspace="prod")
    identity = resolve_identity("alice", config=config)
    assert identity == AetherIdentity(workspace="prod", implementation="alice", specifier="default")


def test_resolve_identity_custom_specifier():
    config = WorkspaceResolverConfig(specifier="v2")
    identity = resolve_identity("bob", config=config)
    assert identity == AetherIdentity(workspace="default", implementation="bob", specifier="v2")


def test_resolve_identity_implementation_suffix():
    config = WorkspaceResolverConfig(implementation_suffix="-worker")
    identity = resolve_identity("processor", config=config)
    assert identity.implementation == "processor-worker"


def test_resolve_identity_from_env_workspace_and_spec():
    env = {"AETHER_AG2_WORKSPACE": "staging", "AETHER_AG2_SPEC": "n7"}
    identity = resolve_identity_from_env("alice", env=env)
    assert identity == AetherIdentity(workspace="staging", implementation="alice", specifier="n7")


def test_resolve_identity_from_env_all_overrides():
    env = {
        "AETHER_AG2_WORKSPACE": "staging",
        "AETHER_AG2_IMPL": "custom-impl",
        "AETHER_AG2_SPEC": "n7",
    }
    identity = resolve_identity_from_env("alice", env=env)
    assert identity == AetherIdentity(workspace="staging", implementation="custom-impl", specifier="n7")


def test_resolve_identity_from_env_defaults_when_empty():
    identity = resolve_identity_from_env("charlie", env={})
    assert identity == AetherIdentity(workspace="default", implementation="charlie", specifier="default")


def test_resolve_identity_from_env_partial_override():
    env = {"AETHER_AG2_WORKSPACE": "dev"}
    identity = resolve_identity_from_env("diana", env=env)
    assert identity.workspace == "dev"
    assert identity.implementation == "diana"
    assert identity.specifier == "default"


def test_resolve_identity_returns_aether_identity_type():
    identity = resolve_identity("agent1")
    assert isinstance(identity, AetherIdentity)


def test_resolve_identity_to_topic():
    identity = resolve_identity("myagent")
    assert identity.to_topic() == "ag::default::myagent::default"
