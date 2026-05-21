"""Unit tests for orchestrator.py and host_entrypoint.py helpers.

No real Aether gateway, no real subprocesses.
Run with: AETHER_RUN_E2E=0 .venv/bin/pytest tests/test_orchestrator_helpers.py -v
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from scitrera_aether_ag2.host_entrypoint import build_host_from_env
from scitrera_aether_ag2.identity import AetherIdentity
from scitrera_aether_ag2.orchestrator import AetherAg2Orchestrator
from scitrera_aether_ag2.remote.host import AetherAgentHost


# ---------------------------------------------------------------------------
# Dummy factory used by factory-resolution tests
# ---------------------------------------------------------------------------

class _FakeAgent:
    """Stand-in for ConversableAgent — no real autogen import needed in tests."""

    def __init__(self, config: dict) -> None:
        self.config = config


def dummy_factory(config: dict) -> _FakeAgent:
    """A minimal agent factory defined directly in this test module."""
    return _FakeAgent(config)


_FACTORY_PATH = f"{__name__}:dummy_factory"


# ---------------------------------------------------------------------------
# Helper: minimal env dict
# ---------------------------------------------------------------------------

def _minimal_env(
    factory: str = _FACTORY_PATH,
    config: dict | None = None,
    workspace: str = "ws1",
    impl: str = "my-impl",
    spec: str = "my-spec",
    endpoint: str = "localhost:50051",
) -> dict:
    return {
        "AETHER_AG2_FACTORY": factory,
        "AETHER_AG2_CONFIG": json.dumps(config or {"key": "val"}),
        "AETHER_AG2_WORKSPACE": workspace,
        "AETHER_AG2_IMPL": impl,
        "AETHER_AG2_SPEC": spec,
        "AETHER_GATEWAY_ENDPOINT": endpoint,
    }


# ---------------------------------------------------------------------------
# 1. Factory resolution
# ---------------------------------------------------------------------------

class TestBuildHostFromEnv:
    """Tests for host_entrypoint.build_host_from_env."""

    def test_returns_aether_agent_host(self):
        host = build_host_from_env(_minimal_env())
        assert isinstance(host, AetherAgentHost)

    def test_identity_fields_match_env(self):
        env = _minimal_env(workspace="prod", impl="echo", spec="echo-1")
        host = build_host_from_env(env)
        assert host.identity == AetherIdentity(
            workspace="prod",
            implementation="echo",
            specifier="echo-1",
        )

    def test_agent_constructed_by_factory(self):
        config = {"model": "gpt-4o", "temperature": 0}
        env = _minimal_env(config=config)
        host = build_host_from_env(env)
        assert isinstance(host.agent, _FakeAgent)
        assert host.agent.config == config

    def test_endpoint_propagated(self):
        env = _minimal_env(endpoint="mygateway:9999")
        host = build_host_from_env(env)
        assert host.endpoint == "mygateway:9999"

    def test_empty_config_defaults_to_empty_dict(self):
        env = _minimal_env()
        env["AETHER_AG2_CONFIG"] = "{}"
        host = build_host_from_env(env)
        assert host.agent.config == {}

    def test_default_workspace_when_missing(self):
        env = _minimal_env()
        del env["AETHER_AG2_WORKSPACE"]
        host = build_host_from_env(env)
        assert host.identity.workspace == "default"

    def test_default_impl_when_missing(self):
        env = _minimal_env()
        del env["AETHER_AG2_IMPL"]
        host = build_host_from_env(env)
        assert host.identity.implementation == "agent"

    def test_default_spec_when_missing(self):
        env = _minimal_env()
        del env["AETHER_AG2_SPEC"]
        host = build_host_from_env(env)
        assert host.identity.specifier == "default"

    def test_default_endpoint_when_missing(self):
        env = _minimal_env()
        del env["AETHER_GATEWAY_ENDPOINT"]
        host = build_host_from_env(env)
        assert host.endpoint == "localhost:50051"


# ---------------------------------------------------------------------------
# 2. Missing / malformed env vars
# ---------------------------------------------------------------------------

class TestBuildHostFromEnvErrors:
    """build_host_from_env raises SystemExit with clear messages on bad input."""

    def test_missing_factory_raises_system_exit(self):
        with pytest.raises(SystemExit) as exc_info:
            build_host_from_env({})
        assert "AETHER_AG2_FACTORY" in str(exc_info.value)

    def test_factory_without_colon_raises_system_exit(self):
        env = _minimal_env(factory="nodot_module_just_a_name")
        with pytest.raises(SystemExit) as exc_info:
            build_host_from_env(env)
        assert "module:callable" in str(exc_info.value)

    def test_nonexistent_module_raises_system_exit(self):
        env = _minimal_env(factory="does_not_exist_xyzzy:factory")
        with pytest.raises(SystemExit) as exc_info:
            build_host_from_env(env)
        assert "does_not_exist_xyzzy" in str(exc_info.value)

    def test_missing_callable_in_module_raises_system_exit(self):
        env = _minimal_env(factory=f"{__name__}:no_such_callable")
        with pytest.raises(SystemExit) as exc_info:
            build_host_from_env(env)
        assert "no_such_callable" in str(exc_info.value)

    def test_invalid_json_config_raises_system_exit(self):
        env = _minimal_env()
        env["AETHER_AG2_CONFIG"] = "not-json{"
        with pytest.raises(SystemExit) as exc_info:
            build_host_from_env(env)
        assert "JSON" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 3. AetherAg2Orchestrator.handle_assignment
# ---------------------------------------------------------------------------

def _make_assignment(
    task_id: str = "task-abc",
    workspace: str = "ws1",
    target_implementation: str = "my-impl",
    specifier: str = "my-spec",
    factory: str = _FACTORY_PATH,
    metadata: dict | None = None,
) -> SimpleNamespace:
    """Build a fake TaskAssignment-like object with plain Python types."""
    return SimpleNamespace(
        task_id=task_id,
        workspace=workspace,
        target_implementation=target_implementation,
        specifier=specifier,
        launch_params={"factory": factory},
        metadata=metadata or {"tier": "free"},
    )


class TestAetherAg2OrchestratorHandleAssignment:
    """handle_assignment calls spawn_module with the correct env dict."""

    def _make_orch(self) -> AetherAg2Orchestrator:
        """Build an orchestrator with the OrchestratorClient constructor patched out.

        OrchestratorClient is imported lazily inside BaseOrchestrator.__init__
        via ``from ..client import OrchestratorClient``, so the correct patch
        target is the class in its defining module.
        """
        with patch(
            "scitrera_aether_client.client.OrchestratorClient",
            return_value=MagicMock(),
        ):
            orch = AetherAg2Orchestrator(
                implementation="test-orch",
                gateway="localhost:50051",
            )
        return orch

    def test_spawn_module_called_once(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment())
        mock_spawn.assert_called_once()

    def test_spawn_module_receives_correct_task_id(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment(task_id="task-xyz"))
        _, kwargs = mock_spawn.call_args
        assert mock_spawn.call_args[1].get("task_id") == "task-xyz" or \
               mock_spawn.call_args[0][0] == "task-xyz"

    def test_spawn_module_env_contains_factory(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment(factory="some.module:make_agent"))
        env = mock_spawn.call_args.kwargs["env"]
        assert env["AETHER_AG2_FACTORY"] == "some.module:make_agent"

    def test_spawn_module_env_contains_workspace(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment(workspace="prod"))
        env = mock_spawn.call_args.kwargs["env"]
        assert env["AETHER_AG2_WORKSPACE"] == "prod"

    def test_spawn_module_env_contains_impl(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment(target_implementation="echo"))
        env = mock_spawn.call_args.kwargs["env"]
        assert env["AETHER_AG2_IMPL"] == "echo"

    def test_spawn_module_env_contains_spec(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment(specifier="echo-1"))
        env = mock_spawn.call_args.kwargs["env"]
        assert env["AETHER_AG2_SPEC"] == "echo-1"

    def test_spawn_module_env_contains_gateway_endpoint(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment())
        env = mock_spawn.call_args.kwargs["env"]
        assert env["AETHER_GATEWAY_ENDPOINT"] == "localhost:50051"

    def test_spawn_module_env_config_is_valid_json(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment(metadata={"tier": "pro", "x": 1}))
        env = mock_spawn.call_args.kwargs["env"]
        parsed = json.loads(env["AETHER_AG2_CONFIG"])
        assert parsed == {"tier": "pro", "x": 1}

    def test_spawn_module_env_has_all_required_keys(self):
        orch = self._make_orch()
        with patch.object(orch, "spawn_module", return_value=None) as mock_spawn:
            orch.handle_assignment(_make_assignment())
        env = mock_spawn.call_args.kwargs["env"]
        required_keys = {
            "AETHER_AG2_FACTORY",
            "AETHER_AG2_CONFIG",
            "AETHER_AG2_WORKSPACE",
            "AETHER_AG2_IMPL",
            "AETHER_AG2_SPEC",
            "AETHER_GATEWAY_ENDPOINT",
        }
        assert required_keys.issubset(env.keys())

    def test_missing_factory_raises_key_error(self):
        orch = self._make_orch()
        bad_assignment = _make_assignment()
        bad_assignment.launch_params = {}  # no factory key
        with pytest.raises(KeyError, match="factory"):
            orch.handle_assignment(bad_assignment)

    def test_supported_profile_constant(self):
        assert AetherAg2Orchestrator.SUPPORTED_PROFILE == "ag2-subprocess"

    def test_get_supported_profiles_returns_list(self):
        orch = self._make_orch()
        profiles = orch.get_supported_profiles()
        assert isinstance(profiles, list)
        assert "ag2-subprocess" in profiles

    def test_get_implementation_returns_constructor_arg(self):
        orch = self._make_orch()
        assert orch.get_implementation() == "test-orch"


# ---------------------------------------------------------------------------
# 4. AetherAg2Orchestrator.shutdown
# ---------------------------------------------------------------------------

class TestAetherAg2OrchestratorShutdown:
    """shutdown() terminates tracked processes, closes the client, and is idempotent."""

    def _make_orch(self) -> AetherAg2Orchestrator:
        with patch(
            "scitrera_aether_client.client.OrchestratorClient",
            return_value=MagicMock(),
        ):
            return AetherAg2Orchestrator(
                implementation="shutdown-orch",
                gateway="localhost:50051",
            )

    def test_shutdown_terminates_each_tracked_process_and_closes(self):
        orch = self._make_orch()
        proc_a = MagicMock(specifier="a")
        proc_b = MagicMock(specifier="b")
        with (
            patch.object(orch, "get_all_processes", return_value={"a": proc_a, "b": proc_b}),
            patch.object(orch, "terminate_process") as mock_term,
            patch.object(orch, "close") as mock_close,
        ):
            orch.shutdown()
        assert mock_term.call_count == 2
        mock_close.assert_called_once()

    def test_shutdown_is_idempotent(self):
        orch = self._make_orch()
        with (
            patch.object(orch, "get_all_processes", return_value={}),
            patch.object(orch, "close") as mock_close,
        ):
            orch.shutdown()
            orch.shutdown()
        mock_close.assert_called_once()

    def test_shutdown_continues_after_terminate_failure(self):
        orch = self._make_orch()
        proc_a = MagicMock(specifier="a")
        proc_b = MagicMock(specifier="b")
        with (
            patch.object(orch, "get_all_processes", return_value={"a": proc_a, "b": proc_b}),
            patch.object(orch, "terminate_process", side_effect=[RuntimeError("boom"), None]) as mock_term,
            patch.object(orch, "close") as mock_close,
        ):
            orch.shutdown()
        assert mock_term.call_count == 2  # second call still happens after first raises
        mock_close.assert_called_once()

    def test_shutdown_swallows_close_failure(self):
        orch = self._make_orch()
        with (
            patch.object(orch, "get_all_processes", return_value={}),
            patch.object(orch, "close", side_effect=RuntimeError("client broken")),
        ):
            orch.shutdown()  # must not raise
