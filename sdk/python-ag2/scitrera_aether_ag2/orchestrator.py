"""AetherAg2Orchestrator — lazy spin-up of ag2 agent host subprocesses."""

from __future__ import annotations

import json
import logging
from typing import Any

from scitrera_aether_client.orchestrator import MultiprocessOrchestrator

logger = logging.getLogger(__name__)


class AetherAg2Orchestrator(MultiprocessOrchestrator):
    """
    Orchestrator that spawns ag2 agent host subprocesses on demand.

    When Aether sends a TaskAssignment for an offline ag2 agent, this
    orchestrator spawns a Python subprocess that boots an AetherAgentHost
    for that agent using the factory specifier supplied in launch_params.

    The assignment's ``launch_params["factory"]`` must be a Python
    ``module:callable`` string (e.g. ``"mypackage.agents:make_agent"``).
    The callable receives a single ``dict`` argument (the assignment metadata)
    and must return a ``ConversableAgent``.

    Example::

        class MyOrchestrator(AetherAg2Orchestrator):
            def get_implementation(self):
                return "my-orchestrator"

        orch = MyOrchestrator(gateway="localhost:50051")
        orch.run()
    """

    SUPPORTED_PROFILE = "ag2-subprocess"

    def __init__(
        self,
        implementation: str,
        *,
        host_module: str = "scitrera_aether_ag2.host_entrypoint",
        **kw: Any,
    ) -> None:
        """
        Initialize the ag2 orchestrator.

        Args:
            implementation: Implementation identifier for this orchestrator.
            host_module: Python module to run as the agent host subprocess.
                Defaults to ``scitrera_aether_ag2.host_entrypoint``.
            **kw: Forwarded verbatim to :class:`MultiprocessOrchestrator`
                (e.g. ``gateway``, ``tls_enabled``, etc.).
        """
        self._implementation = implementation
        self._host_module = host_module
        super().__init__(**kw)

    # -------------------------------------------------------------------------
    # BaseOrchestrator contract
    # -------------------------------------------------------------------------

    def get_implementation(self) -> str:
        """Return the orchestrator implementation identifier."""
        return self._implementation

    def get_supported_profiles(self) -> list[str]:
        """Return the single profile this orchestrator handles."""
        return [self.SUPPORTED_PROFILE]

    def handle_assignment(self, assignment: Any) -> None:
        """
        Handle a task assignment by spawning an ag2 host subprocess.

        Extracts the agent factory specifier from
        ``assignment.launch_params["factory"]``, builds an env dict with all
        required context, and calls :meth:`spawn_module`.

        Args:
            assignment: TaskAssignment proto from the gateway.

        Raises:
            KeyError: If ``launch_params["factory"]`` is absent.
        """
        launch_params = dict(assignment.launch_params)
        factory = launch_params.get("factory")
        if not factory:
            raise KeyError(
                f"launch_params missing required key 'factory' "
                f"for task {assignment.task_id}"
            )

        config: dict = dict(assignment.metadata)

        env = {
            "AETHER_AG2_FACTORY": factory,
            "AETHER_AG2_CONFIG": json.dumps(config),
            "AETHER_AG2_WORKSPACE": assignment.workspace,
            "AETHER_AG2_IMPL": assignment.target_implementation,
            "AETHER_AG2_SPEC": assignment.specifier,
            "AETHER_GATEWAY_ENDPOINT": self.gateway,
        }

        logger.info(
            "[AetherAg2Orchestrator] spawning host for task=%s factory=%s",
            assignment.task_id,
            factory,
        )

        self.spawn_module(
            task_id=assignment.task_id,
            module_name=self._host_module,
            workspace=assignment.workspace,
            implementation=assignment.target_implementation,
            specifier=assignment.specifier,
            env=env,
        )

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def shutdown(self) -> None:
        """Terminate every tracked subprocess and close the orchestrator client.

        Idempotent: safe to call multiple times. Each subprocess is terminated
        independently — a failure on one does not stop the rest. The client is
        closed last so subprocess teardown happens while the connection is still
        usable for any cleanup signals the parent class may need.
        """
        if getattr(self, "_shutdown_called", False):
            return
        self._shutdown_called = True

        for proc in list(self.get_all_processes().values()):
            try:
                self.terminate_process(proc)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "[AetherAg2Orchestrator] terminate_process failed for %s",
                    getattr(proc, "specifier", proc),
                    exc_info=True,
                )

        try:
            self.close()
        except Exception:  # noqa: BLE001
            logger.warning("[AetherAg2Orchestrator] client.close() failed", exc_info=True)


__all__ = ["AetherAg2Orchestrator"]
