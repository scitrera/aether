"""AetherRemoteAgent — implements ag2's Agent protocol, proxies via Aether."""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from typing import Any, Literal

from autogen.agentchat.agent import Agent
from autogen.agentchat.remote.protocol import RequestMessage

from ..identity import AetherIdentity
from ..wire import RequestEnvelope
from .errors import HITLRequired, RemoteAgentError
from .streams import AetherTransport
from .tool_loop import (
    detect_client_tool_calls,
    execute_client_tools,
    get_client_tool_names,
    get_client_tools,
)

logger = logging.getLogger(__name__)

HITLMode = Literal["raise", "sender", "auto_skip"]


class AetherRemoteAgent:
    """A local stub Agent that proxies receive() to a remote ConversableAgent over Aether."""

    def __init__(
        self,
        name: str,
        remote_identity: AetherIdentity,
        transport: AetherTransport,
        description: str | None = None,
        *,
        hitl_mode: HITLMode = "raise",
        iostream_streaming: bool = True,
        max_continuations: int = 8,
    ) -> None:
        self._name = name
        self._description = description or name
        self.remote_identity = remote_identity
        self.transport = transport
        self.chat_messages: dict[Agent, list[dict[str, Any]]] = {}
        self._hitl_mode: HITLMode = hitl_mode
        self._iostream_streaming = iostream_streaming
        self._max_continuations = max(1, max_continuations)

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    def _history(self, peer: Agent) -> list[dict[str, Any]]:
        return self.chat_messages.setdefault(peer, [])

    def send(
        self,
        message: dict[str, Any] | str,
        recipient: Agent,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        msg = self._normalize(message, role="assistant")
        self._history(recipient).append(msg)

    async def a_send(
        self,
        message: dict[str, Any] | str,
        recipient: Agent,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        self.send(message, recipient, request_reply, silent)

    def receive(
        self,
        message: dict[str, Any] | str,
        sender: Agent,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is not None:
            raise RuntimeError(
                "AetherRemoteAgent.receive() must not be called from inside the transport's "
                "event loop; use a_receive() or call from a sync caller"
            )
        asyncio.run(self.a_receive(message, sender, request_reply, silent))

    async def a_receive(
        self,
        message: dict[str, Any] | str,
        sender: Agent,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        sender_name = getattr(sender, "name", "user")
        msg = self._normalize(message, role="user", name=sender_name)
        sender_history = self._history(sender)
        sender_history.append(msg)

        client_tools = get_client_tools(sender)
        known_names = get_client_tool_names(client_tools)
        last_assistant: dict[str, Any] | None = None
        completed_naturally = False

        for _ in range(self._max_continuations):
            request_messages = [
                dict(m, name=sender_name) if m.get("role") == "user" and "name" not in m else m
                for m in sender_history
            ]
            envelope = RequestEnvelope(
                correlation_id=str(uuid.uuid4()),
                reply_to=self.transport.local_identity.to_topic(),
                request=RequestMessage(
                    messages=request_messages,
                    context=None,
                    client_tools=client_tools,
                ),
            )
            hitl_injected: str | None = None
            last_assistant_this_pass: dict[str, Any] | None = None
            async for env in self.transport.submit_request(self.remote_identity, envelope):
                if env.error:
                    raise RemoteAgentError(env.error.get("message", "remote agent error"))
                resp = env.response
                if resp is not None:
                    if resp.streaming_text and self._iostream_streaming:
                        self._forward_streaming(resp.streaming_text)
                    if resp.input_required is not None:
                        injected = await self._handle_hitl(
                            resp.input_required, env.correlation_id, sender
                        )
                        hitl_injected = injected
                    elif resp.message is not None:
                        out_msg = resp.message
                        sender_history.append(out_msg)
                        last_assistant_this_pass = out_msg
                if env.done:
                    break

            if hitl_injected is not None:
                sender_history.append({"role": "user", "content": hitl_injected, "name": sender_name})
                continue

            if last_assistant_this_pass is not None:
                last_assistant = last_assistant_this_pass
                calls = detect_client_tool_calls(last_assistant_this_pass, known_names)
                if calls:
                    tool_resp_msg = await execute_client_tools(
                        sender, last_assistant_this_pass, calls
                    )
                    if tool_resp_msg is not None:
                        sender_history.append(tool_resp_msg)
                        continue
            completed_naturally = True
            break

        if not completed_naturally:
            raise RemoteAgentError(
                f"continuation loop exceeded max_continuations={self._max_continuations}"
            )

        if request_reply is False:
            return
        if last_assistant is not None:
            await sender.a_send(last_assistant, self, request_reply=False)

    def generate_reply(
        self,
        messages: list[dict[str, Any]] | None = None,
        sender: Agent | None = None,
    ) -> str | dict[str, Any] | None:
        if sender is None:
            return None
        hist = self.chat_messages.get(sender, [])
        for m in reversed(hist):
            if m.get("role") == "assistant":
                return m
        return None

    async def a_generate_reply(
        self,
        messages: list[dict[str, Any]] | None = None,
        sender: Agent | None = None,
    ) -> str | dict[str, Any] | None:
        return self.generate_reply(messages, sender)

    def set_ui_tools(self, tools: list[Any]) -> None:
        pass

    def unset_ui_tools(self, tools: list[Any]) -> None:
        pass

    def _forward_streaming(self, chunk: str) -> None:
        try:
            from autogen.events.client_events import StreamEvent
            from autogen.io.base import IOStream

            stream = IOStream.get_default()
            stream.send(StreamEvent(content=chunk))
        except Exception:  # noqa: BLE001
            logger.debug("iostream streaming forward failed", exc_info=True)

    async def _handle_hitl(self, prompt: str, correlation_id: str, sender: Agent) -> str | None:
        if self._hitl_mode == "raise":
            raise HITLRequired(prompt, correlation_id=correlation_id)
        if self._hitl_mode == "auto_skip":
            logger.warning("HITL prompt auto-skipped: %s", prompt)
            return ""
        # "sender" mode
        a_fn = getattr(sender, "a_get_human_input", None)
        s_fn = getattr(sender, "get_human_input", None)
        if a_fn is not None and inspect.iscoroutinefunction(a_fn):
            return await a_fn(prompt)
        if s_fn is not None:
            res = s_fn(prompt)
            if inspect.isawaitable(res):
                res = await res
            return res if isinstance(res, str) else ""
        raise HITLRequired(prompt, correlation_id=correlation_id)

    @staticmethod
    def _normalize(message: dict[str, Any] | str, role: str, name: str | None = None) -> dict[str, Any]:
        if isinstance(message, str):
            out: dict[str, Any] = {"content": message, "role": role}
        else:
            out = dict(message)
            out.setdefault("role", role)
        if name and "name" not in out:
            out["name"] = name
        return out
