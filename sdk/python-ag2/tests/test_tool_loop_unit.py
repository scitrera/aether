"""Mock-only unit tests for tool_loop helpers and proxy continuation/HITL behavior."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from autogen.agentchat.remote.protocol import ServiceResponse

from scitrera_aether_ag2 import (
    AetherIdentity,
    AetherRemoteAgent,
    HITLRequired,
    RemoteAgentError,
)
from scitrera_aether_ag2.remote.tool_loop import (
    detect_client_tool_calls,
    execute_client_tools,
    get_client_tool_names,
)
from scitrera_aether_ag2.wire import RequestEnvelope, ResponseEnvelope


# ---------- fixtures / fakes ----------


def _tool_dict(name: str) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {"name": name, "description": name, "parameters": {"type": "object"}},
    }


def _tool_call(call_id: str, name: str, arguments: str = "{}") -> dict[str, Any]:
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


class FakeTransport:
    """Records submitted envelopes and yields scripted ResponseEnvelopes."""

    def __init__(self, scripts: list[list[ResponseEnvelope]]) -> None:
        self.local_identity = AetherIdentity("default", "caller", "alice")
        self._scripts = scripts
        self.submitted: list[RequestEnvelope] = []

    async def submit_request(
        self, target: AetherIdentity, envelope: RequestEnvelope
    ) -> AsyncIterator[ResponseEnvelope]:
        self.submitted.append(envelope)
        idx = len(self.submitted) - 1
        if idx >= len(self._scripts):
            raise AssertionError(f"unexpected request #{idx + 1}")
        # reassign correlation_id so it matches what proxy used
        for env in self._scripts[idx]:
            env.correlation_id = envelope.correlation_id
            yield env


class FakeSender:
    """Mock ag2 Agent with optional tools + scripted tool-call replies + HITL hook."""

    def __init__(
        self,
        name: str = "alice",
        tools: list[dict[str, Any]] | None = None,
        tool_replies: dict[str, str] | None = None,
        human_replies: list[str] | None = None,
    ) -> None:
        self.name = name
        self.description = name
        self.client_tools = tools or []
        self._tool_replies = tool_replies or {}
        self._human_replies = list(human_replies or [])
        self.sent: list[dict[str, Any]] = []
        self.human_prompts: list[str] = []

    async def a_send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        self.sent.append(message if isinstance(message, dict) else {"content": message})

    def send(
        self,
        message: dict[str, Any] | str,
        recipient: Any,
        request_reply: bool | None = None,
        silent: bool | None = False,
    ) -> None:
        self.sent.append(message if isinstance(message, dict) else {"content": message})

    async def a_generate_tool_calls_reply(
        self, messages: list[dict[str, Any]]
    ) -> tuple[bool, dict[str, Any] | None]:
        msg = messages[-1]
        tool_responses = []
        for tc in msg.get("tool_calls", []):
            fn = tc.get("function", {})
            name = fn.get("name", "")
            content = self._tool_replies.get(name, f"ok:{name}")
            tool_responses.append(
                {"tool_call_id": tc.get("id", ""), "role": "tool", "content": content}
            )
        if not tool_responses:
            return False, None
        return True, {
            "role": "tool",
            "tool_responses": tool_responses,
            "content": "\n\n".join(t["content"] for t in tool_responses),
        }

    async def a_get_human_input(self, prompt: str) -> str:
        self.human_prompts.append(prompt)
        return self._human_replies.pop(0) if self._human_replies else ""


def _resp_env(seq: int, *, done: bool, response: ServiceResponse | None) -> ResponseEnvelope:
    return ResponseEnvelope(
        correlation_id="placeholder",
        sequence=seq,
        done=done,
        response=response,
    )


def _make_proxy(transport: FakeTransport, **kw: Any) -> AetherRemoteAgent:
    return AetherRemoteAgent(
        name="echo-bob",
        remote_identity=AetherIdentity("default", "echo", "bob"),
        transport=transport,  # type: ignore[arg-type]
        **kw,
    )


# ---------- unit tests: tool_loop helpers ----------


def test_get_client_tool_names_returns_set() -> None:
    tools = [_tool_dict("add"), _tool_dict("sub"), {"function": {}}]
    assert get_client_tool_names(tools) == {"add", "sub"}


def test_get_client_tools_legacy_functions_fallback() -> None:
    """A sender exposing only llm_config['functions'] yields wrapped tool dicts."""
    from scitrera_aether_ag2.remote.tool_loop import get_client_tools

    class LegacySender:
        llm_config = {
            "functions": [
                {"name": "add", "description": "Add two ints", "parameters": {"type": "object"}},
                {"name": "sub", "parameters": {"type": "object"}},
                {"description": "no name should be skipped"},  # filtered
                "not-a-dict",  # filtered
            ]
        }

    tools = get_client_tools(LegacySender())
    assert [t["function"]["name"] for t in tools] == ["add", "sub"]
    assert all(t["type"] == "function" for t in tools)
    # the original llm_config dict must not be mutated
    assert LegacySender.llm_config["functions"][0] == {
        "name": "add",
        "description": "Add two ints",
        "parameters": {"type": "object"},
    }


def test_get_client_tools_modern_tools_takes_precedence_over_legacy_functions() -> None:
    """When both llm_config['tools'] and llm_config['functions'] are set, modern wins."""
    from scitrera_aether_ag2.remote.tool_loop import get_client_tools

    class HybridSender:
        llm_config = {
            "tools": [_tool_dict("modern")],
            "functions": [{"name": "legacy", "parameters": {}}],
        }

    tools = get_client_tools(HybridSender())
    assert len(tools) == 1
    assert tools[0]["function"]["name"] == "modern"


def test_detect_client_tool_calls_filters_correctly() -> None:
    msg = {
        "role": "assistant",
        "tool_calls": [
            _tool_call("1", "client_add"),
            _tool_call("2", "server_only"),
            _tool_call("3", "client_sub"),
        ],
    }
    known = {"client_add", "client_sub"}
    out = detect_client_tool_calls(msg, known)
    assert [tc["id"] for tc in out] == ["1", "3"]


def test_detect_client_tool_calls_empty_known() -> None:
    msg = {"tool_calls": [_tool_call("1", "x")]}
    assert detect_client_tool_calls(msg, set()) == []


@pytest.mark.asyncio
async def test_execute_client_tools_returns_tool_role_message() -> None:
    sender = FakeSender(tool_replies={"add": "3"})
    calls = [_tool_call("c1", "add")]
    msg = {"role": "assistant", "tool_calls": calls}
    out = await execute_client_tools(sender, msg, calls)
    assert out is not None
    assert out["role"] == "tool"
    assert out["tool_responses"][0]["content"] == "3"


@pytest.mark.asyncio
async def test_execute_client_tools_no_calls_returns_none() -> None:
    sender = FakeSender()
    out = await execute_client_tools(sender, {"tool_calls": []}, [])
    assert out is None


@pytest.mark.asyncio
async def test_execute_client_tools_exception_synthesizes_error_message() -> None:
    """When the tool executor raises, we synthesize a role=tool error keyed to the call_ids."""

    class ExplodingSender(FakeSender):
        async def a_generate_tool_calls_reply(
            self, messages: list[dict[str, Any]]
        ) -> tuple[bool, dict[str, Any] | None]:
            raise RuntimeError("boom")

    sender = ExplodingSender()
    calls = [_tool_call("c1", "add"), _tool_call("c2", "add")]
    msg = {"role": "assistant", "tool_calls": calls}
    out = await execute_client_tools(sender, msg, calls)
    assert out is not None
    assert out["role"] == "tool"
    ids = [tr["tool_call_id"] for tr in out["tool_responses"]]
    assert ids == ["c1", "c2"]
    assert all("error:" in tr["content"] for tr in out["tool_responses"])
    assert "boom" in out["tool_responses"][0]["content"]


@pytest.mark.asyncio
async def test_execute_client_tools_no_executor_synthesizes_error_message() -> None:
    """No tool executor configured on sender → synthesize a tool-error rather than dropping silently."""

    class BareSender:
        name = "bare"

    calls = [_tool_call("only", "add")]
    msg = {"role": "assistant", "tool_calls": calls}
    out = await execute_client_tools(BareSender(), msg, calls)
    assert out is not None
    assert out["role"] == "tool"
    assert out["tool_responses"][0]["tool_call_id"] == "only"
    assert "error:" in out["tool_responses"][0]["content"]


# ---------- proxy tests ----------


@pytest.mark.asyncio
async def test_smoke_path_single_request_no_tools() -> None:
    """Empty client_tools + no streaming + no HITL → exactly one request, slice-equivalent."""
    transport = FakeTransport(
        [
            [
                _resp_env(
                    0,
                    done=False,
                    response=ServiceResponse(message={"role": "assistant", "content": "echo: hi"}),
                ),
                _resp_env(1, done=True, response=None),
            ]
        ]
    )
    proxy = _make_proxy(transport)
    sender = FakeSender()
    await proxy.a_receive({"content": "hi", "role": "user"}, sender)
    assert len(transport.submitted) == 1
    assert sender.sent and sender.sent[-1].get("content") == "echo: hi"


@pytest.mark.asyncio
async def test_hitl_mode_raise_bubbles_out() -> None:
    transport = FakeTransport(
        [
            [
                _resp_env(
                    0,
                    done=False,
                    response=ServiceResponse(input_required="please confirm"),
                ),
                _resp_env(1, done=True, response=None),
            ]
        ]
    )
    proxy = _make_proxy(transport, hitl_mode="raise")
    sender = FakeSender()
    with pytest.raises(HITLRequired) as ei:
        await proxy.a_receive("hi", sender)
    assert ei.value.prompt == "please confirm"
    assert ei.value.correlation_id


@pytest.mark.asyncio
async def test_hitl_mode_sender_injects_reply_and_continues() -> None:
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(input_required="name?")),
                _resp_env(1, done=True, response=None),
            ],
            [
                _resp_env(
                    0,
                    done=False,
                    response=ServiceResponse(message={"role": "assistant", "content": "hello drew"}),
                ),
                _resp_env(1, done=True, response=None),
            ],
        ]
    )
    proxy = _make_proxy(transport, hitl_mode="sender")
    sender = FakeSender(human_replies=["drew"])
    await proxy.a_receive("hi", sender)
    assert sender.human_prompts == ["name?"]
    assert len(transport.submitted) == 2
    # second request should include the injected user message
    second_msgs = transport.submitted[1].request.messages
    assert any(m.get("content") == "drew" for m in second_msgs)
    assert sender.sent and sender.sent[-1]["content"] == "hello drew"


@pytest.mark.asyncio
async def test_client_tool_call_triggers_continuation() -> None:
    tool_call_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [_tool_call("c1", "add", '{"a":1,"b":2}')],
    }
    final_msg = {"role": "assistant", "content": "result is 3"}
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(message=tool_call_msg)),
                _resp_env(1, done=True, response=None),
            ],
            [
                _resp_env(0, done=False, response=ServiceResponse(message=final_msg)),
                _resp_env(1, done=True, response=None),
            ],
        ]
    )
    proxy = _make_proxy(transport)
    sender = FakeSender(tools=[_tool_dict("add")], tool_replies={"add": "3"})
    await proxy.a_receive("compute 1+2", sender)
    assert len(transport.submitted) == 2
    # continuation request should contain the tool_response
    second_msgs = transport.submitted[1].request.messages
    assert any(m.get("role") == "tool" for m in second_msgs)
    assert sender.sent and sender.sent[-1]["content"] == "result is 3"


@pytest.mark.asyncio
async def test_max_continuations_cap_raises() -> None:
    # produce an unbounded chain of tool-call responses
    looping_msg = {
        "role": "assistant",
        "tool_calls": [_tool_call("loop", "spin")],
    }
    scripts = [
        [
            _resp_env(0, done=False, response=ServiceResponse(message=looping_msg)),
            _resp_env(1, done=True, response=None),
        ]
        for _ in range(3)
    ]
    transport = FakeTransport(scripts)
    proxy = _make_proxy(transport, max_continuations=3)
    sender = FakeSender(tools=[_tool_dict("spin")], tool_replies={"spin": "again"})
    with pytest.raises(RemoteAgentError):
        await proxy.a_receive("go", sender)
    assert len(transport.submitted) == 3


@pytest.mark.asyncio
async def test_max_continuations_return_last_delivers_partial() -> None:
    """on_max_continuations='return_last' delivers the last assistant turn instead of raising."""
    looping_msg = {
        "role": "assistant",
        "content": "partial answer",
        "tool_calls": [_tool_call("loop", "spin")],
    }
    scripts = [
        [
            _resp_env(0, done=False, response=ServiceResponse(message=looping_msg)),
            _resp_env(1, done=True, response=None),
        ]
        for _ in range(3)
    ]
    transport = FakeTransport(scripts)
    proxy = _make_proxy(
        transport, max_continuations=3, on_max_continuations="return_last"
    )
    sender = FakeSender(tools=[_tool_dict("spin")], tool_replies={"spin": "again"})
    await proxy.a_receive("go", sender)  # must not raise
    assert len(transport.submitted) == 3
    assert sender.sent, "sender should have received the last_assistant delivery"
    assert sender.sent[-1].get("content") == "partial answer"


@pytest.mark.asyncio
async def test_max_continuations_return_last_no_assistant_still_raises() -> None:
    """return_last falls back to raising when there is no last_assistant to deliver."""
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(input_required="who?")),
                _resp_env(1, done=True, response=None),
            ]
            for _ in range(3)
        ]
    )
    proxy = _make_proxy(
        transport,
        max_continuations=3,
        on_max_continuations="return_last",
        hitl_mode="auto_skip",
    )
    sender = FakeSender()
    with pytest.raises(RemoteAgentError):
        await proxy.a_receive("hi", sender)


@pytest.mark.asyncio
async def test_hitl_sender_mode_mirrors_into_sender_chat_messages() -> None:
    """HITL sender mode appends the synthetic user reply to sender.chat_messages[proxy]."""

    class SenderWithChatMessages(FakeSender):
        def __init__(self, **kw: Any) -> None:
            super().__init__(**kw)
            self.chat_messages: dict[Any, list[dict[str, Any]]] = {}

    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(input_required="name?")),
                _resp_env(1, done=True, response=None),
            ],
            [
                _resp_env(
                    0,
                    done=False,
                    response=ServiceResponse(message={"role": "assistant", "content": "hi drew"}),
                ),
                _resp_env(1, done=True, response=None),
            ],
        ]
    )
    proxy = _make_proxy(transport, hitl_mode="sender")
    sender = SenderWithChatMessages(human_replies=["drew"])
    await proxy.a_receive("hi", sender)
    assert proxy in sender.chat_messages, (
        f"sender.chat_messages missing proxy key: {list(sender.chat_messages)}"
    )
    mirrored = sender.chat_messages[proxy]
    assert any(m.get("content") == "drew" for m in mirrored), (
        f"sender.chat_messages did not include the human reply: {mirrored}"
    )


@pytest.mark.asyncio
async def test_streaming_chunks_do_not_break_flow() -> None:
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(streaming_text="he")),
                _resp_env(1, done=False, response=ServiceResponse(streaming_text="llo")),
                _resp_env(
                    2,
                    done=False,
                    response=ServiceResponse(message={"role": "assistant", "content": "hello"}),
                ),
                _resp_env(3, done=True, response=None),
            ]
        ]
    )
    # iostream_streaming=True but no IOStream configured → must swallow without raising
    proxy = _make_proxy(transport, iostream_streaming=True)
    sender = FakeSender()
    await proxy.a_receive("hi", sender)
    assert sender.sent[-1]["content"] == "hello"


@pytest.mark.asyncio
async def test_streaming_buffer_fills_empty_content() -> None:
    """When the final message arrives with empty content, the buffered streaming text fills it."""
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(streaming_text="he")),
                _resp_env(1, done=False, response=ServiceResponse(streaming_text="llo")),
                _resp_env(
                    2,
                    done=False,
                    response=ServiceResponse(message={"role": "assistant", "content": None}),
                ),
                _resp_env(3, done=True, response=None),
            ]
        ]
    )
    proxy = _make_proxy(transport, iostream_streaming=False)
    sender = FakeSender()
    await proxy.a_receive("hi", sender)
    assert sender.sent, "sender should have received the synthesized assistant message"
    assert sender.sent[-1].get("content") == "hello", (
        f"streaming buffer not synthesized into content: {sender.sent[-1]}"
    )


@pytest.mark.asyncio
async def test_streaming_buffer_does_not_overwrite_existing_content() -> None:
    """When the final message arrives with explicit content, the buffer is ignored."""
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(streaming_text="chunk")),
                _resp_env(
                    1,
                    done=False,
                    response=ServiceResponse(message={"role": "assistant", "content": "explicit"}),
                ),
                _resp_env(2, done=True, response=None),
            ]
        ]
    )
    proxy = _make_proxy(transport, iostream_streaming=False)
    sender = FakeSender()
    await proxy.a_receive("hi", sender)
    assert sender.sent[-1].get("content") == "explicit"


@pytest.mark.asyncio
async def test_proxy_threads_one_conversation_id_across_continuations() -> None:
    """All envelopes for a single a_receive call share one conversation_id."""
    tool_call_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [_tool_call("c1", "add", '{"a":1,"b":2}')],
    }
    final_msg = {"role": "assistant", "content": "result is 3"}
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(message=tool_call_msg)),
                _resp_env(1, done=True, response=None),
            ],
            [
                _resp_env(0, done=False, response=ServiceResponse(message=final_msg)),
                _resp_env(1, done=True, response=None),
            ],
        ]
    )
    proxy = _make_proxy(transport)
    sender = FakeSender(tools=[_tool_dict("add")], tool_replies={"add": "3"})
    await proxy.a_receive("compute 1+2", sender)
    assert len(transport.submitted) == 2
    cids = [e.conversation_id for e in transport.submitted]
    assert cids[0] is not None
    assert cids[0] == cids[1], (
        f"continuation envelopes must share conversation_id; got {cids}"
    )
    # And correlation_ids must still be unique per envelope
    correlation_ids = [e.correlation_id for e in transport.submitted]
    assert len(set(correlation_ids)) == len(correlation_ids)


@pytest.mark.asyncio
async def test_proxy_assigns_distinct_conversation_id_per_receive_call() -> None:
    """Separate a_receive calls get separate conversation_ids."""
    def _script() -> list[ResponseEnvelope]:
        return [
            _resp_env(
                0,
                done=False,
                response=ServiceResponse(message={"role": "assistant", "content": "hi"}),
            ),
            _resp_env(1, done=True, response=None),
        ]

    transport = FakeTransport([_script(), _script()])
    proxy = _make_proxy(transport)
    sender = FakeSender()
    await proxy.a_receive("hi 1", sender)
    await proxy.a_receive("hi 2", sender)
    cids = [e.conversation_id for e in transport.submitted]
    assert len(cids) == 2 and cids[0] != cids[1]


@pytest.mark.asyncio
async def test_streaming_buffer_resets_between_continuation_passes() -> None:
    """The buffer is per-pass: a later pass with no streaming and explicit content is unaffected."""
    tool_call_msg = {
        "role": "assistant",
        "content": None,  # tool-call passes typically have no content
        "tool_calls": [_tool_call("c1", "add", '{"a":1,"b":2}')],
    }
    final_msg = {"role": "assistant", "content": "result is 3"}
    transport = FakeTransport(
        [
            [
                _resp_env(0, done=False, response=ServiceResponse(streaming_text="stale")),
                _resp_env(1, done=False, response=ServiceResponse(message=tool_call_msg)),
                _resp_env(2, done=True, response=None),
            ],
            [
                _resp_env(0, done=False, response=ServiceResponse(message=final_msg)),
                _resp_env(1, done=True, response=None),
            ],
        ]
    )
    proxy = _make_proxy(transport)
    sender = FakeSender(tools=[_tool_dict("add")], tool_replies={"add": "3"})
    await proxy.a_receive("compute 1+2", sender)
    assert sender.sent[-1].get("content") == "result is 3", (
        f"second-pass content must not include first-pass streaming buffer: {sender.sent[-1]}"
    )
