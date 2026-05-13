"""requests adapter that routes HTTP through an Aether proxy connection.

URL scheme
----------
Mount this adapter on a :class:`requests.Session` under ``aether+sv://``:

    adapter = AetherRequestsAdapter(client)
    session = requests.Session()
    session.mount("aether+sv://", adapter)

**Specific instance** (impl + specifier):

    session.get("aether+sv://my-impl--my-specifier/path/to/resource")
    # → target_topic = "my-impl::my-specifier"

**Wildcard / load-balanced** (impl only):

    session.get("aether+sv://my-impl/path/to/resource")
    # → target_topic = "my-impl"

The ``--`` delimiter in the netloc separates *impl* from *specifier*, because
standard URL parsing interprets ``:`` in the host portion as a port separator.
If either part contains a literal ``--``, URL-encode it as ``%2D%2D`` before
constructing the URL.
"""
from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlparse

try:
    import requests
    import requests.adapters
    import requests.exceptions
    import requests.structures
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "requests is required for AetherRequestsAdapter. Install with `pip install requests`."
    ) from e

from .proxy import ProxyError, proxy_http


def _parse_target_topic(url: str) -> tuple[str, str]:
    """Return (target_topic, path_with_query) from an aether+sv:// URL.

    The netloc ``{impl}--{specifier}`` maps to topic ``{impl}::{specifier}``.
    A bare ``{impl}`` (no ``--``) maps to topic ``{impl}`` (wildcard/load-balanced).
    """
    parsed = urlparse(url)
    netloc = parsed.netloc
    if "--" in netloc:
        impl, specifier = netloc.split("--", 1)
        target_topic = f"{impl}::{specifier}"
    else:
        target_topic = netloc

    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    if parsed.fragment:
        path = f"{path}#{parsed.fragment}"

    return target_topic, path


class AetherRequestsAdapter(requests.adapters.BaseAdapter):
    """A :class:`requests.adapters.BaseAdapter` that routes requests over Aether.

    Parameters
    ----------
    client:
        A connected Aether sync client (``BaseAetherClient`` subclass).
    timeout:
        Default request timeout in seconds. Can be overridden per-request via
        the ``timeout`` parameter on ``session.get()`` etc.
    follow_redirects:
        Whether the sidecar should follow HTTP redirects.
    app_workspace:
        Optional workspace override forwarded in the proxy envelope.
    authorization:
        Pre-built ``AuthorizationContext`` proto for OBO grants.
    authority_mode, subject_type, subject_id, grant_id:
        Shorthand OBO fields; ignored if ``authorization`` is provided.
    backend:
        Optional default terminator backend name applied to every request.
        The backend's allow-list still applies — explicit naming selects
        which backend's ACL is consulted, not whether the request is allowed.
    """

    def __init__(
        self,
        client: Any,
        *,
        timeout: float = 30.0,
        follow_redirects: bool = False,
        app_workspace: Optional[str] = None,
        authorization: Any = None,
        authority_mode: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        grant_id: Optional[str] = None,
        backend: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._timeout = timeout
        self._follow_redirects = follow_redirects
        self._app_workspace = app_workspace
        self._authorization = authorization
        self._authority_mode = authority_mode
        self._subject_type = subject_type
        self._subject_id = subject_id
        self._grant_id = grant_id
        self._backend = backend

    def send(
        self,
        request: "requests.PreparedRequest",
        stream: bool = False,
        timeout: Any = None,
        verify: Any = True,
        cert: Any = None,
        proxies: Any = None,
    ) -> "requests.Response":
        effective_timeout = self._timeout
        if timeout is not None:
            if isinstance(timeout, tuple):
                # requests allows (connect_timeout, read_timeout); use the larger
                effective_timeout = max(t for t in timeout if t is not None)
            else:
                effective_timeout = float(timeout)

        target_topic, path = _parse_target_topic(request.url or "")

        body = request.body or b""
        if isinstance(body, str):
            body = body.encode("utf-8")

        headers = dict(request.headers or {})

        try:
            proxy_resp = proxy_http(
                self._client,
                target_topic,
                request.method or "GET",
                path,
                headers=headers,
                body=body,
                timeout=effective_timeout,
                follow_redirects=self._follow_redirects,
                app_workspace=self._app_workspace,
                authorization=self._authorization,
                authority_mode=self._authority_mode,
                subject_type=self._subject_type,
                subject_id=self._subject_id,
                grant_id=self._grant_id,
                backend=self._backend,
            )
        except ProxyError as e:
            raise requests.exceptions.ConnectionError(str(e)) from e

        response = requests.Response()
        response.status_code = proxy_resp.status_code
        response.headers = requests.structures.CaseInsensitiveDict(
            dict(proxy_resp.headers)
        )
        response._content = proxy_resp.body  # type: ignore[attr-defined]
        response.encoding = response.apparent_encoding
        response.url = request.url or ""
        response.request = request
        return response

    def close(self) -> None:
        return None


__all__ = ["AetherRequestsAdapter"]
