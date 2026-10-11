"""Minimal JSON-over-HTTP transport shared by every client facade.

Round-robins the configured Node endpoints, attaches the bearer key,
and maps (status, code) pairs to typed exceptions. Wire serialization
lives in ``_wire``; error classification lives in ``errors.map_error``.
"""

from __future__ import annotations

import itertools
from typing import Any, List, Optional

import httpx

from .errors import Unavailable, map_error


class Transport:
    def __init__(self, endpoints: List[str], api_key: str, timeout: float = 60.0):
        if not endpoints:
            raise ValueError("at least one endpoint is required")
        self._endpoints = [e.rstrip("/") for e in endpoints]
        self._api_key = api_key
        self._cycle = itertools.cycle(range(len(self._endpoints)))
        self._http = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def _endpoint(self) -> str:
        return self._endpoints[next(self._cycle)]

    def _request(self, method: str, path: str, json_body: Optional[dict]) -> Any:
        url = self._endpoint() + path
        try:
            response = self._http.request(
                method,
                url,
                json=json_body,
                headers={
                    "authorization": f"Bearer {self._api_key}",
                    "accept": "application/json",
                },
            )
        except httpx.HTTPError as exc:
            raise Unavailable(0, None, f"Node unreachable: {exc}") from exc
        body = _parse(response.content)
        if 200 <= response.status_code < 300:
            return body
        raise map_error(response.status_code, body)

    def get(self, path: str) -> Any:
        return self._request("GET", path, None)

    def post(self, path: str, body: dict) -> Any:
        return self._request("POST", path, body)


def _parse(content: bytes) -> Any:
    if not content:
        return None
    try:
        import json

        return json.loads(content)
    except ValueError:
        return None
