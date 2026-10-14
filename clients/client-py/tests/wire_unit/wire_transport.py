"""
A scripted transport for the binary wire forms -- no broker, no sockets.

Same seam as tests/conflation_unit/pop_transport.py (``Queen(transport=...)``),
but it keeps every request's body as bytes (a binary push body does not survive
a JSON parse) and answers with the bytes and content type a test hands it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
from urllib.parse import parse_qs

import httpx


@dataclass
class Recorded:
    """One request as the broker would have received it."""

    method: str
    path: str
    content_type: Optional[str]
    raw: bytes
    query: Dict[str, List[str]] = field(default_factory=dict)

    def param(self, name: str) -> Optional[str]:
        values = self.query.get(name)
        return values[0] if values else None


def json_answer(text: str) -> httpx.Response:
    return httpx.Response(200, content=text.encode(), headers={"Content-Type": "application/json"})


def framed_answer(data: bytes) -> httpx.Response:
    return httpx.Response(200, content=data, headers={"Content-Type": "application/x-queen-pop"})


class WireTransport(httpx.AsyncBaseTransport):
    """Answers every request with ``answer(recorded)``; a pop budget bounds a
    consume loop that does not stop (see PopBudgetExhausted next door)."""

    def __init__(self, answer: Callable[[Recorded], httpx.Response], max_pops: int = 20) -> None:
        self.answer = answer
        self.max_pops = max_pops
        self.requests: List[Recorded] = []

    @property
    def pushes(self) -> List[Recorded]:
        return [r for r in self.requests if r.method == "POST" and r.path == "/api/v1/push"]

    @property
    def pops(self) -> List[Recorded]:
        return [r for r in self.requests if r.method == "GET" and r.path.startswith("/api/v1/pop")]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # A scripted transport never suspends on its own: one yield per request
        # keeps a consume loop cancellable.
        await asyncio.sleep(0)
        query = request.url.query
        rec = Recorded(
            method=request.method,
            path=request.url.path,
            content_type=request.headers.get("content-type"),
            raw=request.content,
            query=parse_qs(query.decode() if isinstance(query, bytes) else query),
        )
        self.requests.append(rec)
        if len(self.pops) > self.max_pops:
            raise RuntimeError(f"the consume loop issued more than {self.max_pops} pops")
        return self.answer(rec)
