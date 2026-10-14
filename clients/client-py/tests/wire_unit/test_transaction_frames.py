"""
The binary transaction body (queen/http/push_binary.py), asserted against a
scripted transport. What is at stake is the default (a client that does not set
both binary_push and push_compression sends the JSON bundle every released
broker knows) and, with the options, the order of the results: the broker lists
the head's acks before the pushes, and the caller must get them back in the
order of its own operations.
"""

from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Tuple

import pytest

from queen import Queen
from queen.http import zstd
from queen.http.push_binary import PushBatch, encode_transaction_frames

from .wire_transport import Recorded, WireTransport, json_answer

MESSAGE = {"transactionId": "in-1", "partitionId": "7", "leaseId": "lease-1"}
FRAMED = {"binary_push": True, "push_compression": True}


def answer(results: List[Dict[str, Any]]):
    body = json.dumps({"transactionId": "tx", "success": True, "results": results})
    return lambda _request: json_answer(body)


def make(respond, **config) -> Tuple[Queen, WireTransport]:
    transport = WireTransport(respond)
    return Queen(url="http://wire.local", transport=transport, retry_attempts=1, **config), transport


def only_transaction(transport: WireTransport) -> Recorded:
    sent = [r for r in transport.requests if r.method == "POST" and r.path == "/api/v1/transaction"]
    assert len(sent) == 1
    return sent[0]


def frame_groups(data: bytes) -> List[Dict[str, Any]]:
    """Decodes ``u32 groups | per group: u16 len, queue | u16 len, partition |
    u32 n | u32 z_len | z`` and every frame of every group."""
    at = 0

    def take(n: int) -> bytes:
        nonlocal at
        at += n
        return data[at - n : at]

    def u16() -> int:
        return struct.unpack("<H", take(2))[0]

    def u32() -> int:
        return struct.unpack("<I", take(4))[0]

    groups = []
    for _ in range(u32()):
        group: Dict[str, Any] = {
            "queue": take(u16()).decode(),
            "partition": take(u16()).decode(),
            "txns": [],
            "payloads": [],
        }
        count = u32()
        frames = zstd.decompress(take(u32()))
        p = 0
        for _ in range(count):
            # u32 body_len | u8 0 | id[16] | u16 len, txn | payload
            (length,) = struct.unpack_from("<I", frames, p)
            frame = frames[p + 4 : p + 4 + length]
            p += 4 + length
            assert frame[0] == 0, "a frame carries no flags"
            assert frame[7] >> 4 == 7, "the message id is a version-7 UUID"
            assert frame[9] & 0xC0 == 0x80
            (txn_length,) = struct.unpack_from("<H", frame, 17)
            group["txns"].append(frame[19 : 19 + txn_length].decode())
            group["payloads"].append(frame[19 + txn_length :].decode())
        assert p == len(frames), "bytes after the frames of a group"
        groups.append(group)
    assert at == len(data), "bytes after the last group"
    return groups


def decode(raw: bytes) -> Tuple[int, Any, List[Dict[str, Any]]]:
    """The flags, the head and the groups of a binary transaction body."""
    assert raw[:4] == b"QTB1"
    (head_length,) = struct.unpack_from("<I", raw, 5)
    head = raw[9 : 9 + head_length]
    return raw[4], (json.loads(head) if head else None), frame_groups(raw[9 + head_length :])


@pytest.mark.parametrize(
    "config",
    [{}, {"binary_push": True}, {"push_compression": True}],
    ids=["no option set", "binary_push alone", "push_compression alone"],
)
async def test_a_transaction_is_the_json_bundle_by_default(config):
    results = [{"index": 0, "type": "ack", "success": True}, {"index": 1, "type": "push", "success": True}]
    client, transport = make(answer(results), **config)
    try:
        await client.transaction().ack(MESSAGE).queue("out").partition("p1").push([{"data": {"n": 1}}]).commit()
    finally:
        await client.close()

    sent = only_transaction(transport)
    assert sent.content_type == "application/json"
    assert [op["type"] for op in json.loads(sent.raw)["operations"]] == ["ack", "push"]


async def test_the_pushes_travel_as_frames_and_the_rest_as_the_json_head():
    results = [
        {"index": 0, "type": "ack", "success": True, "transactionId": "in-1"},
        {"index": 1, "type": "push", "success": True, "transactionId": "a"},
        {"index": 2, "type": "push", "success": True, "transactionId": "tx-b"},
        {"index": 3, "type": "push", "success": True, "transactionId": "c"},
    ]
    client, transport = make(answer(results), **FRAMED)
    try:
        result = await (
            client.transaction()
            .ack(MESSAGE)
            .queue("out").partition("p1").push([{"data": {"n": 1}}])
            .queue("out").partition("p1").push([{"data": {"n": 2}, "transactionId": "tx-b"}])
            .queue("out").partition("p2").push([{"data": "text"}])
            .commit()
        )
    finally:
        await client.close()

    assert result["success"] is True and len(result["results"]) == 4
    sent = only_transaction(transport)
    assert sent.content_type == "application/x-queen-push"
    flags, head, groups = decode(sent.raw)
    assert flags == 0
    # The head: the ack and the lease, and no push.
    assert head == {
        "operations": [
            {"type": "ack", "transactionId": "in-1", "partitionId": "7", "status": "completed", "leaseId": "lease-1"}
        ],
        "requiredLeases": ["lease-1"],
    }
    # The pushes: consecutive items of one partition share a group.
    assert len(groups) == 2
    assert (groups[0]["queue"], groups[0]["partition"], groups[0]["payloads"]) == ("out", "p1", ['{"n":1}', '{"n":2}'])
    assert len(groups[0]["txns"][0]) == 36 and groups[0]["txns"][1] == "tx-b"
    assert (groups[1]["queue"], groups[1]["partition"], groups[1]["payloads"]) == ("out", "p2", ['"text"'])


async def test_the_results_come_back_in_the_order_of_the_callers_operations():
    # The caller pushes, acks, pushes. The broker answers the ack first (it is
    # in the head), then the two pushes, then the rider.
    results = [
        {"index": 0, "type": "ack", "success": True, "transactionId": "in-1"},
        {"index": 1, "type": "push", "success": True, "transactionId": "first"},
        {"index": 2, "type": "push", "success": True, "transactionId": "second"},
        {"index": 3, "type": "kv", "opIndex": 0, "applied": True},
    ]
    client, transport = make(answer(results), **FRAMED)
    try:
        result = await (
            client.transaction()
            .queue("out").push([{"data": 1, "transactionId": "first"}])
            .ack(MESSAGE)
            .queue("out").push([{"data": 2, "transactionId": "second"}])
            .kv.put("saga", "k1", {"step": 1}, ttl_seconds=86400)
            .commit()
        )
    finally:
        await client.close()

    assert [(r["index"], r["type"], r.get("transactionId")) for r in result["results"]] == [
        (0, "push", "first"),
        (1, "ack", "in-1"),
        (2, "push", "second"),
        (3, "kv", None),
    ]
    # On the wire the two pushes are one group, in the caller's order, and the
    # rider travels in the head.
    _, head, groups = decode(only_transaction(transport).raw)
    assert [op["type"] for op in head["operations"]] == ["ack"]
    assert len(head["kv"]) == 1
    assert len(groups) == 1 and groups[0]["txns"] == ["first", "second"]
    assert groups[0]["partition"] == "Default"


async def test_the_results_stay_as_they_are_when_the_acks_come_first_anyway():
    results = [
        {"index": 0, "type": "ack", "success": True, "transactionId": "in-1"},
        {"index": 1, "type": "push", "success": True, "transactionId": "a"},
    ]
    client, _ = make(answer(results), **FRAMED)
    try:
        result = await client.transaction().ack(MESSAGE).queue("out").push([{"data": 1, "transactionId": "a"}]).commit()
    finally:
        await client.close()

    assert result["results"] == results


async def test_a_rolled_back_bundle_comes_back_as_the_broker_answered_it():
    lost = {
        "transactionId": "tx", "success": False, "reason": "kv_precondition",
        "results": [], "failedIndex": 2, "kvReason": "exists",
    }
    client, _ = make(lambda _request: json_answer(json.dumps(lost)), **FRAMED)
    try:
        result = await (
            client.transaction()
            .queue("out").push([{"data": 1}])
            .ack(MESSAGE)
            .once("idem", "order-1", ttl_seconds=86400)
            .commit()
        )
    finally:
        await client.close()

    assert result == lost


async def test_a_bundle_whose_push_carries_a_trace_id_stays_json():
    trace = "0190a0a0-0000-4000-8000-000000000001"
    client, transport = make(answer([{"index": 0, "type": "push", "success": True}]), **FRAMED)
    try:
        await client.transaction().queue("out").push([{"data": 1, "traceId": trace}]).commit()
    finally:
        await client.close()

    sent = only_transaction(transport)
    assert sent.content_type == "application/json"
    assert json.loads(sent.raw)["operations"][0]["items"][0]["traceId"] == trace


async def test_a_bundle_without_pushes_stays_json():
    # There is nothing to frame.
    client, transport = make(answer([{"index": 0, "type": "ack", "success": True}]), **FRAMED)
    try:
        await client.transaction().ack(MESSAGE).commit()
    finally:
        await client.close()

    sent = only_transaction(transport)
    assert sent.content_type == "application/json" and sent.raw[:1] == b"{"


def test_the_counts_flag_and_an_empty_head():
    body = encode_transaction_frames(None, [PushBatch("q", "p", [b"1"])], counts=True)

    assert body[:9] == b"QTB1\x02\x00\x00\x00\x00"
    flags, head, groups = decode(body)
    assert (flags, head) == (2, None)
    assert [g["payloads"] for g in groups] == [["1"]]
