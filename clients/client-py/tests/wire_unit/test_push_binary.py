"""
The binary push body (queen/http/push_binary.py), asserted against a scripted
transport. What is at stake is the default: a client that does not set
binary_push must send the JSON request every released broker knows.
"""

from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Tuple

import pytest

from queen import Queen
from queen.http import zstd
from queen.http.push_binary import (
    PushBatch,
    binary_push_body,
    encode_push_binary,
    encode_push_frames,
)

from .wire_transport import Recorded, WireTransport, json_answer

TXN = "0190aaaa-0000-7000-8000-000000000001"
# The client sends a trace id only when it is a UUID.
TRACE = "0190aaaa-0000-4000-8000-000000000001"


def queued(count: int = 1):
    return lambda _request: json_answer(json.dumps([{"status": "queued"}] * count))


def make(answer, **config) -> Tuple[Queen, WireTransport]:
    transport = WireTransport(answer)
    return Queen(url="http://wire.local", transport=transport, retry_attempts=1, **config), transport


class Reader:
    """Reads the fields of a binary push body in order."""

    def __init__(self, data: bytes) -> None:
        self.data, self.at = data, 0

    def take(self, n: int) -> bytes:
        self.at += n
        return self.data[self.at - n : self.at]

    def u16(self) -> int:
        return struct.unpack("<H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def text16(self) -> str:
        return self.take(self.u16()).decode()

    def left(self) -> int:
        return len(self.data) - self.at


def plain_groups(data: bytes, with_txn: bool) -> List[Dict[str, Any]]:
    """The groups of a plain binary push body (after magic and flags)."""
    r = Reader(data)
    groups = []
    for _ in range(r.u32()):
        group: Dict[str, Any] = {"queue": r.text16(), "partition": r.text16(), "items": []}
        for _ in range(r.u32()):
            txn = r.text16() if with_txn else None
            group["items"].append((txn, r.take(r.u32()).decode()))
        groups.append(group)
    assert r.left() == 0, "bytes left after the last payload"
    return groups


def framed_groups(data: bytes) -> List[Dict[str, Any]]:
    """The groups of a framed binary push body, each with its frames unpacked."""
    r = Reader(data)
    groups = []
    for _ in range(r.u32()):
        group: Dict[str, Any] = {"queue": r.text16(), "partition": r.text16(), "count": r.u32(), "frames": []}
        f = Reader(zstd.decompress(r.take(r.u32())))
        while f.left() > 0:
            body = Reader(f.take(f.u32()))
            flags = body.take(1)[0]
            message_id = body.take(16)
            group["frames"].append(
                {"flags": flags, "id": message_id, "txn": body.text16(), "payload": body.take(body.left()).decode()}
            )
        groups.append(group)
    assert r.left() == 0, "bytes left after the last group"
    return groups


def one_push(transport: WireTransport) -> Recorded:
    assert len(transport.pushes) == 1
    return transport.pushes[0]


async def test_push_is_json_by_default():
    client, transport = make(queued())
    try:
        res = await client.queue("orders").partition("p1").push({"data": {"a": 1}})
    finally:
        await client.close()

    assert res[0]["status"] == "queued"
    sent = one_push(transport)
    assert sent.content_type == "application/json"
    body = json.loads(sent.raw)
    assert list(body) == ["items"]
    assert body["items"][0]["payload"] == {"a": 1}


async def test_binary_push_sends_the_binary_body_with_each_transaction_id():
    client, transport = make(queued(), binary_push=True)
    try:
        res = await client.queue("orders").partition("p1").push({"data": {"a": 1}, "transactionId": TXN})
    finally:
        await client.close()

    assert res[0]["status"] == "queued"
    sent = one_push(transport)
    assert sent.content_type == "application/x-queen-push"
    assert sent.raw[:5] == b"QPB1\x04"
    assert plain_groups(sent.raw[5:], True) == [
        {"queue": "orders", "partition": "p1", "items": [(TXN, '{"a":1}')]}
    ]


def test_consecutive_items_of_one_partition_share_a_group():
    def item(partition: str, payload: int) -> Dict[str, Any]:
        return {"queue": "q", "partition": partition, "payload": payload, "transactionId": f"t{payload}"}

    data = binary_push_body([item("a", 1), item("a", 2), item("b", 3), item("a", 4)], False)

    assert data is not None and data[:5] == b"QPB1\x04"
    assert [(g["partition"], g["items"]) for g in plain_groups(data[5:], True)] == [
        ("a", [("t1", "1"), ("t2", "2")]),
        ("b", [("t3", "3")]),
        ("a", [("t4", "4")]),
    ]


async def test_binary_push_carries_payloads_that_are_not_objects_and_non_ascii_text():
    client, transport = make(queued(5), binary_push=True)
    payloads = ['testo "con" virgolette\n', [1, 2], 7, None, {"è": "€"}]
    try:
        await client.queue("ordini").partition("città").push([{"data": p} for p in payloads])
    finally:
        await client.close()

    (group,) = plain_groups(one_push(transport).raw[5:], True)
    assert group["partition"] == "città"
    assert [json.loads(payload) for _, payload in group["items"]] == payloads


async def test_an_item_with_a_trace_id_stays_json():
    client, transport = make(queued(), binary_push=True)
    try:
        await client.queue("orders").push({"data": {"a": 1}, "traceId": TRACE})
    finally:
        await client.close()

    sent = one_push(transport)
    assert sent.content_type == "application/json"
    assert json.loads(sent.raw)["items"][0]["traceId"] == TRACE


def test_a_payload_json_does_not_carry_stays_with_the_json_request():
    # The binary body is not built; the JSON request then fails as it always did.
    assert binary_push_body([{"queue": "q", "partition": "p", "payload": {1, 2}}], False) is None
    assert binary_push_body([{"queue": "q", "partition": "p", "payload": float("nan")}], False) is None
    assert binary_push_body([{"queue": "q", "partition": "p" * 70000, "payload": 1}], False) is None
    assert binary_push_body([{"queue": "q", "partition": "p", "payload": 1, "transactionId": 5}], False) is None


async def test_a_buffered_push_is_binary_too():
    client, transport = make(queued(), binary_push=True)
    try:
        await client.queue("orders").partition("p1").buffer({"message_count": 10, "time_millis": 60000}).push({"data": {"a": 1}})
        await client.flush_all_buffers()
    finally:
        await client.close()

    sent = one_push(transport)
    assert sent.raw[:5] == b"QPB1\x04"
    assert plain_groups(sent.raw[5:], True)[0]["items"][0][1] == '{"a":1}'


async def test_push_compression_sends_each_partition_as_one_zstd_frame_of_stored_frames():
    client, transport = make(queued(3), binary_push=True, push_compression=True)
    try:
        await client.queue("orders").partition("p1").push(
            [{"data": {"a": 1}, "transactionId": TXN}, {"data": {"a": 2}}, {"data": "x"}]
        )
    finally:
        await client.close()

    sent = one_push(transport)
    assert sent.content_type == "application/x-queen-push"
    assert sent.raw[:5] == b"QPB1\x08"
    (group,) = framed_groups(sent.raw[5:])
    assert (group["queue"], group["partition"], group["count"]) == ("orders", "p1", 3)
    assert [f["payload"] for f in group["frames"]] == ['{"a":1}', '{"a":2}', '"x"']
    assert group["frames"][0]["txn"] == TXN
    for frame in group["frames"]:
        assert frame["flags"] == 0
        assert frame["id"][6] >> 4 == 7, "the message id is a version-7 UUID"
        assert frame["id"][8] & 0xC0 == 0x80
        assert len(frame["txn"]) == 36
    assert len({f["id"] for f in group["frames"]}) == 3


def test_a_message_without_a_transaction_id_gets_the_text_of_its_id():
    data = encode_push_frames([PushBatch("q", "p", [b"1"])])
    frame = framed_groups(data[5:])[0]["frames"][0]
    h = frame["id"].hex()
    assert frame["txn"] == f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def test_each_zstd_frame_carries_its_content_size():
    # The broker asks for it of a frame it stores as it came.
    data = encode_push_frames([PushBatch("q", "p", [b'{"a":1}'])])
    r = Reader(data[5:])
    r.u32(), r.text16(), r.text16(), r.u32()
    frame = r.take(r.u32())
    assert struct.unpack("<I", frame[:4])[0] == 0xFD2FB528, "a zstd frame"
    # Frame header descriptor: the single-segment bit, or a content-size field
    # of 2 bytes or more, says the frame carries its content size.
    assert frame[4] & 0x20 or frame[4] >> 6


def test_the_counts_flag():
    batches = [PushBatch("q", "p", [b"1"])]
    assert encode_push_binary(batches, counts=True)[:5] == b"QPB1\x02"
    assert encode_push_frames(batches, counts=True)[4] == 8 | 2


def test_the_zstd_flag_compresses_the_whole_body_with_its_raw_length_in_front():
    batches = [PushBatch("q", "p", [b"1"])]
    plain = encode_push_binary(batches)
    packed = encode_push_binary(batches, compress=True)
    assert packed[:5] == b"QPB1\x01"
    assert struct.unpack("<I", packed[5:9])[0] == len(plain) - 5
    assert zstd.decompress(packed[9:]) == plain[5:]


def test_transaction_ids_must_match_the_payloads():
    with pytest.raises(ValueError, match="1 transaction ids for 2 payloads"):
        encode_push_binary([PushBatch("q", "p", [b"1", b"2"], ["t"])])
