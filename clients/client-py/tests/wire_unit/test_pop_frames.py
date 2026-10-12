"""
The framed pop answer (queen/http/pop_frames.py), against hand-built answers
and a scripted transport. What is at stake: a consumer that sets framed_pop
must be handed exactly the messages the JSON answer gives it, and one that does
not set it must send the request every released broker knows.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import List, Optional, Tuple

import httpx
import pytest

from queen import Queen
from queen.http import zstd
from queen.http.pop_frames import decode_pop_frames, is_pop_frames
from queen.errors import ConflationUnsupportedError

from .wire_transport import WireTransport, framed_answer, json_answer

CONSUME_TIMEOUT_S = 5.0


def u16(n: int) -> bytes:
    return struct.pack("<H", n)


def u32(n: int) -> bytes:
    return struct.pack("<I", n)


def u64(n: int) -> bytes:
    return struct.pack("<Q", n)


def text(s: str) -> bytes:
    data = s.encode()
    return u16(len(data)) + data


def stored_frame(
    message_id: int, txn: str, payload: str, producer: Optional[str] = None, trace: Optional[str] = None
) -> bytes:
    """One stored frame: u32 body_len | flags | id | [trace] | txn | [producer] | payload."""
    id_bytes = bytearray(16)
    id_bytes[0], id_bytes[6], id_bytes[8] = message_id, 0x70, 0x80
    body = bytes([(1 if trace else 0) | (2 if producer else 0)]) + bytes(id_bytes)
    if trace:
        body += bytes.fromhex(trace.replace("-", ""))
    body += text(txn)
    if producer:
        body += text(producer)
    body += payload.encode()
    return u32(len(body)) + body


def framed(own: str, start: int, end: int, compress: bool, frames: List[bytes]) -> bytes:
    """A framed pop answer of one claim on partition 7 ("p7") of "orders",
    lease "lease-1": one record at base offset 10 holding ``frames``, created
    at 2023-11-14T22:13:20Z, of which the claim covers start..=end."""
    raw = b"".join(frames)
    if compress:
        raw = zstd.compress(raw)
    return b"".join(
        [
            b"QPF1\x01", u32(len(own.encode())), own.encode(),
            text("orders"), text("lease-1"), text("__QUEUE_MODE__"),
            u32(1),  # claims
            u64(7), text("p7"), u64(start), u64(end),
            u32(3),  # delivery attempt
            u32(1),  # records
            u64(10), struct.pack("<q", 1_700_000_000_000_000), u32(len(frames)),
            bytes([1 if compress else 0]), u32(len(raw)), raw,
        ]
    )


def two_frames() -> List[bytes]:
    return [stored_frame(1, "t-10", '{"n":10}'), stored_frame(2, "t-11", '{"n":11,"s":"x"}', producer="svc")]


# The JSON answer of the same two messages, as the broker renders it.
TWO_MESSAGES_JSON = (
    '{"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":['
    '{"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},'
    '{"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11}'
    '],"partitionsClaimed":1}'
)


def make(answer, **config) -> Tuple[Queen, WireTransport]:
    transport = WireTransport(answer)
    return Queen(url="http://wire.local", transport=transport, retry_attempts=1, **config), transport


def strip_trace(messages):
    """The client hangs a trace() function on each message; the rest is the answer's."""
    return [{k: v for k, v in m.items() if k != "trace"} for m in messages]


@pytest.mark.parametrize("compress", [False, True])
def test_decode_unpacks_a_record_into_the_json_answers_dict(compress):
    got = decode_pop_frames(framed('{"frames":true}', 10, 11, compress, two_frames()))
    want = json.loads(TWO_MESSAGES_JSON)

    assert got == want
    assert list(got["messages"][0]) == list(want["messages"][0])


@pytest.mark.parametrize("compress", [False, True])
def test_decode_keeps_only_the_messages_inside_the_claim(compress):
    got = decode_pop_frames(framed('{"frames":true}', 11, 11, compress, two_frames()))

    assert len(got["messages"]) == 1
    message = got["messages"][0]
    assert (message["offset"], message["transactionId"], message["producerSub"]) == (11, "t-11", "svc")
    assert message["id"] == "02000000-0000-7000-8000-000000000000"


def test_decode_reads_a_trace_id_an_empty_payload_and_an_answer_without_own_keys():
    trace = "0190aaaa-0000-4000-8000-000000000001"
    answer = framed("", 10, 11, False, [stored_frame(1, "t-10", "", trace=trace), stored_frame(2, "t-11", '[1,"due"]')])
    got = decode_pop_frames(answer)

    assert got["messages"][0]["traceId"] == trace
    assert got["messages"][0]["data"] is None
    assert got["messages"][1]["data"] == [1, "due"]

    # Version 0 has no JSON object in front of the names.
    assert decode_pop_frames(b"QPF1\x00" + answer[9:]) == got


def test_decode_adds_the_answers_own_keys_without_frames():
    own = '{"frames":true,"conflation":true,"autopilot":{"partitions":4,"batch":50}}'
    got = decode_pop_frames(framed(own, 10, 11, False, two_frames()))

    assert "frames" not in got
    assert got["conflation"] is True
    assert got["autopilot"] == {"partitions": 4, "batch": 50}


def test_decode_rejects_damage():
    answer = framed('{"frames":true}', 10, 11, False, two_frames())
    frames = two_frames()

    assert is_pop_frames(answer)
    assert not is_pop_frames(TWO_MESSAGES_JSON.encode())
    with pytest.raises(ValueError, match="framed pop answer"):
        decode_pop_frames(answer[:-3])
    with pytest.raises(ValueError, match="not a framed pop answer"):
        decode_pop_frames(TWO_MESSAGES_JSON.encode())

    future = bytearray(answer)
    future[4] = 9
    with pytest.raises(ValueError, match="unknown version 9"):
        decode_pop_frames(bytes(future))

    # A record that declares more frames than it holds.
    short = bytearray(answer)
    at = len(short) - len(frames[0]) - len(frames[1]) - 9
    short[at : at + 4] = u32(3)
    with pytest.raises(ValueError, match="a record is shorter than its 3 frames"):
        decode_pop_frames(bytes(short))

    bad_codec = bytearray(answer)
    bad_codec[len(bad_codec) - len(frames[0]) - len(frames[1]) - 5] = 7
    with pytest.raises(ValueError, match="unknown codec 7"):
        decode_pop_frames(bytes(bad_codec))


async def test_a_pop_sends_no_frames_parameter_by_default():
    client, transport = make(lambda _r: json_answer(TWO_MESSAGES_JSON))
    try:
        messages = await client.queue("orders").batch(2).wait(False).pop()
    finally:
        await client.close()

    assert len(messages) == 2
    assert transport.pops[0].param("frames") is None


@pytest.mark.parametrize("compress", [False, True])
async def test_a_framed_pop_gives_the_messages_of_the_json_answer(compress):
    plain, _ = make(lambda _r: json_answer(TWO_MESSAGES_JSON))
    try:
        want = await plain.queue("orders").batch(2).wait(False).pop()
    finally:
        await plain.close()

    answer = framed('{"frames":true}', 10, 11, compress, two_frames())
    client, transport = make(lambda _r: framed_answer(answer), framed_pop=True)
    try:
        got = await client.queue("orders").batch(2).wait(False).pop()
    finally:
        await client.close()

    assert transport.pops[0].param("frames") == "true"
    assert len(got) == 2
    assert strip_trace(got) == strip_trace(want)


async def test_a_framed_pop_asks_for_frames_on_the_partition_and_namespace_routes_too():
    client, transport = make(lambda _r: httpx.Response(204), framed_pop=True)
    try:
        await client.queue("orders").partition("p7").wait(False).pop()
        await client.queue().namespace("ns").wait(False).pop()
    finally:
        await client.close()

    by_partition, by_namespace = transport.pops
    assert by_partition.path == "/api/v1/pop/queue/orders/partition/p7"
    assert by_partition.param("frames") == "true"
    assert by_namespace.path == "/api/v1/pop"
    assert by_namespace.param("frames") == "true"


@pytest.mark.parametrize("content_type", ["application/octet-stream", "application/json"])
async def test_a_framed_answer_is_known_by_its_first_bytes_whatever_its_content_type(content_type):
    answer = framed('{"frames":true}', 10, 11, False, two_frames())
    client, _ = make(
        lambda _r: httpx.Response(200, content=answer, headers={"Content-Type": content_type}),
        framed_pop=True,
    )
    try:
        got = await client.queue("orders").batch(2).wait(False).pop()
    finally:
        await client.close()

    assert [m["transactionId"] for m in got] == ["t-10", "t-11"]


async def test_a_framed_pop_reads_a_json_answer_as_always():
    # A broker that does not know the parameter answers JSON.
    client, _ = make(lambda _r: json_answer(TWO_MESSAGES_JSON), framed_pop=True)
    try:
        got = await client.queue("orders").batch(2).wait(False).pop()
    finally:
        await client.close()

    assert len(got) == 2
    assert got[1]["transactionId"] == "t-11"
    assert got[1]["data"]["s"] == "x"


async def test_a_framed_pop_of_nothing():
    client, _ = make(lambda _r: httpx.Response(204), framed_pop=True)
    try:
        assert await client.queue("orders").wait(False).pop() == []
    finally:
        await client.close()


async def test_a_framed_pop_carries_the_autopilots_choice():
    own = '{"frames":true,"autopilot":{"partitions":4,"batch":50}}'
    answer = framed(own, 10, 11, False, two_frames())
    client, _ = make(lambda _r: framed_answer(answer), framed_pop=True)
    try:
        result = await client.queue("orders").wait(False).pop_result()
    finally:
        await client.close()

    assert len(result.messages) == 2
    assert result.autopilot is not None
    assert (result.autopilot.partitions, result.autopilot.batch) == (4, 50)


async def test_a_framed_pop_carries_the_conflation_echo_and_still_raises_without_it():
    echoed = framed('{"frames":true,"conflation":true}', 10, 11, False, two_frames())
    client, _ = make(lambda _r: framed_answer(echoed), framed_pop=True)
    try:
        got = await client.queue("orders").group("g").conflation(True).wait(False).pop()
    finally:
        await client.close()
    assert len(got) == 2

    silent = framed('{"frames":true}', 10, 11, False, two_frames())
    client, _ = make(lambda _r: framed_answer(silent), framed_pop=True)
    try:
        with pytest.raises(ConflationUnsupportedError):
            await client.queue("orders").group("g2").conflation(True).wait(False).pop()
    finally:
        await client.close()


async def test_consume_with_framed_pops_hands_the_handler_the_claim_in_order():
    answer = framed('{"frames":true}', 10, 11, True, two_frames())
    client, transport = make(lambda _r: framed_answer(answer), framed_pop=True)
    seen: List[str] = []

    async def handler(message):
        seen.append(f"{message['transactionId']}@{message['partitionId']}/{message['leaseId']}")

    try:
        await asyncio.wait_for(
            client.queue("orders").batch(2).wait(False).auto_ack(False).each().limit(2).consume(handler),
            CONSUME_TIMEOUT_S,
        )
    finally:
        await client.close()

    assert seen == ["t-10@7/lease-1", "t-11@7/lease-1"]
    assert transport.pops
    assert all(pop.param("frames") == "true" for pop in transport.pops)
