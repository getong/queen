/**
 * The framed pop answer (client-v2/http/popFrames.js), against hand-built
 * answers and a server that plays them. What is at stake: a consumer that
 * sets framedPop must be handed exactly the messages the JSON answer gives it,
 * and one that does not set it must send the request every released broker
 * knows.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { zstdCompressSync } from 'node:zlib'

import { Queen, CONFLATION_UNSUPPORTED } from '../../client-v2/index.js'
import { decodePopFrames, isPopFrames } from '../../client-v2/http/popFrames.js'
import { withWireServer, query } from './_wireServer.js'

const u16 = (n) => { const b = Buffer.alloc(2); b.writeUInt16LE(n); return b }
const u32 = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const u64 = (n) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(BigInt(n)); return b }
const text = (s) => Buffer.concat([u16(Buffer.byteLength(s)), Buffer.from(s)])

/** One stored frame: u32 body_len | flags | id | [trace] | txn | [producer] | payload. */
function storedFrame(id, txn, payload, { producer = null, trace = null } = {}) {
  const idBytes = Buffer.alloc(16)
  idBytes[0] = id
  idBytes[6] = 0x70
  idBytes[8] = 0x80
  const body = Buffer.concat([
    Buffer.of((trace ? 1 : 0) | (producer ? 2 : 0)),
    idBytes,
    trace ? Buffer.from(trace.replaceAll('-', ''), 'hex') : Buffer.alloc(0),
    text(txn),
    producer ? text(producer) : Buffer.alloc(0),
    Buffer.from(payload)
  ])
  return Buffer.concat([u32(body.length), body])
}

/**
 * A framed pop answer of one claim on partition 7 ("p7") of "orders", lease
 * "lease-1": one record at base offset 10 holding `frames`, created at
 * 2023-11-14T22:13:20Z, of which the claim covers start..=end.
 */
function framedAnswer(own, start, end, compress, frames) {
  let raw = Buffer.concat(frames)
  if (compress) raw = zstdCompressSync(raw)
  return Buffer.concat([
    Buffer.from('QPF1'), Buffer.of(1), u32(Buffer.byteLength(own)), Buffer.from(own),
    text('orders'), text('lease-1'), text('__QUEUE_MODE__'),
    u32(1), // claims
    u64(7), text('p7'), u64(start), u64(end),
    u32(3), // delivery attempt
    u32(1), // records
    u64(10), u64(1_700_000_000_000_000n), u32(frames.length), Buffer.of(compress ? 1 : 0), u32(raw.length), raw
  ])
}

const twoFrames = () => [storedFrame(1, 't-10', '{"n":10}'), storedFrame(2, 't-11', '{"n":11,"s":"x"}', { producer: 'svc' })]

// The JSON answer of the same two messages, as the broker renders it.
const TWO_MESSAGES_JSON = '{"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":[' +
  '{"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},' +
  '{"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11}' +
  '],"partitionsClaimed":1}'

const json = (body) => ({ body })
const framed = (body) => ({ contentType: 'application/x-queen-pop', body })
const pops = (hits) => hits.filter(h => h.method === 'GET' && h.url.startsWith('/api/v1/pop'))

async function withQueen(config, answer, run) {
  await withWireServer(answer, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1, ...config })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('decodePopFrames', () => {
  for (const compress of [false, true]) {
    it(`unpacks ${compress ? 'a zstd' : 'a raw'} record into the JSON answer's object`, async () => {
      const got = await decodePopFrames(framedAnswer('{"frames":true}', 10, 11, compress, twoFrames()))

      assert.deepEqual(got, JSON.parse(TWO_MESSAGES_JSON))
      assert.deepEqual(Object.keys(got.messages[0]), Object.keys(JSON.parse(TWO_MESSAGES_JSON).messages[0]))
    })

    it(`keeps only the messages inside the claim (${compress ? 'zstd' : 'raw'})`, async () => {
      const got = await decodePopFrames(framedAnswer('{"frames":true}', 11, 11, compress, twoFrames()))

      assert.equal(got.messages.length, 1)
      assert.equal(got.messages[0].offset, 11)
      assert.equal(got.messages[0].transactionId, 't-11')
      assert.equal(got.messages[0].producerSub, 'svc')
      assert.equal(got.messages[0].id, '02000000-0000-7000-8000-000000000000')
    })
  }

  it('reads a trace id, an empty payload as null, and the answer without its own keys', async () => {
    const trace = '0190aaaa-0000-4000-8000-000000000001'
    const answer = framedAnswer('', 10, 11, false, [storedFrame(1, 't-10', '', { trace }), storedFrame(2, 't-11', '[1,"due"]')])
    const got = await decodePopFrames(answer)

    assert.equal(got.messages[0].traceId, trace)
    assert.equal(got.messages[0].data, null)
    assert.deepEqual(got.messages[1].data, [1, 'due'])

    // Version 0 has no JSON object in front of the names.
    const v0 = Buffer.concat([Buffer.from('QPF1'), Buffer.of(0), answer.subarray(9)])
    assert.deepEqual(await decodePopFrames(v0), got)
  })

  it('adds the answer\'s own keys, without `frames`', async () => {
    const own = '{"frames":true,"conflation":true,"autopilot":{"partitions":4,"batch":50}}'
    const got = await decodePopFrames(framedAnswer(own, 10, 11, false, twoFrames()))

    assert.equal('frames' in got, false)
    assert.equal(got.conflation, true)
    assert.deepEqual(got.autopilot, { partitions: 4, batch: 50 })
  })

  it('rejects damage', async () => {
    const answer = framedAnswer('{"frames":true}', 10, 11, false, twoFrames())

    assert.equal(isPopFrames(answer), true)
    assert.equal(isPopFrames(Buffer.from(TWO_MESSAGES_JSON)), false)
    await assert.rejects(decodePopFrames(answer.subarray(0, answer.length - 3)), /framed pop answer/)
    await assert.rejects(decodePopFrames(Buffer.from(TWO_MESSAGES_JSON)), /not a framed pop answer/)

    const future = Buffer.from(answer)
    future[4] = 9
    await assert.rejects(decodePopFrames(future), /unknown version 9/)

    // A record that declares more frames than it holds.
    const short = Buffer.from(answer)
    const frames = twoFrames()
    short.writeUInt32LE(3, short.length - frames[0].length - frames[1].length - 9)
    await assert.rejects(decodePopFrames(short), /a record is shorter than its 3 frames/)

    const badCodec = Buffer.from(answer)
    badCodec[badCodec.length - frames[0].length - frames[1].length - 5] = 7
    await assert.rejects(decodePopFrames(badCodec), /unknown codec 7/)
  })
})

describe('pop — the default', () => {
  it('sends no frames parameter', async () => {
    await withQueen({}, () => json(TWO_MESSAGES_JSON), async (queen, hits) => {
      const messages = await queen.queue('orders').batch(2).wait(false).pop()

      assert.equal(messages.length, 2)
      assert.equal(query(pops(hits)[0].url).has('frames'), false)
    })
  })
})

describe('pop — framedPop', () => {
  for (const compress of [false, true]) {
    it(`gives the messages of the JSON answer (${compress ? 'zstd' : 'raw'} record)`, async () => {
      let want = null
      await withQueen({}, () => json(TWO_MESSAGES_JSON), async (queen) => {
        want = await queen.queue('orders').batch(2).wait(false).pop()
      })

      const answer = framedAnswer('{"frames":true}', 10, 11, compress, twoFrames())
      await withQueen({ framedPop: true }, () => framed(answer), async (queen, hits) => {
        const got = await queen.queue('orders').batch(2).wait(false).pop()

        assert.equal(query(pops(hits)[0].url).get('frames'), 'true')
        assert.equal(got.length, 2)
        assert.deepEqual(got, want)
      })
    })
  }

  it('asks for frames on the partition and the namespace routes too', async () => {
    await withQueen({ framedPop: true }, () => ({ status: 204 }), async (queen, hits) => {
      await queen.queue('orders').partition('p7').wait(false).pop()
      await queen.queue().namespace('ns').wait(false).pop()

      const [byPartition, byNamespace] = pops(hits)
      assert.ok(byPartition.url.startsWith('/api/v1/pop/queue/orders/partition/p7?'))
      assert.equal(query(byPartition.url).get('frames'), 'true')
      assert.ok(byNamespace.url.startsWith('/api/v1/pop?'))
      assert.equal(query(byNamespace.url).get('frames'), 'true')
    })
  })

  it('knows a framed answer by its first bytes, whatever its content type', async () => {
    const answer = framedAnswer('{"frames":true}', 10, 11, false, twoFrames())
    for (const contentType of ['application/octet-stream', 'application/json']) {
      await withQueen({ framedPop: true }, () => ({ contentType, body: answer }), async (queen) => {
        const got = await queen.queue('orders').batch(2).wait(false).pop()

        assert.deepEqual(got.map(m => m.transactionId), ['t-10', 't-11'], contentType)
      })
    }
  })

  it('reads a JSON answer as always: a broker that does not know the parameter', async () => {
    await withQueen({ framedPop: true }, () => json(TWO_MESSAGES_JSON), async (queen) => {
      const got = await queen.queue('orders').batch(2).wait(false).pop()

      assert.equal(got.length, 2)
      assert.equal(got[1].transactionId, 't-11')
      assert.equal(got[1].data.s, 'x')
    })
  })

  it('reads an empty pop', async () => {
    await withQueen({ framedPop: true }, () => ({ status: 204 }), async (queen) => {
      assert.deepEqual(await queen.queue('orders').wait(false).pop(), [])
    })
  })

  it('carries the autopilot\'s choice in the answer\'s own keys', async () => {
    const own = '{"frames":true,"autopilot":{"partitions":4,"batch":50}}'
    await withQueen({ framedPop: true }, () => framed(framedAnswer(own, 10, 11, false, twoFrames())), async (queen) => {
      const { messages, autopilot } = await queen.queue('orders').wait(false).popResult()

      assert.equal(messages.length, 2)
      assert.deepEqual(autopilot, { partitions: 4, batch: 50, waitMillis: 0 })
    })
  })

  it('carries the conflation echo, and still raises when the broker sent none', async () => {
    const echoed = framedAnswer('{"frames":true,"conflation":true}', 10, 11, false, twoFrames())
    await withQueen({ framedPop: true }, () => framed(echoed), async (queen) => {
      const got = await queen.queue('orders').group('g').conflation(true).wait(false).pop()
      assert.equal(got.length, 2)
    })

    const silent = framedAnswer('{"frames":true}', 10, 11, false, twoFrames())
    await withQueen({ framedPop: true }, () => framed(silent), async (queen) => {
      await assert.rejects(
        queen.queue('orders').group('g2').conflation(true).wait(false).pop(),
        { code: CONFLATION_UNSUPPORTED }
      )
    })
  })
})

describe('consume — framedPop', () => {
  // The consumer loop on framed answers: the handler gets every message of
  // the claim, in order, and the loop stops at its limit.
  it('hands the handler the messages of the claim, in order', async () => {
    const answer = framedAnswer('{"frames":true}', 10, 11, true, twoFrames())
    await withQueen({ framedPop: true }, () => framed(answer), async (queen, hits) => {
      const seen = []
      await queen.queue('orders').batch(2).wait(false).autoAck(false).each().limit(2)
        .consume(async (message) => {
          seen.push(`${message.transactionId}@${message.partitionId}/${message.leaseId}`)
        })

      assert.deepEqual(seen, ['t-10@7/lease-1', 't-11@7/lease-1'])
      assert.ok(pops(hits).length >= 1)
      for (const hit of pops(hits)) {
        assert.equal(query(hit.url).get('frames'), 'true')
      }
    })
  })
})
