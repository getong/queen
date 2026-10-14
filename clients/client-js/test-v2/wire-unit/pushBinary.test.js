/**
 * The binary push body (client-v2/http/pushBinary.js), asserted against a
 * server that records the bytes. What is at stake is the default: a client
 * that does not set binaryPush must send the JSON request every released
 * broker knows.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { zstdDecompressSync } from 'node:zlib'

import { Queen } from '../../client-v2/index.js'
import { binaryPushBody, encodePushBinary, encodePushFrames } from '../../client-v2/http/pushBinary.js'
import { withWireServer } from './_wireServer.js'

const TXN = '0190aaaa-0000-7000-8000-000000000001'
// The client sends a trace id only when it is a version-4 UUID.
const TRACE = '0190aaaa-0000-4000-8000-000000000001'

const queued = (n = 1) => ({ body: JSON.stringify(Array.from({ length: n }, () => ({ status: 'queued' }))) })
const pushes = (hits) => hits.filter(h => h.method === 'POST' && h.url === '/api/v1/push')

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

/** Reads the fields of a binary push body in order. */
function reader(bytes) {
  let at = 0
  const r = {
    u16() { at += 2; return bytes.readUInt16LE(at - 2) },
    u32() { at += 4; return bytes.readUInt32LE(at - 4) },
    take(n) { at += n; return bytes.subarray(at - n, at) },
    text16() { return r.take(r.u16()).toString() },
    text32() { return r.take(r.u32()).toString() },
    left() { return bytes.length - at }
  }
  return r
}

/** The groups of a plain binary push body (after magic and flags). */
function plainGroups(bytes, withTxn) {
  const r = reader(bytes)
  const groups = []
  for (let g = r.u32(); g > 0; g--) {
    const group = { queue: r.text16(), partition: r.text16(), items: [] }
    for (let n = r.u32(); n > 0; n--) {
      const txn = withTxn ? r.text16() : null
      group.items.push({ txn, payload: r.text32() })
    }
    groups.push(group)
  }
  assert.equal(r.left(), 0, 'bytes left after the last payload')
  return groups
}

/** The groups of a framed binary push body, each with its frames unpacked. */
function framedGroups(bytes) {
  const r = reader(bytes)
  const groups = []
  for (let g = r.u32(); g > 0; g--) {
    const group = { queue: r.text16(), partition: r.text16(), count: r.u32(), frames: [] }
    const f = reader(zstdDecompressSync(r.take(r.u32())))
    while (f.left() > 0) {
      const body = reader(f.take(f.u32()))
      const flags = body.take(1)[0]
      const id = Buffer.from(body.take(16))
      group.frames.push({ flags, id, txn: body.text16(), payload: body.take(body.left()).toString() })
    }
    groups.push(group)
  }
  assert.equal(r.left(), 0, 'bytes left after the last group')
  return groups
}

describe('push — the default', () => {
  it('is the JSON request', async () => {
    await withQueen({}, () => queued(), async (queen, hits) => {
      const res = await queen.queue('orders').partition('p1').push({ data: { a: 1 } })

      assert.equal(res[0].status, 'queued')
      const hit = pushes(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/json')
      const body = JSON.parse(hit.raw.toString())
      assert.deepEqual(Object.keys(body), ['items'])
      assert.deepEqual(body.items[0].payload, { a: 1 })
    })
  })
})

describe('push — binaryPush', () => {
  it('sends the binary body, each item with its transaction id', async () => {
    await withQueen({ binaryPush: true }, () => queued(), async (queen, hits) => {
      const res = await queen.queue('orders').partition('p1').push({ data: { a: 1 }, transactionId: TXN })

      assert.equal(res[0].status, 'queued')
      const hit = pushes(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/x-queen-push')
      assert.equal(hit.raw.subarray(0, 5).toString('latin1'), 'QPB1\x04')
      assert.deepEqual(plainGroups(hit.raw.subarray(5), true), [
        { queue: 'orders', partition: 'p1', items: [{ txn: TXN, payload: '{"a":1}' }] }
      ])
    })
  })

  it('keeps the order of the items: consecutive items of one partition share a group', async () => {
    const item = (partition, payload) => ({ queue: 'q', partition, payload, transactionId: `t${payload}` })
    const bytes = Buffer.from(await binaryPushBody([item('a', 1), item('a', 2), item('b', 3), item('a', 4)], false))

    assert.equal(bytes.subarray(0, 5).toString('latin1'), 'QPB1\x04')
    assert.deepEqual(
      plainGroups(bytes.subarray(5), true).map(g => [g.partition, g.items.map(i => i.txn + '=' + i.payload)]),
      [['a', ['t1=1', 't2=2']], ['b', ['t3=3']], ['a', ['t4=4']]]
    )
  })

  it('carries payloads that are not objects, and non-ASCII text', async () => {
    await withQueen({ binaryPush: true }, () => queued(5), async (queen, hits) => {
      await queen.queue('ordini').partition('città').push([
        { data: 'testo "con" virgolette\n' }, { data: [1, 2] }, { data: 7 }, { data: null }, { data: { è: '€' } }
      ])

      const [group] = plainGroups(pushes(hits)[0].raw.subarray(5), true)
      assert.equal(group.partition, 'città')
      assert.deepEqual(group.items.map(i => JSON.parse(i.payload)), ['testo "con" virgolette\n', [1, 2], 7, null, { è: '€' }])
    })
  })

  it('leaves an item with a trace id to the JSON request', async () => {
    await withQueen({ binaryPush: true }, () => queued(), async (queen, hits) => {
      await queen.queue('orders').push({ data: { a: 1 }, traceId: TRACE })

      const hit = pushes(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/json')
      assert.equal(JSON.parse(hit.raw.toString()).items[0].traceId, TRACE)
    })
  })

  it('sends a buffered push as the binary body too', async () => {
    const { flags, body } = await bodyOf(async (queen) => {
      await queen.queue('orders').partition('p1').buffer({ messageCount: 10, timeMillis: 60000 }).push({ data: { a: 1 } })
      await queen.flushAllBuffers()
    })
    assert.equal(flags, 4)
    assert.equal(body[0].items[0].payload, '{"a":1}')
  })

  /** Runs `use` on a binaryPush client and returns the one push it sent, decoded. */
  async function bodyOf(use) {
    let sent = null
    await withQueen({ binaryPush: true }, () => queued(2), async (queen, hits) => {
      await use(queen)
      assert.equal(pushes(hits).length, 1)
      sent = pushes(hits)[0]
    })
    assert.equal(sent.raw.subarray(0, 4).toString('latin1'), 'QPB1')
    return { flags: sent.raw[4], body: plainGroups(sent.raw.subarray(5), true) }
  }
})

describe('push — binaryPush with pushCompression', () => {
  it('sends each partition as one zstd frame of stored frames, ids minted here', async () => {
    await withQueen({ binaryPush: true, pushCompression: true }, () => queued(3), async (queen, hits) => {
      await queen.queue('orders').partition('p1').push([{ data: { a: 1 }, transactionId: TXN }, { data: { a: 2 } }, { data: 'x' }])

      const hit = pushes(hits)[0]
      assert.equal(hit.headers['content-type'], 'application/x-queen-push')
      assert.equal(hit.raw.subarray(0, 5).toString('latin1'), 'QPB1\x08')
      const [group, ...rest] = framedGroups(hit.raw.subarray(5))
      assert.equal(rest.length, 0)
      assert.equal(group.queue, 'orders')
      assert.equal(group.partition, 'p1')
      assert.equal(group.count, 3)
      assert.deepEqual(group.frames.map(f => f.payload), ['{"a":1}', '{"a":2}', '"x"'])
      assert.equal(group.frames[0].txn, TXN)
      for (const frame of group.frames) {
        assert.equal(frame.flags, 0)
        assert.equal(frame.id[6] >> 4, 7, 'the message id is a version-7 UUID')
        assert.equal(frame.id[8] & 0xc0, 0x80)
        assert.equal(frame.txn.length, 36)
      }
      assert.equal(new Set(group.frames.map(f => f.id.toString('hex'))).size, 3)
    })
  })

  it('gives a message without a transaction id the text of its id', async () => {
    const bytes = Buffer.from(await encodePushFrames([{ queue: 'q', partition: 'p', payloads: ['1'] }]))
    const [{ frames: [frame] }] = framedGroups(bytes.subarray(5))
    const hex = frame.id.toString('hex')
    assert.equal(frame.txn, `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`)
  })

  it('writes the content size in each zstd frame: the broker asks for it', async () => {
    const bytes = Buffer.from(await encodePushFrames([{ queue: 'q', partition: 'p', payloads: ['{"a":1}'] }]))
    const r = reader(bytes.subarray(5))
    r.u32(); r.text16(); r.text16(); r.u32()
    const frame = r.take(r.u32())
    assert.equal(frame.readUInt32LE(0), 0xfd2fb528, 'a zstd frame')
    // Frame header descriptor: the single-segment bit, or a content-size
    // field of 2 bytes or more, says the frame carries its content size.
    assert.ok((frame[4] & 0x20) !== 0 || (frame[4] >> 6) !== 0)
  })
})

describe('encodePushBinary — flags', () => {
  const batches = [{ queue: 'q', partition: 'p', payloads: ['1'] }]

  it('asks for the counts answer with flag 2', async () => {
    const bytes = Buffer.from(await encodePushBinary(batches, { counts: true }))
    assert.equal(bytes.subarray(0, 5).toString('latin1'), 'QPB1\x02')
    assert.equal(Buffer.from(await encodePushFrames(batches, { counts: true }))[4], 8 | 2)
  })

  it('compresses the whole body with flag 1, its raw length in front', async () => {
    const plain = Buffer.from(await encodePushBinary(batches))
    const packed = Buffer.from(await encodePushBinary(batches, { compress: true }))
    assert.equal(packed.subarray(0, 5).toString('latin1'), 'QPB1\x01')
    assert.equal(packed.readUInt32LE(5), plain.length - 5)
    assert.deepEqual(zstdDecompressSync(packed.subarray(9)), plain.subarray(5))
  })

  it('refuses transaction ids that do not match the payloads', async () => {
    await assert.rejects(
      encodePushBinary([{ queue: 'q', partition: 'p', payloads: ['1', '2'], transactionIds: ['t'] }]),
      { message: '1 transaction ids for 2 payloads' }
    )
  })
})
