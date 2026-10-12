/**
 * The binary transaction body (client-v2/http/pushBinary.js), asserted against
 * a server that records the bytes. What is at stake is the default (a client
 * that does not set both binaryPush and pushCompression sends the JSON bundle
 * every released broker knows) and, with the options, the order of the
 * results: the broker lists the head's acks before the pushes, and the caller
 * must get them back in the order of its own operations.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { zstdDecompressSync } from 'node:zlib'

import { Queen } from '../../client-v2/index.js'
import { encodeTransactionFrames } from '../../client-v2/http/pushBinary.js'
import { withWireServer } from './_wireServer.js'

const MESSAGE = { transactionId: 'in-1', partitionId: '7', leaseId: 'lease-1' }

const answer = (results) => () => ({ body: JSON.stringify({ transactionId: 'tx', success: true, results }) })
const transactions = (hits) => hits.filter(h => h.method === 'POST' && h.url === '/api/v1/transaction')

async function withQueen(config, respond, run) {
  await withWireServer(respond, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1, ...config })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

const framed = { binaryPush: true, pushCompression: true }

/** The head and the groups of a binary transaction body. */
function decode(raw) {
  assert.equal(raw.subarray(0, 4).toString('latin1'), 'QTB1')
  const headLength = raw.readUInt32LE(5)
  const head = raw.subarray(9, 9 + headLength).toString()
  return { flags: raw[4], head: head === '' ? null : JSON.parse(head), groups: frameGroups(raw.subarray(9 + headLength)) }
}

/**
 * Decodes `u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
 * | u32 z_len | z` and every frame of every group.
 */
function frameGroups(bytes) {
  let at = 0
  const u16 = () => { at += 2; return bytes.readUInt16LE(at - 2) }
  const u32 = () => { at += 4; return bytes.readUInt32LE(at - 4) }
  const take = (n) => { at += n; return bytes.subarray(at - n, at) }
  const groups = []
  for (let g = u32(); g > 0; g--) {
    const group = { queue: take(u16()).toString(), partition: take(u16()).toString(), txns: [], payloads: [] }
    const count = u32()
    const frames = zstdDecompressSync(take(u32()))
    let p = 0
    for (let i = 0; i < count; i++) {
      // u32 body_len | u8 0 | id[16] | u16 len, txn | payload
      const frame = frames.subarray(p + 4, p + 4 + frames.readUInt32LE(p))
      p += 4 + frame.length
      assert.equal(frame[0], 0, 'a frame carries no flags')
      assert.equal(frame[7] >> 4, 7, 'the message id is a version-7 UUID')
      assert.equal(frame[9] & 0xc0, 0x80)
      const txnLength = frame.readUInt16LE(17)
      group.txns.push(frame.subarray(19, 19 + txnLength).toString())
      group.payloads.push(frame.subarray(19 + txnLength).toString())
    }
    assert.equal(p, frames.length, 'bytes after the frames of a group')
    groups.push(group)
  }
  assert.equal(at, bytes.length, 'bytes after the last group')
  return groups
}

describe('transaction — the default', () => {
  const results = [{ index: 0, type: 'ack', success: true }, { index: 1, type: 'push', success: true }]

  for (const [label, config] of [
    ['no option set', {}],
    ['binaryPush alone', { binaryPush: true }],
    ['pushCompression alone', { pushCompression: true }]
  ]) {
    it(`is the JSON bundle with ${label}`, async () => {
      await withQueen(config, answer(results), async (queen, hits) => {
        await queen.transaction().ack(MESSAGE).queue('out').partition('p1').push([{ data: { n: 1 } }]).commit()

        const sent = transactions(hits)[0]
        assert.equal(sent.headers['content-type'], 'application/json')
        const body = JSON.parse(sent.raw.toString())
        assert.deepEqual(body.operations.map(op => op.type), ['ack', 'push'])
      })
    })
  }
})

describe('transaction — binaryPush with pushCompression', () => {
  it('sends the pushes as frames, the rest of the bundle as the JSON head', async () => {
    const results = [
      { index: 0, type: 'ack', success: true, transactionId: 'in-1' },
      { index: 1, type: 'push', success: true, transactionId: 'a' },
      { index: 2, type: 'push', success: true, transactionId: 'tx-b' },
      { index: 3, type: 'push', success: true, transactionId: 'c' }
    ]
    await withQueen(framed, answer(results), async (queen, hits) => {
      const result = await queen.transaction()
        .ack(MESSAGE)
        .queue('out').partition('p1').push([{ data: { n: 1 } }])
        .queue('out').partition('p1').push([{ data: { n: 2 }, transactionId: 'tx-b' }])
        .queue('out').partition('p2').push([{ data: 'text' }])
        .commit()

      assert.equal(result.success, true)
      assert.equal(result.results.length, 4)
      const sent = transactions(hits)[0]
      assert.equal(sent.headers['content-type'], 'application/x-queen-push')
      const { flags, head, groups } = decode(sent.raw)
      assert.equal(flags, 0)
      // The head: the ack and the lease, and no push.
      assert.deepEqual(head, {
        operations: [{ type: 'ack', transactionId: 'in-1', partitionId: '7', status: 'completed', leaseId: 'lease-1' }],
        requiredLeases: ['lease-1']
      })
      // The pushes: consecutive items of one partition share a group.
      assert.equal(groups.length, 2)
      assert.deepEqual([groups[0].queue, groups[0].partition, groups[0].payloads], ['out', 'p1', ['{"n":1}', '{"n":2}']])
      assert.equal(groups[0].txns[0].length, 36)
      assert.equal(groups[0].txns[1], 'tx-b')
      assert.deepEqual([groups[1].queue, groups[1].partition, groups[1].payloads], ['out', 'p2', ['"text"']])
    })
  })

  it('hands the results back in the order of the caller\'s operations', async () => {
    // The caller pushes, acks, pushes. The broker answers the ack first (it is
    // in the head), then the two pushes, then the rider.
    const results = [
      { index: 0, type: 'ack', success: true, transactionId: 'in-1' },
      { index: 1, type: 'push', success: true, transactionId: 'first' },
      { index: 2, type: 'push', success: true, transactionId: 'second' },
      { index: 3, type: 'kv', opIndex: 0, applied: true }
    ]
    await withQueen(framed, answer(results), async (queen, hits) => {
      const result = await queen.transaction()
        .queue('out').push([{ data: 1, transactionId: 'first' }])
        .ack(MESSAGE)
        .queue('out').push([{ data: 2, transactionId: 'second' }])
        .kv.put('saga', 'k1', { step: 1 }, { ttl: '24h' })
        .commit()

      assert.deepEqual(result.results.map(r => [r.index, r.type, r.transactionId ?? null]), [
        [0, 'push', 'first'], [1, 'ack', 'in-1'], [2, 'push', 'second'], [3, 'kv', null]
      ])
      // On the wire the two pushes are one group, in the caller's order, and
      // the rider travels in the head.
      const { head, groups } = decode(transactions(hits)[0].raw)
      assert.deepEqual(head.operations.map(op => op.type), ['ack'])
      assert.equal(head.kv.length, 1)
      assert.equal(groups.length, 1)
      assert.deepEqual(groups[0].txns, ['first', 'second'])
      assert.equal(groups[0].partition, 'Default')
    })
  })

  it('leaves the results alone when the acks come first anyway', async () => {
    const results = [
      { index: 0, type: 'ack', success: true, transactionId: 'in-1' },
      { index: 1, type: 'push', success: true, transactionId: 'a' }
    ]
    await withQueen(framed, answer(results), async (queen) => {
      const result = await queen.transaction().ack(MESSAGE).queue('out').push([{ data: 1, transactionId: 'a' }]).commit()

      assert.deepEqual(result.results, results)
    })
  })

  it('returns a rolled-back bundle as the broker answered it', async () => {
    const lost = {
      transactionId: 'tx', success: false, reason: 'kv_precondition', results: [], failedIndex: 2, kvReason: 'exists'
    }
    await withQueen(framed, () => ({ body: JSON.stringify(lost) }), async (queen) => {
      const result = await queen.transaction()
        .queue('out').push([{ data: 1 }])
        .ack(MESSAGE)
        .once('idem', 'order-1', { ttl: '24h' })
        .commit()

      assert.deepEqual(result, lost)
    })
  })

  it('leaves a bundle whose push carries a trace id to JSON', async () => {
    await withQueen(framed, answer([{ index: 0, type: 'push', success: true }]), async (queen, hits) => {
      await queen.transaction().queue('out')
        .push([{ data: 1, traceId: '0190a0a0-0000-4000-8000-000000000001' }]).commit()

      const sent = transactions(hits)[0]
      assert.equal(sent.headers['content-type'], 'application/json')
      assert.equal(JSON.parse(sent.raw.toString()).operations[0].items[0].traceId, '0190a0a0-0000-4000-8000-000000000001')
    })
  })

  it('leaves a bundle without pushes to JSON: there is nothing to frame', async () => {
    await withQueen(framed, answer([{ index: 0, type: 'ack', success: true }]), async (queen, hits) => {
      await queen.transaction().ack(MESSAGE).commit()

      assert.equal(transactions(hits)[0].headers['content-type'], 'application/json')
      assert.equal(transactions(hits)[0].raw[0], '{'.charCodeAt(0))
    })
  })
})

describe('encodeTransactionFrames', () => {
  it('asks for the counts answer with flag 2, and takes an empty head', async () => {
    const body = Buffer.from(await encodeTransactionFrames(null, [{ queue: 'q', partition: 'p', payloads: ['1'] }], { counts: true }))

    assert.equal(body.subarray(0, 9).toString('latin1'), 'QTB1\x02\x00\x00\x00\x00')
    const { head, groups } = decode(body)
    assert.equal(head, null)
    assert.deepEqual(groups.map(g => g.payloads), [['1']])
  })
})
