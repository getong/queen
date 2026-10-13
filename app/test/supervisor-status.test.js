import { test } from 'node:test'
import assert from 'node:assert/strict'
import { STATUS_FORMAT, decodeSupervisorSlot, readSupervisorPage, supervisorObservation, supervisorObservations, supervisorPart, supervisorGroupPrefix, validSupervisorGroup } from '../src/composables/supervisorStatus.js'
import { supervisorGroups, filterSupervisorGroups } from '../src/composables/supervisorGroups.js'

const NOW = 1_800_000_000
const ID = 'a'.repeat(32)
const WRITE = 'b'.repeat(32)
const SLOT = `orders-production/${ID}`
const pool = (overrides = {}) => ({ supervisor: 'main', queue: 'orders', running: 4, desired: 4, draining: 0,
  depth: 200, depth_available: true, ready: true, healthy: true, restart_state: 'closed', restart_failures: 0,
  process_cost_per_worker: 2, reserved_processes: 8, renewal_helpers_reserved: 4, ...overrides })
const document = (overrides = {}) => ({ schema: 'queen.supervisor.status/v1', instance_id: ID, hostname: 'worker-1', pid: 42,
  updated_at_epoch: NOW - 5, state: 'running', paused: false, stopping: false, engine: 'php', ready: true, capacity_satisfied: true,
  configuration: { process_limit: 8, heartbeat_timeout: 30 }, draining: 0,
  process_budget: { limit: 8, used: 8, available: 0, active_worker_processes: 4, draining_worker_processes: 0, renewal_helpers_reserved: 4 },
  pool_status: [pool()], ...overrides })
// The publishers slice UTF-8 bytes, not JSON characters. The fixture uses the
// published PHP/Rust head+chunk fields, including per-generation write ids.
function wire(doc, slot = SLOT, chunkSize = 45_000) {
  const bytes = Buffer.from(JSON.stringify(doc))
  const rows = [{ key: `${slot}/head`, value: { format: STATUS_FORMAT, write: WRITE, chunks: Math.ceil(bytes.length / chunkSize), bytes: bytes.length }, expired: false }]
  for (let offset = 0, index = 0; offset < bytes.length; offset += chunkSize, index++) rows.push({
    key: `${slot}/chunk/${String(index).padStart(4, '0')}`, value: { write: WRITE, index, data: bytes.subarray(offset, offset + chunkSize).toString('base64') }, expired: false,
  })
  return rows.sort((a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key)))
}
const decode = rows => decodeSupervisorSlot(rows, supervisorPart(`${SLOT}/head`))
const observe = (overrides = {}, unconfirmed = false) => supervisorObservation(decode(wire(document(overrides))), NOW * 1000, unconfirmed)

const consumer = (overrides = {}) => ({ schema: 'queen.consumer.status/v1', instance_id: ID, engine: 'go',
  hostname: 'worker-1', pid: 42, execution_model: 'goroutines', state: 'running', updated_at_epoch: NOW - 5,
  started_at_epoch: NOW - 100, uptime_seconds: 95, configuration: { heartbeat_timeout: 30 },
  pool_status: [{ name: 'consumer', queue: 'orders', namespace: null, task: null, consumer_group: 'billing',
    running: 4, desired: 4, busy: 1, completed: 12, failed: 2, last_completed_at_epoch: NOW - 10, oldest_inflight_seconds: 20 }], ...overrides })
const observeConsumer = (raw = consumer(), unconfirmed = false) => supervisorObservation(decode(wire(raw)), NOW * 1000, unconfirmed)

const replica = (row, id, group = row.group) => ({ ...row, instance: id, slot: `${group}/${id}`, group, hostname: `worker-${id}` })

test('application cards merge replicas for every SDK and PHP/Rust supervisors, deduplicating queues', () => {
  const observations = ['js', 'go', 'python', 'rust', 'cpp', 'php', 'cli'].map(engine => observeConsumer(consumer({ engine })))
  observations.push(observe({ engine: 'php' }), observe({ engine: 'rust' }))
  for (const row of observations) {
    const [group] = supervisorGroups([replica(row, 'a'), replica(row, 'b')])
    assert.equal(group.count, 2)
    assert.equal(group.workers, 8)
    assert.equal(group.desired, 8)
    assert.equal(group.queueCount, 1)
    assert.equal(group.tone, 'good')
    assert.equal(group.counts.good, 2)
    assert.deepEqual(group.engines, [row.engine])
  }
})

test('reporting capacity stays visible without masking critical pools or overdue publications', () => {
  const healthy = replica(observe(), 'a')
  const critical = replica(observe({ pool_status: [pool({ running: 0 })] }), 'b')
  const overdue = replica(observe({ updated_at_epoch: NOW - 31 }), 'c')
  const [group] = supervisorGroups([healthy, critical, overdue])
  assert.equal(group.tone, 'bad')
  assert.equal(group.rows[0].slot, critical.slot)
  assert.deepEqual(group.counts, { good: 1, warn: 1, bad: 1, idle: 0 })
  assert.equal(group.workers, 4)
  assert.equal(group.desired, 8)
  assert.equal(group.reportingCount, 2)
  assert.equal(group.missingWorkers, 4)
  assert.equal(group.poolCount, 2)
  assert.equal(group.affectedPools, 1)
  assert.equal(group.oldestAge, 5)
  assert.equal(group.queues[0].tone, 'bad')
  assert.equal(supervisorGroups([healthy, overdue])[0].tone, 'warn')
})

test('partial reads and failed refreshes cannot produce green complete totals', () => {
  const [partial] = supervisorGroups([observe()], { partial: true })
  assert.equal(partial.tone, 'warn')
  assert.equal(partial.label, 'Partial view')
  assert.equal(partial.partial, true)
  const [failed] = supervisorGroups([observe({}, true)])
  assert.equal(failed.tone, 'warn')
  assert.equal(failed.workers, null)
  assert.equal(failed.detail, 'Current state unconfirmed')
})

test('filters retain all replicas and their problems when a healthy host or engine matches', () => {
  const healthy = replica(observeConsumer(consumer()), 'a')
  const overdue = replica(observe({ updated_at_epoch: NOW - 31 }), 'b')
  const groups = supervisorGroups([healthy, overdue, replica(observe(), 'c', 'other-production')])
  for (const options of [{ search: 'worker-a' }, { filter: 'engine:go' }, { filter: 'attention' }, { filter: 'stale' }]) {
    const filtered = filterSupervisorGroups(groups, options)
    assert.equal(filtered.length, 1)
    assert.equal(filtered[0].count, 2)
    assert.equal(filtered[0].tone, 'warn')
  }
  assert.equal(filterSupervisorGroups(groups, { group: 'other-production' })[0].count, 1)
  assert.equal(filterSupervisorGroups(groups, { search: 'no-match' }).length, 0)
})

test('different deployment groups remain separate even with the same host and queues', () => {
  const groups = supervisorGroups([observe(), { ...observe(), group: 'orders-staging' }])
  assert.equal(groups.length, 2)
  assert.ok(groups.every(group => group.count === 1 && group.workers === 4))
})

test('missing allocations are summed per replica and cannot be offset by surplus workers', () => {
  const reduced = replica(observe({ pool_status: [pool({ running: 3 })] }), 'a')
  const surplus = replica(observe({ pool_status: [pool({ running: 5 })] }), 'b')
  const [group] = supervisorGroups([reduced, surplus])
  assert.equal(group.workers, 8)
  assert.equal(group.desired, 8)
  assert.equal(group.missingWorkers, 1)
  assert.equal(group.tone, 'warn')
})

test('completed instances stay inspectable without invalidating their replacement capacity', () => {
  const stopped = replica(observeConsumer(consumer({ state: 'stopped' })), 'b')
  assert.equal(supervisorGroups([stopped])[0].tone, 'idle')
  assert.equal(supervisorGroups([stopped])[0].workers, null)
  const [mixed] = supervisorGroups([replica(observe(), 'a'), stopped])
  assert.equal(mixed.tone, 'good')
  assert.equal(mixed.workers, 4)
  assert.equal(mixed.desired, 4)
  assert.equal(mixed.reportingCount, 1)
  assert.equal(mixed.stoppedCount, 1)
  assert.equal(mixed.poolCount, 1)
  assert.equal(mixed.affectedPools, 0)
  assert.equal(mixed.draining, 0)
  assert.equal(mixed.headroom, 0)
  assert.equal(mixed.processOnly, true)
  assert.equal(mixed.rows.at(-1).slot, stopped.slot)
  assert.equal(mixed.counts.idle, 1)
})

test('a replaced pod final report stays stopped after its heartbeat and publication expire', () => {
  const replacement = replica(observe({ engine: 'rust' }), 'new')
  for (const engine of ['php', 'rust']) for (const expired of [false, true]) {
    const publication = wire(document({ engine, state: 'stopped', stopping: true, ready: false,
      updated_at_epoch: NOW - 300, pool_status: [pool({ running: 0, desired: 0, ready: false })] }))
    publication.forEach(row => { row.expired = expired })
    const stopped = replica(supervisorObservation(decode(publication), NOW * 1000), 'old')
    assert.equal(stopped.fresh, false)
    assert.equal(stopped.terminal, true)
    assert.equal(stopped.label, 'Stopped')
    const [group] = supervisorGroups([stopped, replacement])
    assert.equal(group.tone, 'good')
    assert.equal(group.count, 2)
    assert.equal(group.workers, 4)
    assert.equal(group.desired, 4)
    assert.equal(group.poolCount, 1)
    assert.equal(group.affectedPools, 0)
    assert.equal(group.oldestAge, 5)
    assert.deepEqual(group.queues, [{ name: 'orders', running: 4, desired: 4, tone: 'good' }])
    assert.equal(filterSupervisorGroups([group], { filter: 'attention' }).length, 0)
    assert.equal(filterSupervisorGroups([group], { filter: 'stale' }).length, 0)
    assert.equal(filterSupervisorGroups([group], { filter: 'stopped' }).length, 1)
    assert.equal(filterSupervisorGroups([group], { search: 'worker-old' }).length, 1)
    const [alone] = supervisorGroups([stopped])
    assert.equal(alone.tone, 'idle')
    assert.equal(alone.label, 'Stopped')
    assert.equal(alone.workers, null)
    assert.equal(alone.reportingCount, 0)
    assert.equal(alone.queues[0].tone, 'idle')
  }
})

test('stopped SDK consumers remain final across heartbeat and KV expiry', () => {
  for (const expired of [false, true]) {
    const publication = wire(consumer({ state: 'stopped', updated_at_epoch: NOW - 300, started_at_epoch: NOW - 600,
      pool_status: [{ ...consumer().pool_status[0], last_completed_at_epoch: NOW - 310 }] }))
    publication.forEach(row => { row.expired = expired })
    const stopped = replica(supervisorObservation(decode(publication), NOW * 1000), 'old')
    assert.equal(stopped.label, 'Stopped')
    assert.equal(stopped.terminal, true)
    const [group] = supervisorGroups([replica(observeConsumer(), 'new'), stopped])
    assert.equal(group.tone, 'good')
    assert.equal(group.workers, 4)
    assert.equal(group.busy, 1)
    assert.equal(group.handlerFailures, 2)
    assert.equal(group.poolCount, 1)
  }
})

test('an overdue or expired running pod remains uncertain while fresh capacity is reported', () => {
  for (const expired of [false, true]) {
    const publication = wire(document({ updated_at_epoch: NOW - 300 }))
    publication.forEach(row => { row.expired = expired })
    const old = replica(supervisorObservation(decode(publication), NOW * 1000), 'old')
    const [group] = supervisorGroups([old, replica(observe(), 'new')])
    assert.equal(old.terminal, false)
    assert.equal(old.label, expired ? 'Publication expired' : 'Heartbeat overdue')
    assert.equal(group.tone, 'warn')
    assert.equal(group.workers, 4)
    assert.equal(group.desired, 4)
    assert.equal(group.reportingCount, 1)
    assert.equal(group.stoppedCount, 0)
    assert.equal(group.poolCount, 1)
    assert.deepEqual(group.queues, [{ name: 'orders', running: 4, desired: 4, tone: 'warn' }])
    assert.equal(filterSupervisorGroups([group], { filter: 'stale' }).length, 1)
    const [alone] = supervisorGroups([old])
    assert.equal(alone.tone, 'warn')
    assert.equal(alone.workers, null)
    assert.equal(alone.desired, null)
    assert.equal(alone.poolCount, null)
    assert.equal(alone.queues[0].running, null)
  }
})

test('queues from final reports do not inflate the replacement inventory', () => {
  const stopped = replica(observe({ state: 'stopped', pool_status: [pool({ queue: 'retired' })] }), 'old')
  const [group] = supervisorGroups([replica(observe(), 'new'), stopped])
  assert.equal(group.queueCount, 1)
  assert.deepEqual(group.queues.map(queue => queue.name), ['orders'])
  const overdue = replica(observe({ updated_at_epoch: NOW - 300, pool_status: [pool({ queue: 'unconfirmed' })] }), 'missing')
  const [uncertain] = supervisorGroups([replica(observe(), 'new'), overdue, stopped])
  assert.equal(uncertain.queueCount, 2)
  assert.deepEqual(uncertain.queues.find(queue => queue.name === 'unconfirmed'), { name: 'unconfirmed', running: null, desired: null, tone: 'warn' })
})

test('paused, starting and terminating instances cannot be mistaken for completed shutdowns', () => {
  for (const state of ['paused', 'starting', 'terminating']) {
    for (const updated_at_epoch of [NOW - 5, NOW - 300]) {
      const row = replica(observe({ state, updated_at_epoch }), 'old')
      const [group] = supervisorGroups([replica(observe(), 'new'), row])
      assert.equal(row.terminal, false)
      assert.equal(group.tone, 'warn')
      assert.equal(group.workers, 4)
      assert.equal(group.stoppedCount, 0)
    }
  }
})

test('failed refreshes and partial inventories remain unconfirmed after a replacement', () => {
  const stopped = replica(observe({ state: 'stopped', updated_at_epoch: NOW - 300 }), 'old')
  const [partial] = supervisorGroups([replica(observe(), 'new'), stopped], { partial: true })
  assert.equal(partial.tone, 'warn')
  assert.equal(partial.label, 'Partial view')
  assert.equal(partial.workers, 4)
  for (const row of [observe({ state: 'stopped' }, true), observeConsumer(consumer({ state: 'stopped' }), true)]) {
    assert.equal(row.label, 'Current state unconfirmed')
    assert.equal(row.terminal, false)
    const [failed] = supervisorGroups([replica(observe({}, true), 'new'), replica(row, 'old')])
    assert.equal(failed.tone, 'warn')
    assert.equal(failed.workers, null)
    assert.equal(failed.reportingCount, 0)
  }
})

test('unreadable telemetry and dynamic scopes do not invent queue counts', () => {
  const raw = consumer()
  raw.pool_status[0].queue = null
  raw.pool_status[0].namespace = 'invoices'
  const [scoped] = supervisorGroups([observeConsumer(raw)])
  assert.equal(scoped.queueCount, 0)
  assert.equal(scoped.scopedConsumers, true)
  const unreadable = supervisorObservation({ slot: SLOT, group: 'orders-production', instance: ID, reason: 'Missing chunk' })
  const [unknown] = supervisorGroups([unreadable])
  assert.equal(unknown.queueCount, null)
  assert.equal(unknown.workers, null)
  assert.equal(unknown.tone, 'warn')
})

test('consumer tasks report progress without inventing process budgets or broker readiness', () => {
  for (const engine of ['js', 'go', 'python', 'rust', 'cpp', 'php', 'cli']) {
    const row = observeConsumer(consumer({ engine }))
    assert.equal(row.label, 'Consumers running')
    assert.equal(row.consumer, true)
    assert.equal(row.workers, 4)
    assert.equal(row.budget, null)
    assert.equal(row.readiness, null)
    assert.equal(row.pools[0].busy, 1)
    assert.equal(row.pools[0].failed, 2)
    assert.equal(row.pools[0].oldest, 20)
  }
})

test('consumer loss, stale heartbeats and malformed counts never appear healthy', () => {
  const raw = consumer()
  raw.pool_status[0].running = 3
  assert.equal(observeConsumer(raw).label, 'Consumer capacity reduced')
  raw.pool_status[0].busy = 4
  assert.equal(observeConsumer(raw).label, 'Incomplete telemetry')
  assert.equal(observeConsumer(consumer({ updated_at_epoch: NOW - 31 })).label, 'Heartbeat overdue')
  assert.equal(observeConsumer(consumer(), true).fresh, false)
  assert.equal(observeConsumer(consumer({ state: 'stopped' })).label, 'Stopped')
  assert.equal(observeConsumer(consumer({ pool_status: [] })).label, 'Incomplete telemetry')
  const rows = wire(consumer()); rows[0].expired = true
  assert.equal(supervisorObservation(decode(rows), NOW * 1000).label, 'Publication expired')
})

test('consumer batch counters and scope remain separate from queue metrics and secrets', () => {
  const raw = consumer({ credentials: 'secret' })
  raw.pool_status[0].queue = null
  raw.pool_status[0].namespace = 'invoices'
  raw.pool_status[0].task = 'send'
  const row = observeConsumer(raw)
  assert.equal(row.queueCount, 0)
  assert.equal(row.pools[0].queue, 'invoices / send')
  assert.equal(row.pools[0].queueName, null)
  assert.equal(JSON.stringify(row).includes('secret'), false)
})

test('PHP and Rust status documents decode, including UTF-8 split between chunks', () => {
  for (const engine of ['php', 'rust']) {
    const raw = document({ engine, hostname: 'worker-à-東京', padding: 'à'.repeat(30_000) })
    assert.deepEqual(decode(wire(raw)).document, raw)
    assert.equal(observe({ engine }).label, 'At desired capacity')
  }
})

test('future engines use the same schema without being mistaken for PHP or Rust', () => {
  for (const engine of ['nodejs', 'python', 'go', 'custom-engine']) {
    const row = observe({ engine })
    assert.equal(row.engine, engine)
    assert.equal(row.label, 'At desired capacity')
  }
  assert.equal(observe({ engine: '\ninvalid' }).engine, null)
})

test('group names occupy one case-sensitive segment and cannot use the coordination root', () => {
  for (const group of ['pmsintool', 'pmsintool-staging', 'Orders.production_1', 'a'.repeat(255)]) {
    assert.equal(validSupervisorGroup(group), true)
    assert.equal(supervisorGroupPrefix(group), `${group}/`)
  }
  assert.equal(supervisorGroupPrefix(''), '')
  for (const group of ['', 'coordination', '/orders', 'orders/', 'orders/production', '.', '_orders', 'a b', 'città', 'a\n', 'a'.repeat(256)]) {
    assert.equal(validSupervisorGroup(group), false, group)
    if (group) assert.throws(() => supervisorGroupPrefix(group))
  }
})

test('exact group scans separate similar names and the same instance id in different groups', async () => {
  const all = ['pmsintool', 'pmsintool-staging', 'pmsintool/production', 'coordination'].flatMap(group => wire(document(), `${group}/${ID}`))
    .sort((a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key)))
  const list = async body => ({ rows: all.filter(row => !body.prefix || row.key.startsWith(body.prefix)), truncated: false })
  const page = await readSupervisorPage(list, { namespace: 'queen-supervisor', group: 'pmsintool' })
  assert.deepEqual(page.entries.map(entry => entry.group), ['pmsintool'])
  const unfiltered = await readSupervisorPage(list, { namespace: 'queen-supervisor' })
  assert.equal(supervisorObservations(unfiltered.entries, NOW * 1000).length, 3)
  const legacy = supervisorPart(`pmsintool/production/${ID}/head`)
  assert.equal(legacy.group, 'pmsintool/production')
  assert.equal(legacy.legacy, true)
  assert.equal(supervisorPart(`pmsintool/${ID}/head`).legacy, false)
})

test('a legacy slot is supported but a per-instance slot cannot impersonate another instance', () => {
  const rows = wire(document(), 'orders-production')
  assert.equal(decodeSupervisorSlot(rows, supervisorPart('orders-production/head')).document.instance_id, ID)
  assert.equal(decode(wire(document({ instance_id: 'c'.repeat(32) }))).document, null)
  assert.equal(supervisorPart('replicas/123/metadata'), null)
})

test('mixed generations, missing chunks, oversized heads and invalid encodings never produce a status', () => {
  const cases = [
    rows => { rows[0].value.write = 'c'.repeat(32) },
    rows => { rows.shift() },
    rows => { rows.at(-1).value.bytes++ },
    rows => { rows.at(-1).value.bytes = 1_048_577 },
    rows => { rows.at(-1).value.chunks = 25 },
    rows => { rows.at(-1).value.write = { toString: 'invalid' } },
    rows => { rows[0].value.data = 'not base64!' },
    rows => { rows[0].value.index = 1 },
    rows => { rows[0].expired = undefined },
  ]
  for (const mutate of cases) { const rows = wire(document()); mutate(rows); assert.equal(decode(rows).document, null) }
  const unknown = wire(document()); unknown.at(-1).value.format = 'queen.supervisor.remote-status/v2'
  assert.match(decode(unknown).reason, /Unsupported/)
})

test('expired chunks invalidate freshness even when the heartbeat is recent; leftover generations are ignored', () => {
  const rows = wire(document())
  rows.push({ key: `${SLOT}/chunk/0023`, value: { write: 'c'.repeat(32), index: 23, data: 'AAAA' }, expired: true })
  assert.equal(supervisorObservation(decode(rows), NOW * 1000).fresh, true)
  rows[0].expired = true
  assert.equal(supervisorObservation(decode(rows), NOW * 1000).label, 'Publication expired')
})

test('a page boundary re-reads the whole trailing slot and never joins two snapshots', async () => {
  const second = `orders-production/${'c'.repeat(32)}`
  const firstRows = wire(document()), secondRows = wire(document({ instance_id: 'c'.repeat(32) }), second)
  const calls = []
  const listing = async (body) => {
    calls.push(body)
    return body.after ? { rows: wire(document({ instance_id: 'c'.repeat(32), hostname: 'new-heartbeat' }), second), truncated: false, nextAfter: null }
      : { rows: [...firstRows, secondRows[0]], truncated: true, nextAfter: secondRows[0].key }
  }
  const first = await readSupervisorPage(listing, { namespace: 'queen-supervisor' })
  assert.equal(first.entries.length, 1)
  assert.equal(first.after, `${SLOT}/head`)
  const next = await readSupervisorPage(listing, { namespace: 'queen-supervisor', after: first.after })
  assert.equal(next.entries[0].document.hostname, 'new-heartbeat')
  assert.equal(next.after, null)
  assert.deepEqual(calls[0], { namespace: 'queen-supervisor', limit: 100, includeExpired: true })
})

test('the reader ignores coordination data, recognizes broken publications, and rejects unsafe cursors', async () => {
  const rows = [{ key: 'coordination/pool/instance', value: { alive: true } }, ...wire(document())]
  const page = await readSupervisorPage(async () => ({ rows, truncated: false }), { namespace: 'queen-supervisor' })
  assert.equal(page.entries.length, 1)
  const broken = await readSupervisorPage(async () => ({ rows: wire(document()).slice(0, 1), truncated: false }), { namespace: 'queen-supervisor' })
  assert.equal(broken.entries[0].document, null)
  for (const invalid of [{ rows: [], truncated: true, nextAfter: 'x' }, { rows: wire(document()).reverse(), truncated: false }, { rows: wire(document()), truncated: true, nextAfter: 'wrong' }]) {
    await assert.rejects(readSupervisorPage(async () => invalid, { namespace: 'queen-supervisor' }), /pagination/)
  }
})

test('freshness is distinct from running state, readiness and the success of the latest read', () => {
  assert.equal(observe({ updated_at_epoch: NOW - 31 }).label, 'Heartbeat overdue')
  assert.equal(observe({ updated_at_epoch: NOW + 30 }).label, 'Heartbeat unconfirmed')
  assert.equal(observe({ configuration: { process_limit: 8 } }).label, 'Heartbeat unconfirmed')
  assert.equal(observe({ state: 'paused', paused: true }).label, 'Paused')
  assert.equal(observe({ state: 'stopped', stopping: true }).label, 'Stopped')
  assert.equal(observe({}, true).label, 'Current state unconfirmed')
  assert.equal(observe({}, true).fresh, false)
  assert.equal(observe({ ready: false }).label, 'Readiness not confirmed')
})

test('backlog is not an alert and unknown counts never become zero workers', () => {
  assert.equal(observe({ pool_status: [pool({ depth: 1_000_000 })] }).label, 'At desired capacity')
  assert.equal(observe({ pool_status: [pool({ running: undefined, desired: undefined })] }).label, 'Incomplete telemetry')
  assert.equal(observe({ pool_status: [pool({ running: undefined })] }).workers, null)
  assert.equal(observe({ pool_status: [pool({ running: 0, desired: 0 })] }).label, 'Pending work, no workers')
})

test('process-headroom diagnosis needs a consistent budget and worker cost', () => {
  assert.equal(observe({ pool_status: [pool({ desired: 6 })] }).label, 'No process headroom')
  assert.equal(observe({ pool_status: [pool({ desired: 6, reserved_processes: 7 })] }).label, 'Below desired capacity')
  assert.equal(observe({ pool_status: [pool({ desired: 6 })], process_budget: { limit: 8, available: 0 } }).label, 'Below desired capacity')
  assert.equal(observe({ pool_status: [pool({ desired: 6, restart_state: 'backoff' })] }).label, 'Restart backoff')
})

test('invalid or duplicated pools and inconsistent readiness cannot be called healthy', () => {
  assert.equal(observe({ pool_status: [pool(), pool()] }).label, 'Incomplete telemetry')
  assert.equal(observe({ pool_status: [null] }).label, 'Incomplete telemetry')
  assert.equal(observe({ pool_status: [] }).label, 'Incomplete telemetry')
  assert.equal(observe({ pool_status: [pool({ healthy: true, restart_failures: 1 })] }).label, 'Readiness not confirmed')
  assert.equal(observe({ pool_status: [pool({ ready: true, depth_available: false })] }).label, 'Incomplete telemetry')
  assert.equal(observe({ configuration: { heartbeat_timeout: 30, process_limit: 0 } }).label, 'Incomplete telemetry')
})

test('only the normalized read model reaches the UI, without arbitrary configuration or credentials', () => {
  const row = observe({ secret: 'must-not-appear', configuration: { heartbeat_timeout: 30, process_limit: 8, bearer_token: 'must-not-appear' } })
  assert.equal(JSON.stringify(row).includes('must-not-appear'), false)
})

test('instance overviews count distinct queues, all pools and their reported workers', () => {
  const row = observe({ configuration: { process_limit: 32, heartbeat_timeout: 30 }, draining: 1, pool_status: [
    pool(), pool({ supervisor: 'overflow', running: 2, desired: 4, draining: 1 }),
    pool({ queue: 'payments', running: 3, desired: 3 }),
  ] })
  assert.equal(row.queueCount, 2)
  assert.equal(row.poolCount, 3)
  assert.equal(row.workers, 9)
  assert.equal(row.desired, 11)
  assert.equal(row.draining, 1)
  assert.equal(row.affectedPools, 1)
})

test('pool issue totals stay unknown when health or the pool inventory cannot be confirmed', () => {
  for (const raw of [{ updated_at_epoch: NOW - 31 }, { state: 'paused' }, { pool_status: [pool(), null] }, { pool_status: [pool(), pool()] }]) {
    assert.equal(observe(raw).affectedPools, null)
  }
  assert.equal(observe({}, true).affectedPools, null)
  const partial = observe({ pool_status: [pool(), null] })
  assert.equal(partial.poolCount, null)
  assert.equal(partial.queueCount, null)
  assert.equal(partial.workers, null)
  assert.equal(observe({ draining: 3 }).draining, null)
})

test('large supervisor overviews aggregate the complete inventory rather than the visible page', () => {
  const pools = Array.from({ length: 256 }, (_, index) => pool({ supervisor: `pool-${index}`, queue: `queue-${index % 60}`, running: 2, desired: 2 }))
  pools[255] = { ...pools[255], running: 0, restart_state: 'open' }
  const row = observe({ configuration: { process_limit: 1024, heartbeat_timeout: 30 }, pool_status: pools })
  assert.equal(row.queueCount, 60)
  assert.equal(row.poolCount, 256)
  assert.equal(row.workers, 510)
  assert.equal(row.desired, 512)
  assert.equal(row.affectedPools, 1)
  assert.equal(row.pools[0].name, 'pool-255')
})

test('legacy and per-instance copies do not double count workers during upgrades', () => {
  const current = decode(wire(document()))
  const legacy = decodeSupervisorSlot(wire(document(), 'orders-production'), supervisorPart('orders-production/head'))
  for (const entries of [[current, legacy], [legacy, current]]) {
    const rows = supervisorObservations(entries, NOW * 1000)
    assert.equal(rows.length, 1)
    assert.equal(rows[0].slot, SLOT)
    assert.equal(rows[0].workers, 4)
  }
})

test('shortfall counts missing allocations without offsetting a surplus in another pool', () => {
  const row = observe({ pool_status: [pool({ running: 2 }), pool({ queue: 'payments', running: 6 })] })
  assert.equal(row.workers, row.desired)
  assert.equal(row.missingWorkers, 2)
  assert.equal(row.capacity, false)
  assert.equal(row.readiness, true)
  assert.equal(observe({ pool_status: [pool({ running: undefined })] }).missingWorkers, null)
})

test('readiness and capacity have independent meanings and become unconfirmed on stale reads', () => {
  const raw = { pool_status: [pool({ desired: 6, capacity_satisfied: false })], capacity_satisfied: false }
  const row = observe(raw)
  assert.equal(row.readiness, true)
  assert.equal(row.capacity, false)
  assert.equal(row.pools[0].readiness, true)
  assert.equal(row.pools[0].capacity, false)
  for (const stale of [observe({ ...raw, updated_at_epoch: NOW - 60 }), observe(raw, true)]) {
    assert.equal(stale.readiness, null)
    assert.equal(stale.capacity, null)
  }
  assert.equal(observe({ ready: undefined }).readiness, null)
  assert.equal(observe({ capacity_satisfied: undefined }).capacity, null)
  assert.equal(observe().pools[0].capacity, null)
})

test('pool configuration joins by name and queue, rejects ambiguous matches and exposes only selected fields', () => {
  const settings = { name: 'main', queues: ['orders'], connection: 'queen', consumer_group: 'orders-workers', balance: 'auto', strategy: 'time',
    min_processes: 2, max_processes: 8, timeout: 60, retry_after: 90, lease_renewal: true, tries: 0, memory: 128, bearer_token: 'secret-value' }
  const read = supervisors => observe({ configuration: { process_limit: 8, heartbeat_timeout: 30, supervisors } }).pools[0].configuration
  assert.deepEqual(read([settings]), { connection: 'queen', consumerGroup: 'orders-workers', balance: 'auto', strategy: 'time', min: 2, max: 8,
    timeout: 60, lease: 90, leaseRenewal: true, tries: 0, memoryLimit: 128 })
  assert.equal(read([settings, settings]), null)
  assert.equal(read([{ ...settings, queues: ['another-queue'] }]), null)
  const invalid = read([{ ...settings, min_processes: 20, timeout: '60', lease_renewal: 'true' }])
  assert.equal(invalid.min, null)
  assert.equal(invalid.max, null)
  assert.equal(invalid.timeout, null)
  assert.equal(invalid.leaseRenewal, null)
})

test('runtime metadata is optional, bounded and taken from the heartbeat instead of browser elapsed time', () => {
  const row = observe({ started_at_epoch: NOW - 7200, uptime_seconds: 7195, engine_version: '0.8.0', client_version: 'v1.2.3' })
  assert.equal(row.startedAt, (NOW - 7200) * 1000)
  assert.equal(row.uptime, 7195)
  assert.equal(row.engineVersion, '0.8.0')
  assert.equal(row.clientVersion, 'v1.2.3')
  assert.equal(row.pid, 42)
  assert.equal(observe().uptime, null)
  assert.equal(observe().engineVersion, null)
  const invalid = observe({ started_at_epoch: NOW + 60, uptime_seconds: '120', engine_version: 'bad\nversion', client_version: 'a'.repeat(65) })
  for (const key of ['startedAt', 'uptime', 'engineVersion', 'clientVersion']) assert.equal(invalid[key], null)
  assert.equal(invalid.label, 'At desired capacity')
})

test('dashboard diagnostics aggregate SDK counters and PHP process slots without inventing unsupported fields', () => {
  const sdk = observeConsumer()
  const [consumers] = supervisorGroups([replica(sdk, 'a'), replica(sdk, 'b')])
  assert.equal(consumers.busy, 2)
  assert.equal(consumers.handlerFailures, 4)
  assert.equal(consumers.draining, null)
  assert.equal(consumers.headroom, null)
  assert.equal(consumers.poolCount, 2)
  assert.equal(consumers.affectedPools, 0)
  assert.deepEqual(consumers.queues, [{ name: 'orders', running: 8, desired: 8, tone: 'good' }])
  const [processes] = supervisorGroups([replica(observe(), 'a'), replica(observe(), 'b')])
  assert.equal(processes.busy, null)
  assert.equal(processes.handlerFailures, null)
  assert.equal(processes.draining, 0)
  assert.equal(processes.headroom, 0)
  assert.deepEqual(processes.queues, [{ name: 'orders', running: 8, desired: 8, tone: 'good' }])
})

test('stale consumer inventories are excluded from reporting subtotals and retain their warning', () => {
  const stale = consumer({ updated_at_epoch: NOW - 60, pool_status: [{ ...consumer().pool_status[0], last_completed_at_epoch: NOW - 65 }] })
  const [group] = supervisorGroups([replica(observeConsumer(), 'a'), replica(observeConsumer(stale), 'b')])
  assert.equal(group.tone, 'warn')
  assert.equal(group.reportingCount, 1)
  assert.equal(group.busy, 1)
  assert.equal(group.handlerFailures, 2)
  assert.equal(group.affectedPools, 0)
  assert.deepEqual(group.queues, [{ name: 'orders', running: 4, desired: 4, tone: 'warn' }])
})

test('incomplete reporting inventories cannot produce confirmed runtime and allocation totals', () => {
  const healthy = replica(observeConsumer(), 'a')
  for (const invalid of [consumer({ pool_status: [consumer().pool_status[0], null] }), consumer({ pool_status: [] })]) {
    const [group] = supervisorGroups([healthy, replica(observeConsumer(invalid), 'b')])
    assert.equal(group.busy, null)
    assert.equal(group.handlerFailures, null)
    assert.equal(group.affectedPools, null)
    assert.equal(group.queues[0].running, null)
    assert.equal(group.queues[0].desired, null)
    assert.equal(group.queues[0].tone, 'warn')
  }
})
