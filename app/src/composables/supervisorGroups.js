// Group only within the current source. Publisher language and hostname are
// metadata; the publication group is the application's deployment identity.
export function supervisorTone(row) {
  if (row.terminal) return 'idle'
  if (!row.fresh) return 'warn'
  if (row.severity === 'bad') return 'bad'
  if (row.severity) return 'warn'
  if (row.state !== 'running') return 'idle'
  return row.affectedPools === 0 ? 'good' : 'warn'
}

const rank = { bad: 3, warn: 2, idle: 1, good: 0 }
const sum = (rows, field) => rows.every(row => Number.isSafeInteger(row[field]))
  ? rows.reduce((total, row) => total + row[field], 0) : null

export function supervisorGroups(rows, { partial = false } = {}) {
  const groups = new Map()
  for (const row of rows) {
    if (!groups.has(row.group)) groups.set(row.group, [])
    groups.get(row.group).push(row)
  }
  return [...groups].map(([name, members]) => {
    const instances = [...members].sort((a, b) => Number(a.terminal) - Number(b.terminal) || rank[supervisorTone(b)] - rank[supervisorTone(a)]
      || b.priority - a.priority || a.slot.localeCompare(b.slot))
    const counts = { good: 0, warn: 0, bad: 0, idle: 0 }
    for (const row of instances) counts[supervisorTone(row)]++
    // Final stopped reports are retained for inspection, not current capacity.
    // A missing heartbeat alone does not establish that a pod was replaced.
    const ongoing = instances.filter(row => !row.terminal)
    const inventory = ongoing.length ? ongoing : instances
    let tone = ongoing.length ? supervisorTone(ongoing[0]) : 'idle'
    if (tone === 'idle' && counts.good) tone = 'warn'
    if (partial && tone !== 'bad') tone = 'warn'
    // These are explicitly reporting-instance subtotals. An overdue sibling
    // still raises attention, without hiding the capacity we can observe.
    const reporting = ongoing.filter(row => row.fresh && row.state === 'running')
    const current = reporting.length > 0
    const completePools = reporting.every(row => row.poolCount !== null)
    const completeInventory = inventory.every(row => row.poolCount !== null)
    const completeQueues = inventory.every(row => row.queueCount !== null)
    const queues = inventory.flatMap(row => row.pools.map(pool => row.consumer ? pool.queueName : pool.queue).filter(Boolean))
    const ages = (current ? reporting : ongoing).map(row => row.age)
    const states = new Set(inventory.map(row => row.state))
    const pools = reporting.flatMap(row => row.pools)
    const consumerOnly = (current ? reporting : inventory).every(row => row.consumer)
    const processOnly = (current ? reporting : inventory).every(row => !row.consumer)
    const queueRows = new Map()
    for (const row of inventory) for (const pool of row.pools) {
      const name = row.consumer ? pool.queueName : pool.queue
      if (!name) continue
      if (!queueRows.has(name)) queueRows.set(name, [])
      queueRows.get(name).push({ row, pool })
    }
    const queueSummaries = [...queueRows].map(([name, contributions]) => {
      const allocations = contributions.filter(({ row }) => row.fresh && row.state === 'running').map(({ pool }) => pool)
      const known = allocations.length > 0 && completePools
      return {
        name, running: known ? sum(allocations, 'running') : null,
        desired: known ? sum(allocations, 'desired') : null,
        tone: !ongoing.length ? 'idle' : allocations.some(pool => pool.severity === 'bad') ? 'bad'
          : !known || !completeInventory || allocations.length < contributions.length || allocations.some(pool => pool.severity) ? 'warn' : 'good',
      }
    }).sort((a, b) => rank[b.tone] - rank[a.tone] || a.name.localeCompare(b.name))
    return {
      name, rows: instances, count: instances.length, counts, tone, partial,
      reportingCount: reporting.length, stoppedCount: instances.length - ongoing.length,
      label: tone === 'bad' ? 'Critical issue' : tone === 'good' ? 'No issues reported'
        : partial ? 'Partial view' : tone === 'warn' ? 'Needs attention'
          : states.size === 1 ? inventory[0].label : 'Not running',
      detail: tone === 'good' ? 'All ongoing instances reporting normally'
        : partial && tone !== 'bad' ? 'More publications available · totals may be incomplete'
          : inventory[0].label,
      engines: [...new Set(instances.map(row => row.engine).filter(Boolean))].sort(),
      workers: current ? sum(reporting, 'workers') : null,
      desired: current ? sum(reporting, 'desired') : null,
      // A surplus on one replica cannot cancel a shortfall on another.
      missingWorkers: current ? sum(reporting, 'missingWorkers') : null,
      queueCount: completeQueues ? new Set(queues).size : null,
      queues: queueSummaries, consumerOnly, processOnly,
      poolCount: current ? sum(reporting, 'poolCount') : null,
      affectedPools: current ? sum(reporting, 'affectedPools') : null,
      busy: current && completePools && consumerOnly ? sum(pools, 'busy') : null,
      handlerFailures: current && completePools && consumerOnly ? sum(pools, 'failed') : null,
      draining: current && processOnly ? sum(reporting, 'draining') : null,
      headroom: current && processOnly && reporting.every(row => row.budget) ? sum(reporting.map(row => row.budget), 'available') : null,
      scopedConsumers: inventory.some(row => row.consumer && row.pools.some(pool => !pool.queueName)),
      oldestAge: ages.length && ages.every(age => age !== null && age >= -5) ? Math.max(0, ...ages) : null,
    }
  }).sort((a, b) => rank[b.tone] - rank[a.tone] || a.name.localeCompare(b.name))
}

// Filters select whole groups. Searching for a healthy host must never hide its
// unhealthy siblings. Summaries remain scoped to the reporting instances.
export function filterSupervisorGroups(groups, { search = '', filter = 'all', group = null } = {}) {
  const term = search.trim().toLocaleLowerCase()
  return groups.filter(item => {
    if (group !== null && item.name !== group) return false
    return item.rows.some(row => {
      if (term && ![row.group, row.hostname, row.instance, ...row.pools.map(pool => `${pool.name} ${pool.queue}`)]
        .join(' ').toLocaleLowerCase().includes(term)) return false
      return filter === 'all' || (filter === 'attention' && ['bad', 'warn'].includes(item.tone))
        || (filter === 'stale' && !row.fresh && !row.terminal) || (filter === 'stopped' && row.terminal) || filter === `engine:${row.engine}`
    })
  })
}
