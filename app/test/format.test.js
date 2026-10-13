import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  formatDateTimeLocal, formatTimestamp, formatTimestampRange, formatTimestampRangeUtc,
  formatTimestampTime, formatTimestampCompact, formatTimestampUtc,
} from '../src/composables/useFormat.js'

const instant = '2026-09-03T14:03:06.000Z'
const options = { timeZone: 'Europe/Rome' }

test('a navigation window round-trips through local inputs without losing precision', () => {
  const date = new Date('2026-10-01T00:00:12.345Z')
  assert.equal(new Date(formatDateTimeLocal(date, true)).toISOString(), date.toISOString())
  assert.equal(formatDateTimeLocal(date).length, 16)
})

test('absolute timestamps use one compact browser-local format', () => {
  assert.equal(
    formatTimestamp(instant, 'en-US', options),
    'Sep 3, 2026, 04:03:06 PM',
  )
  assert.equal(
    formatTimestampTime(instant, 'en-US', options),
    '04:03:06 PM',
  )
})

test('timestamp ranges apply the same contract to both endpoints', () => {
  const range = formatTimestampRange(
    instant,
    '2026-09-03T15:03:06.000Z',
    'en-US',
    options,
  )
  assert.match(range, /04:03:06 PM →/)
  assert.match(range, /05:03:06 PM$/)
  assert.equal(
    formatTimestampRangeUtc(instant, '2026-09-03T15:03:06.000Z'),
    'UTC: 2026-09-03T14:03:06.000Z → 2026-09-03T15:03:06.000Z',
  )
})

test('message timestamps use DD/MM HH:mm:ss with zero padding and a 24-hour clock', () => {
  assert.equal(formatTimestampCompact(instant, options), '03/09 16:03:06')
  assert.equal(formatTimestampCompact('2026-10-13T07:14:03Z', options), '13/10 09:14:03')
})

test('compact timestamps follow timezone date boundaries and daylight saving time', () => {
  assert.equal(formatTimestampCompact('2026-12-31T23:00:00Z', options), '01/01 00:00:00')
  assert.equal(formatTimestampCompact('2026-12-31T23:00:00Z', { timeZone: 'UTC' }), '31/12 23:00:00')
  assert.equal(formatTimestampCompact('2026-10-25T00:30:00Z', options), '25/10 02:30:00')
  assert.equal(formatTimestampCompact('2026-10-25T01:30:00Z', options), '25/10 02:30:00')
})

test('UTC hover title preserves the exact instant', () => {
  assert.equal(formatTimestampUtc(instant), 'UTC: 2026-09-03T14:03:06.000Z')
})

test('timestamp formatting degrades safely for missing and invalid values', () => {
  assert.equal(formatTimestamp(null), '—')
  assert.equal(formatTimestamp('not-a-date'), 'not-a-date')
  assert.equal(formatTimestampUtc('not-a-date'), '')
  assert.equal(formatTimestampCompact(null), '—')
  assert.equal(formatTimestampCompact('not-a-date'), 'not-a-date')
})
