<template>
  <span class="queue-status" :class="`queue-status-${tone}`">
    <span class="g" :class="glyph" aria-hidden="true" />
    <span>{{ label }}</span>
  </span>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  label: { type: String, required: true },
  severity: { type: String, default: 'unknown' },
})

const tones = { ok: 'healthy', ice: 'idle', warn: 'warning', bad: 'critical' }
const tone = computed(() => Object.hasOwn(tones, props.severity) ? tones[props.severity] : 'unknown')
const glyph = computed(() => ({ healthy: 'ok', warning: 'warn', critical: 'bad' }[tone.value] || 'idle'))
</script>

<style scoped>
.queue-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 7px;
  border: 1px solid var(--bd);
  border-radius: var(--r-chip);
  color: var(--text-low);
  font-size: 12px;
  font-weight: 500;
  line-height: 1.4;
  white-space: nowrap;
}
.queue-status-healthy { color: var(--text-mid); background: var(--ok-glow); border-color: var(--ok-bd); }
.queue-status-healthy .g.ok { background: currentColor; }
.queue-status .g.idle { border-color: currentColor; }
.queue-status-warning { color: var(--warn-400); background: var(--warn-glow); border-color: var(--warn-bd); }
.queue-status-critical { color: var(--ember-400); background: var(--ember-glow); border-color: var(--ember-bd); }
.queue-status-unknown { border-style: dashed; border-color: var(--bd-hi); }
</style>
