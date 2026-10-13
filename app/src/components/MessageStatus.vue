<template>
  <span class="message-status" :class="`message-status-${appearance.tone}`">
    <span class="message-status-dot" aria-hidden="true" />
    {{ appearance.label }}
  </span>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({ status: { type: String, default: '' } })
const states = {
  pending: { label: 'Pending', tone: 'pending' },
  processing: { label: 'Processing', tone: 'processing' },
  completed: { label: 'Completed', tone: 'completed' },
  dead_letter: { label: 'Dead letter', tone: 'failed' },
  failed: { label: 'Failed', tone: 'failed' },
}
const appearance = computed(() => Object.hasOwn(states, props.status)
  ? states[props.status]
  : { label: props.status || 'Unknown', tone: 'unknown' })
</script>

<style scoped>
.message-status { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-low); white-space: nowrap; }
.message-status-dot { flex: none; width: 6px; height: 6px; border-radius: 50%; background: currentColor; }
.message-status-pending { color: var(--warn-400); }
.message-status-processing { color: var(--text-hi); }
.message-status-processing .message-status-dot { background: var(--warn-400); box-shadow: 0 0 0 3px var(--warn-glow); }
.message-status-completed { color: var(--ok-500); }
.message-status-failed { color: var(--ember-400); }
</style>
