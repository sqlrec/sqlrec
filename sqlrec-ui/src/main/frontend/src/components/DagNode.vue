<template>
  <div class="dag-node" :style="{ background: nodeColor }">
    <Handle type="target" :position="Position.Left" />
    <span class="node-title">{{ data.label }}</span>
    <Handle type="source" :position="Position.Right" />
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { Handle, Position } from '@vue-flow/core'

const props = defineProps({
  data: {
    type: Object,
    required: true
  },
  selected: {
    type: Boolean,
    default: false
  },
  zScore: {
    type: Number,
    default: 0
  }
})

const nodeColor = computed(() => {
  const z = props.zScore
  
  const clampedZ = Math.max(-2, Math.min(2, z))
  
  // 单色绿色阶：浅绿 → 深绿
  // Z = -2 → 浅绿 (最快) → lightness = 92%
  // Z = 0  → 中绿 (平均) → lightness = 62%
  // Z = +2 → 深绿 (最慢) → lightness = 32%
  const lightness = 62 - clampedZ * 15
  
  return `hsl(140, 50%, ${lightness}%)`
})
</script>

<style scoped>
.dag-node {
  padding: 8px 12px;
  border-radius: 6px;
  width: 180px;
  font-size: 12px;
  position: relative;
}

.node-title {
  font-weight: 600;
  font-size: 12px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: block;
}
</style>
