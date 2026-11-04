<template>
  <Transition name="drawer">
    <div v-if="visible" class="drawer-mask" @click.self="close">
      <div ref="panelRef" class="drawer-panel" role="dialog" aria-modal="true" aria-labelledby="node-drawer-title">
        <div class="drawer-header">
          <span id="node-drawer-title" class="drawer-title">{{ nodeData?.label || 'Node Detail' }}</span>
          <button class="drawer-close" type="button" aria-label="Close node details" @click="close">&times;</button>
        </div>
        <div class="drawer-body">
          <div class="props-card">
            <table class="props-table">
              <tbody>
                <tr>
                  <td class="prop-key">Avg Exec Time</td>
                  <td class="prop-val">{{ formatMetric(nodeData?.avgExecTimeMs, 'ms') }}</td>
                </tr>
                <tr>
                  <td class="prop-key">Avg Data Count</td>
                  <td class="prop-val">{{ formatMetric(nodeData?.avgDataCount, '') }}</td>
                </tr>
                <tr v-if="nodeData?.type === 'function' && dependencyFunctions.length">
                  <td class="prop-key">Call Function</td>
                  <td class="prop-val">
                    <button
                      v-for="fn in dependencyFunctions"
                      :key="fn"
                      class="function-link"
                      type="button"
                      @click="onNavigateFunction(fn)"
                    >{{ fn }}</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-if="nodeData?.sql" class="code-section">
            <CodeBlock title="SQL" :code="nodeData.sql" language="sql" max-height="none" />
          </div>
          <div v-if="cacheTableSchema" class="code-section">
            <CodeBlock title="Cache Table Schema" :code="cacheTableSchema" language="sql" max-height="none" />
          </div>
          <div v-if="nodeData?.logicalPlan" class="code-section">
            <CodeBlock title="Logical Plan" :code="nodeData.logicalPlan" max-height="none" />
          </div>
          <div v-if="nodeData?.physicalPlan" class="code-section">
            <CodeBlock title="Physical Plan" :code="nodeData.physicalPlan" max-height="none" />
          </div>
          <div v-if="nodeData?.javaExpression" class="code-section">
            <CodeBlock title="Java Expression" :code="nodeData.javaExpression" language="java" max-height="none" />
          </div>
        </div>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { computed, ref } from 'vue'
import CodeBlock from './CodeBlock.vue'
import { useDrawerBehavior } from '../composables/useDrawerBehavior.js'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  },
  nodeData: {
    type: Object,
    default: null
  }
})

const emit = defineEmits(['close', 'navigate-function'])
const panelRef = ref(null)
useDrawerBehavior(() => props.visible, panelRef, () => emit('close'))

const cacheTableSchema = computed(() => {
  if (!props.nodeData?.cacheTableName || !props.nodeData?.cacheTableDataFields?.length) return null
  const fields = props.nodeData.cacheTableDataFields
    .map(f => `  ${f.name} ${f.type}`)
    .join(',\n')
  return `CREATE TABLE ${props.nodeData.cacheTableName} (\n${fields}\n)`
})

const formatMetric = (value, unit) => {
  if (value === null || value === undefined || value < 0) {
    return '-'
  }
  const formatted = Number.isInteger(value) ? value : value.toFixed(1)
  return unit ? `${formatted} ${unit}` : `${formatted}`
}

const dependencyFunctions = computed(() => {
  if (!props.nodeData?.dependencyFunction) return []
  return props.nodeData.dependencyFunction.split(',').map(s => s.trim()).filter(Boolean)
})

const close = () => {
  emit('close')
}

const onNavigateFunction = (functionName) => {
  emit('navigate-function', functionName)
}

</script>

<style scoped src="./drawer.css"></style>

<style scoped>
.function-link {
  border: 0;
  background: transparent;
  padding: 0;
  font: inherit;
  color: var(--brand);
  cursor: pointer;
  text-decoration: underline;
  margin-right: 12px;
  display: inline-block;
}

.function-link:hover {
  color: var(--brand-hover);
}

.function-link:focus-visible { outline: 2px solid var(--brand); outline-offset: 2px; }
</style>
