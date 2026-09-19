<template>
  <Transition name="drawer">
    <div v-if="visible" class="drawer-mask" @click.self="close">
      <div class="drawer-panel">
        <div class="drawer-header">
          <span class="drawer-title">{{ nodeData?.label || 'Node Detail' }}</span>
          <button class="drawer-close" @click="close">&times;</button>
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
                    <span
                      v-for="fn in dependencyFunctions"
                      :key="fn"
                      class="function-link"
                      @click="onNavigateFunction(fn)"
                    >{{ fn }}</span>
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
import { computed } from 'vue'
import CodeBlock from './CodeBlock.vue'

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

<style scoped>
.drawer-mask {
  position: fixed;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: 1000;
  background: rgba(31, 41, 55, 0.24);
  display: flex;
  justify-content: flex-end;
}

.drawer-panel {
  width: 520px;
  max-width: 90vw;
  height: 100%;
  background: var(--surface);
  border-left: 1px solid var(--border);
  box-shadow: -8px 0 24px rgba(31, 41, 55, 0.08);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.drawer-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 52px;
  padding: 12px 18px;
  background: var(--surface-subtle);
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}

.drawer-title {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-h);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.drawer-close {
  width: 28px;
  height: 28px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: none;
  border-radius: var(--radius-control);
  font-size: 22px;
  color: var(--text-muted);
  cursor: pointer;
  padding: 0;
  line-height: 1;
}

.drawer-close:hover {
  background: var(--brand-soft);
  color: var(--text-h);
}

.drawer-body {
  flex: 1;
  overflow-y: auto;
  padding: 0;
  background: var(--page-bg);
  text-align: left;
}

.props-table {
  width: 100%;
  border-collapse: collapse;
  background: var(--surface);
  font-size: 13px;
}

.props-card {
  margin: 16px 16px 0;
  overflow: hidden;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
}

.props-table tr {
  border-bottom: 1px solid var(--border);
}

.props-table tr:last-child {
  border-bottom: 0;
}

.prop-key {
  padding: 10px 18px;
  color: var(--text-muted);
  white-space: nowrap;
  width: 130px;
  vertical-align: top;
}

.prop-val {
  padding: 10px 18px 10px 0;
  color: var(--text-h);
  word-break: break-all;
}

.function-link {
  color: var(--brand);
  cursor: pointer;
  text-decoration: underline;
  margin-right: 12px;
  display: inline-block;
}

.function-link:hover {
  color: var(--brand-hover);
}

.code-section {
  padding: 12px 16px 0;
}

.props-card + .code-section {
  padding-top: 12px;
}

.code-section:last-child {
  padding-bottom: 16px;
}

.drawer-body::-webkit-scrollbar {
  width: 6px;
}

.drawer-body::-webkit-scrollbar-track {
  background: transparent;
}

.drawer-body::-webkit-scrollbar-thumb {
  background: #d0d0d0;
  border-radius: 3px;
}

.drawer-body::-webkit-scrollbar-thumb:hover {
  background: #b0b0b0;
}

.drawer-enter-active,
.drawer-leave-active {
  transition: all 0.25s ease;
}

.drawer-enter-active .drawer-panel,
.drawer-leave-active .drawer-panel {
  transition: transform 0.25s ease;
}

.drawer-enter-from,
.drawer-leave-to {
  background: transparent;
}

.drawer-enter-from .drawer-panel,
.drawer-leave-to .drawer-panel {
  transform: translateX(100%);
}
</style>
