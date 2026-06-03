<template>
  <Transition name="drawer">
    <div v-if="visible" class="drawer-mask" @click.self="close">
      <div class="drawer-panel">
        <div class="drawer-header">
          <span class="drawer-title">{{ nodeData?.label || 'Node Detail' }}</span>
          <button class="drawer-close" @click="close">&times;</button>
        </div>
        <div class="drawer-body">
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
              <tr v-if="nodeData?.type === 'function' && nodeData?.dependencyFunction">
                <td class="prop-key">Call Function</td>
                <td class="prop-val link" @click="onNavigateFunction">{{ nodeData.dependencyFunction }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="nodeData?.sql" class="code-section">
            <div class="section-title">SQL</div>
            <CodeBlock :code="nodeData.sql" language="sql" />
          </div>
          <div v-if="cacheTableSchema" class="code-section">
            <div class="section-title">Cache Table Schema</div>
            <CodeBlock :code="cacheTableSchema" language="sql" />
          </div>
          <div v-if="nodeData?.logicalPlan" class="code-section">
            <div class="section-title">Logical Plan</div>
            <CodeBlock :code="nodeData.logicalPlan" />
          </div>
          <div v-if="nodeData?.physicalPlan" class="code-section">
            <div class="section-title">Physical Plan</div>
            <CodeBlock :code="nodeData.physicalPlan" />
          </div>
          <div v-if="nodeData?.javaExpression" class="code-section">
            <div class="section-title">Java Expression</div>
            <CodeBlock :code="nodeData.javaExpression" language="java" />
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

const close = () => {
  emit('close')
}

const onNavigateFunction = () => {
  emit('navigate-function', props.nodeData.dependencyFunction)
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
  background: rgba(0, 0, 0, 0.15);
  display: flex;
  justify-content: flex-end;
}

.drawer-panel {
  width: 560px;
  max-width: 90vw;
  height: 100%;
  background: #fff;
  box-shadow: -4px 0 16px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.drawer-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  border-bottom: 1px solid #eee;
  flex-shrink: 0;
}

.drawer-title {
  font-size: 16px;
  font-weight: 600;
  color: #333;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.drawer-close {
  background: none;
  border: none;
  font-size: 22px;
  color: #999;
  cursor: pointer;
  padding: 0 4px;
  line-height: 1;
}

.drawer-close:hover {
  color: #333;
}

.drawer-body {
  flex: 1;
  overflow-y: auto;
  padding: 0;
  text-align: left;
}

.props-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.props-table tr {
  border-bottom: 1px solid #f0f0f0;
}

.props-table tr:last-child {
  border-bottom: 1px solid #eee;
}

.prop-key {
  padding: 10px 20px;
  color: #888;
  white-space: nowrap;
  width: 130px;
  vertical-align: top;
}

.prop-val {
  padding: 10px 20px 10px 0;
  color: #333;
  word-break: break-all;
}

.prop-val.link {
  color: #1890ff;
  cursor: pointer;
  text-decoration: underline;
}

.prop-val.link:hover {
  color: #40a9ff;
}

.code-section {
  padding: 16px 20px;
  border-top: 1px solid #eee;
}

.section-title {
  font-size: 13px;
  font-weight: 600;
  color: #666;
  margin-bottom: 8px;
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
