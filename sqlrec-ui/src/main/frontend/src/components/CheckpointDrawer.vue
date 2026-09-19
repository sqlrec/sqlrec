<template>
  <Transition name="drawer">
    <div v-if="visible" class="drawer-mask" @click.self="close">
      <div class="drawer-panel">
        <div class="drawer-header">
          <span class="drawer-title">{{ checkpointData?.checkpointName || 'Checkpoint Detail' }}</span>
          <button class="drawer-close" @click="close">&times;</button>
        </div>
        <div class="drawer-body">
          <div class="props-card">
            <table class="props-table">
              <tbody>
                <tr>
                  <td class="prop-key">Model Name</td>
                  <td class="prop-val">{{ checkpointData?.modelName }}</td>
                </tr>
                <tr>
                  <td class="prop-key">Type</td>
                  <td class="prop-val">{{ checkpointData?.checkpointType || '-' }}</td>
                </tr>
                <tr>
                  <td class="prop-key">Status</td>
                  <td class="prop-val">{{ checkpointData?.status || '-' }}</td>
                </tr>
                <tr>
                  <td class="prop-key">Created At</td>
                  <td class="prop-val">{{ checkpointData?.createdAt || '-' }}</td>
                </tr>
                <tr>
                  <td class="prop-key">Updated At</td>
                  <td class="prop-val">{{ checkpointData?.updatedAt || '-' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-if="checkpointData?.ddl" class="code-section">
            <CodeBlock title="DDL" :code="checkpointData.ddl" language="sql" max-height="none" />
          </div>
          <div v-if="checkpointData?.modelDdl" class="code-section">
            <CodeBlock title="Model DDL" :code="checkpointData.modelDdl" language="sql" max-height="none" />
          </div>
          <div v-if="checkpointData?.yaml" class="code-section">
            <CodeBlock title="YAML" :code="checkpointData.yaml" language="yaml" max-height="none" />
          </div>
        </div>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { computed, watch, ref } from 'vue'
import CodeBlock from './CodeBlock.vue'
import { encodePathSegment } from '../utils/url.js'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  },
  modelName: {
    type: String,
    default: ''
  },
  checkpointName: {
    type: String,
    default: ''
  }
})

const emit = defineEmits(['close'])

const checkpointData = ref(null)

const fetchCheckpointDetail = async () => {
  if (!props.modelName || !props.checkpointName) return
  
  try {
    const response = await fetch(
      `/ui/api/models/${encodePathSegment(props.modelName)}/checkpoints/${encodePathSegment(props.checkpointName)}`
    )
    if (response.ok) {
      checkpointData.value = await response.json()
    }
  } catch (error) {
    console.error('Failed to fetch checkpoint detail:', error)
  }
}

watch(() => [props.visible, props.modelName, props.checkpointName], () => {
  if (props.visible && props.modelName && props.checkpointName) {
    fetchCheckpointDetail()
  }
}, { immediate: true })

const close = () => {
  emit('close')
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
