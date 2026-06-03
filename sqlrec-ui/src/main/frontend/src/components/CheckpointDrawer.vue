<template>
  <Transition name="drawer">
    <div v-if="visible" class="drawer-mask" @click.self="close">
      <div class="drawer-panel">
        <div class="drawer-header">
          <span class="drawer-title">{{ checkpointData?.checkpointName || 'Checkpoint Detail' }}</span>
          <button class="drawer-close" @click="close">&times;</button>
        </div>
        <div class="drawer-body">
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
          <div v-if="checkpointData?.ddl" class="code-section">
            <div class="section-title">DDL</div>
            <CodeBlock :code="checkpointData.ddl" language="sql" />
          </div>
          <div v-if="checkpointData?.modelDdl" class="code-section">
            <div class="section-title">Model DDL</div>
            <CodeBlock :code="checkpointData.modelDdl" language="sql" />
          </div>
          <div v-if="checkpointData?.yaml" class="code-section">
            <div class="section-title">YAML</div>
            <CodeBlock :code="checkpointData.yaml" language="yaml" />
          </div>
        </div>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { computed, watch, ref } from 'vue'
import CodeBlock from './CodeBlock.vue'

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
    const response = await fetch(`/ui/api/models/${props.modelName}/checkpoints/${props.checkpointName}`)
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
