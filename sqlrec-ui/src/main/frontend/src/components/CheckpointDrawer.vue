<template>
  <Transition name="drawer">
    <div v-if="visible" class="drawer-mask" @click.self="close">
      <div ref="panelRef" class="drawer-panel" role="dialog" aria-modal="true" aria-labelledby="checkpoint-drawer-title">
        <div class="drawer-header">
          <span id="checkpoint-drawer-title" class="drawer-title">{{ checkpointData?.checkpointName || checkpointName || 'Checkpoint Detail' }}</span>
          <button class="drawer-close" type="button" aria-label="Close checkpoint details" @click="close">&times;</button>
        </div>
        <div class="drawer-body">
          <div v-if="loading" class="drawer-state" role="status">Loading checkpoint details...</div>
          <div v-else-if="error" class="drawer-state" role="alert">
            {{ error }} <button type="button" @click="fetchCheckpointDetail">Retry</button>
          </div>
          <template v-else-if="checkpointData">
            <div class="props-card">
              <table class="props-table">
                <tbody>
                  <tr>
                    <td class="prop-key">Model Name</td>
                    <td class="prop-val">{{ checkpointData.modelName }}</td>
                  </tr>
                  <tr>
                    <td class="prop-key">Type</td>
                    <td class="prop-val">{{ checkpointData.checkpointType || '-' }}</td>
                  </tr>
                  <tr>
                    <td class="prop-key">Status</td>
                    <td class="prop-val">{{ checkpointData.status || '-' }}</td>
                  </tr>
                  <tr>
                    <td class="prop-key">Created At</td>
                    <td class="prop-val">{{ checkpointData.createdAt || '-' }}</td>
                  </tr>
                  <tr>
                    <td class="prop-key">Updated At</td>
                    <td class="prop-val">{{ checkpointData.updatedAt || '-' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-if="checkpointData.ddl" class="code-section">
              <CodeBlock title="DDL" :code="checkpointData.ddl" language="sql" max-height="none" />
            </div>
            <div v-if="checkpointData.modelDdl" class="code-section">
              <CodeBlock title="Model DDL" :code="checkpointData.modelDdl" language="sql" max-height="none" />
            </div>
            <div v-if="checkpointData.yaml" class="code-section">
              <CodeBlock title="YAML" :code="checkpointData.yaml" language="yaml" max-height="none" />
            </div>
          </template>
          <div v-else class="drawer-state">No details available</div>
        </div>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { onUnmounted, watch, ref } from 'vue'
import CodeBlock from './CodeBlock.vue'
import { encodePathSegment } from '../utils/url.js'
import { useDrawerBehavior } from '../composables/useDrawerBehavior.js'
import { fetchJson } from '../utils/http.js'

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
const loading = ref(false)
const error = ref('')
const panelRef = ref(null)
let controller
useDrawerBehavior(() => props.visible, panelRef, () => emit('close'))

const fetchCheckpointDetail = async () => {
  if (!props.modelName || !props.checkpointName) return
  controller?.abort()
  controller = new AbortController()
  const currentController = controller
  checkpointData.value = null
  loading.value = true
  error.value = ''
  try {
    const data = await fetchJson(
      `/ui/api/models/${encodePathSegment(props.modelName)}/checkpoints/${encodePathSegment(props.checkpointName)}`,
      currentController.signal
    )
    if (!currentController.signal.aborted) checkpointData.value = data
  } catch (reason) {
    if (!currentController.signal.aborted) error.value = 'Checkpoint Failed to load details. Please retry.'
  } finally {
    if (controller === currentController) loading.value = false
  }
}

watch(() => [props.visible, props.modelName, props.checkpointName], () => {
  if (props.visible && props.modelName && props.checkpointName) {
    fetchCheckpointDetail()
  } else {
    controller?.abort()
    checkpointData.value = null
    loading.value = false
  }
}, { immediate: true })
onUnmounted(() => controller?.abort())

const close = () => {
  emit('close')
}
</script>

<style scoped src="./drawer.css"></style>

<style scoped>
.drawer-state { margin: 16px; padding: 24px; border: 1px solid var(--border); border-radius: var(--radius-card); background: var(--surface); color: var(--text); text-align: center; }
.drawer-state button { margin-left: 8px; border: 0; background: transparent; color: var(--brand); cursor: pointer; }
</style>
