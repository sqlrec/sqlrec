<template>
  <div class="view-container">
    <Sidebar search-label="functions" :items="functions" :selected-id="selectedFunction?.name"
      :loading="loading" :error="error" @select="handleSelect" @retry="fetchFunctions" />
    <main class="detail-container">
      <ContentState v-if="loading" message="Loading functions..." />
      <ContentState v-else-if="error" :message="error" action="Retry" @action="fetchFunctions" />
      <ContentState v-else-if="!route.params.id" plain message="Select a function from the sidebar to view its execution graph" />
      <ContentState v-else-if="!selectedFunction" message="Function not found." />
      <DagPanel v-else :function-name="selectedFunction.name" @node-click="handleNodeClick" />
    </main>
    <NodeDrawer :visible="drawerVisible" :node-data="drawerNodeData"
      @close="drawerVisible = false" @navigate-function="handleNavigateFunction" />
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import ContentState from '../components/ContentState.vue'
import DagPanel from '../components/DagPanel.vue'
import NodeDrawer from '../components/NodeDrawer.vue'

const route = useRoute()
const router = useRouter()
const functions = ref([])
const loading = ref(false)
const error = ref('')
const drawerVisible = ref(false)
const drawerNodeData = ref(null)
const selectedFunction = computed(() => functions.value.find(item =>
  item.name.toUpperCase() === String(route.params.id || '').toUpperCase()
) || null)
let listController

const fetchFunctions = async () => {
  listController?.abort()
  const controller = new AbortController()
  listController = controller
  loading.value = true
  error.value = ''
  try {
    const response = await fetch('/ui/api/functions', { signal: controller.signal })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const data = await response.json()
    if (!controller.signal.aborted) functions.value = data
  } catch (reason) {
    if (!controller.signal.aborted) error.value = 'Failed to load functions. Please retry.'
  } finally {
    if (listController === controller) loading.value = false
  }
}

const handleSelect = item => router.push({ name: 'FunctionDetail', params: { id: item.name } })
const handleNodeClick = nodeData => {
  drawerNodeData.value = nodeData
  drawerVisible.value = true
}
const handleNavigateFunction = functionName => {
  const item = functions.value.find(fn => fn.name.toUpperCase() === functionName.toUpperCase())
  if (item) handleSelect(item)
  drawerVisible.value = false
}

watch(() => route.params.id, () => { drawerVisible.value = false })
onMounted(fetchFunctions)
onUnmounted(() => listController?.abort())
</script>

<style scoped>
.view-container { display: flex; height: calc(100svh - var(--header-height)); }
.detail-container { position: relative; flex: 1; min-width: 0; min-height: 0; display: flex; flex-direction: column; overflow: hidden; background: var(--page-bg); }
@media (max-width: 720px) { .view-container { flex-direction: column; } }
</style>
