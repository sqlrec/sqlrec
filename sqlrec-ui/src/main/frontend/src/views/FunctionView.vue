<template>
  <div class="view-container">
    <Sidebar 
      title="函数列表" 
      :items="functions"
      :selected-id="selectedFunction?.id"
      @select="handleSelect"
    />
    <div class="detail-container">
      <template v-if="selectedFunction">
        <div class="dag-section">
          <DagPanel 
            :function-name="selectedFunction.name" 
            @node-click="handleNodeClick"
          />
        </div>
      </template>
    </div>
    <NodeDrawer
      :visible="drawerVisible"
      :node-data="drawerNodeData"
      @close="drawerVisible = false"
      @navigate-function="handleNavigateFunction"
    />
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DagPanel from '../components/DagPanel.vue'
import NodeDrawer from '../components/NodeDrawer.vue'

const route = useRoute()
const router = useRouter()

const selectedFunction = ref(null)
const functions = ref([])
const drawerVisible = ref(false)
const drawerNodeData = ref(null)

const fetchFunctions = async () => {
  try {
    const response = await fetch('/ui/api/functions')
    if (response.ok) {
      functions.value = await response.json()
      if (route.params.id) {
        const func = functions.value.find(f => f.name.toUpperCase() === route.params.id.toUpperCase())
        if (func) {
          selectedFunction.value = func
        }
      }
    }
  } catch (error) {
    console.error('Failed to fetch functions:', error)
  }
}

const handleSelect = async (item) => {
  selectedFunction.value = item
  drawerVisible.value = false
  router.push({ name: 'FunctionDetail', params: { id: item.name } })
}

const handleNodeClick = (nodeData) => {
  drawerNodeData.value = nodeData
  drawerVisible.value = true
}

const handleNavigateFunction = async (functionName) => {
  drawerVisible.value = false
  const func = functions.value.find(f => f.name.toUpperCase() === functionName.toUpperCase())
  if (func) {
    await handleSelect(func)
  }
}

watch(() => route.params.id, (newId) => {
  if (newId && functions.value.length > 0) {
    const func = functions.value.find(f => f.name.toUpperCase() === newId.toUpperCase())
    if (func) {
      selectedFunction.value = func
      drawerVisible.value = false
    }
  }
})

onMounted(() => {
  fetchFunctions()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}

.detail-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: #fafafa;
  overflow: hidden;
}

.dag-section {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
</style>
