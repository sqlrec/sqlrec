<template>
  <div class="view-container">
    <Sidebar 
      title="模型列表" 
      :items="models"
      :selected-id="selectedModel?.id"
      @select="handleSelect"
    />
    <div class="detail-wrapper">
      <DetailPanel :item="selectedModel" />
      <div v-if="selectedModel?.ddl" class="code-section">
        <div class="section-header">
          <span class="section-title"># DDL</span>
        </div>
        <CodeBlock :code="selectedModel.ddl" language="sql" />
      </div>
      <CheckpointList 
        v-if="selectedModel"
        :model-name="selectedModel.name"
        @checkpoint-click="handleCheckpointClick"
      />
    </div>
    <CheckpointDrawer
      :visible="drawerVisible"
      :model-name="selectedModel?.name"
      :checkpoint-name="selectedCheckpointName"
      @close="drawerVisible = false"
    />
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import CheckpointList from '../components/CheckpointList.vue'
import CheckpointDrawer from '../components/CheckpointDrawer.vue'
import CodeBlock from '../components/CodeBlock.vue'

const route = useRoute()
const router = useRouter()

const selectedModel = ref(null)
const models = ref([])
const drawerVisible = ref(false)
const selectedCheckpointName = ref('')

const fetchModels = async () => {
  try {
    const response = await fetch('/ui/api/models')
    if (response.ok) {
      models.value = await response.json()
      if (route.params.id) {
        const model = models.value.find(m => m.name === route.params.id)
        if (model) {
          await loadModelDetail(model)
        }
      }
    }
  } catch (error) {
    console.error('Failed to fetch models:', error)
  }
}

const loadModelDetail = async (item) => {
  try {
    const response = await fetch(`/ui/api/models/${item.name}`)
    if (response.ok) {
      const data = await response.json()
      selectedModel.value = {
        ...item,
        tableData: data.tableData,
        ddl: data.ddl || null
      }
    }
  } catch (error) {
    console.error('Failed to fetch model details:', error)
  }
}

const handleSelect = async (item) => {
  await loadModelDetail(item)
  router.push({ name: 'ModelDetail', params: { id: item.name } })
}

const handleCheckpointClick = (checkpointName) => {
  selectedCheckpointName.value = checkpointName
  drawerVisible.value = true
}

watch(() => route.params.id, async (newId) => {
  if (newId && models.value.length > 0) {
    const model = models.value.find(m => m.name === newId)
    if (model) {
      await loadModelDetail(model)
    }
  }
})

onMounted(() => {
  fetchModels()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}

.detail-wrapper {
  flex: 1;
  background: #fafafa;
  overflow-y: auto;
  text-align: left;
}

.code-section {
  padding: 0 32px 32px 32px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px;
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.05) 0%, rgba(118, 75, 162, 0.05) 100%);
  border-radius: 8px 8px 0 0;
}

.section-title {
  font-weight: 700;
  font-size: 15px;
  color: #667eea;
}
</style>
