<template>
  <div class="view-container">
    <Sidebar 
      title="模型列表" 
      :items="models"
      @select="handleSelect"
    />
    <DetailPanel :item="selectedModel" />
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'

const selectedModel = ref(null)
const models = ref([])

const fetchModels = async () => {
  try {
    const response = await fetch('/ui/api/models')
    if (response.ok) {
      models.value = await response.json()
    }
  } catch (error) {
    console.error('Failed to fetch models:', error)
  }
}

const handleSelect = async (item) => {
  try {
    const response = await fetch(`/ui/api/models/${item.name}`)
    if (response.ok) {
      const tableData = await response.json()
      selectedModel.value = {
        ...item,
        tableData
      }
    }
  } catch (error) {
    console.error('Failed to fetch model details:', error)
  }
}

onMounted(() => {
  fetchModels()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}
</style>
