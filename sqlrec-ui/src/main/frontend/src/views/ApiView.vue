<template>
  <div class="view-container">
    <Sidebar 
      title="API列表" 
      :items="apis"
      @select="handleSelect"
    />
    <DetailPanel :item="selectedApi" />
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'

const selectedApi = ref(null)
const apis = ref([])

const fetchApis = async () => {
  try {
    const response = await fetch('/ui/api/apis')
    if (response.ok) {
      apis.value = await response.json()
    }
  } catch (error) {
    console.error('Failed to fetch apis:', error)
  }
}

const handleSelect = async (item) => {
  try {
    const response = await fetch(`/ui/api/apis/${item.name}`)
    if (response.ok) {
      const tableData = await response.json()
      selectedApi.value = {
        ...item,
        tableData
      }
    }
  } catch (error) {
    console.error('Failed to fetch api details:', error)
  }
}

onMounted(() => {
  fetchApis()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}
</style>
