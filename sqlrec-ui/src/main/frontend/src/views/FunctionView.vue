<template>
  <div class="view-container">
    <Sidebar 
      title="函数列表" 
      :items="functions"
      @select="handleSelect"
    />
    <DetailPanel :item="selectedFunction" />
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'

const selectedFunction = ref(null)
const functions = ref([])

const fetchFunctions = async () => {
  try {
    const response = await fetch('/ui/api/functions')
    if (response.ok) {
      functions.value = await response.json()
    }
  } catch (error) {
    console.error('Failed to fetch functions:', error)
  }
}

const handleSelect = async (item) => {
  try {
    const response = await fetch(`/ui/api/functions/${item.name}`)
    if (response.ok) {
      const tableData = await response.json()
      selectedFunction.value = {
        ...item,
        tableData
      }
    }
  } catch (error) {
    console.error('Failed to fetch function details:', error)
  }
}

onMounted(() => {
  fetchFunctions()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}
</style>
