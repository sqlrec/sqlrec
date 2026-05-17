<template>
  <div class="view-container">
    <Sidebar 
      title="服务列表" 
      :items="services"
      @select="handleSelect"
    />
    <DetailPanel :item="selectedService" />
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'

const selectedService = ref(null)
const services = ref([])

const fetchServices = async () => {
  try {
    const response = await fetch('/ui/api/services')
    if (response.ok) {
      services.value = await response.json()
    }
  } catch (error) {
    console.error('Failed to fetch services:', error)
  }
}

const handleSelect = async (item) => {
  try {
    const response = await fetch(`/ui/api/services/${item.name}`)
    if (response.ok) {
      const tableData = await response.json()
      selectedService.value = {
        ...item,
        tableData
      }
    }
  } catch (error) {
    console.error('Failed to fetch service details:', error)
  }
}

onMounted(() => {
  fetchServices()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}
</style>
