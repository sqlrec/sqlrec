<template>
  <div class="view-container">
    <Sidebar 
      title="服务列表" 
      :items="services"
      :selected-id="selectedService?.id"
      @select="handleSelect"
    />
    <div class="detail-wrapper">
      <DetailPanel :item="selectedService" />
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'

const route = useRoute()
const router = useRouter()

const selectedService = ref(null)
const services = ref([])

const fetchServices = async () => {
  try {
    const response = await fetch('/ui/api/services')
    if (response.ok) {
      services.value = await response.json()
      if (route.params.id) {
        const service = services.value.find(s => s.name === route.params.id)
        if (service) {
          await loadServiceDetail(service)
        }
      }
    }
  } catch (error) {
    console.error('Failed to fetch services:', error)
  }
}

const loadServiceDetail = async (item) => {
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

const handleSelect = async (item) => {
  await loadServiceDetail(item)
  router.push({ name: 'ServiceDetail', params: { id: item.name } })
}

watch(() => route.params.id, async (newId) => {
  if (newId && services.value.length > 0) {
    const service = services.value.find(s => s.name === newId)
    if (service) {
      await loadServiceDetail(service)
    }
  }
})

onMounted(() => {
  fetchServices()
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
}
</style>
