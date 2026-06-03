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
      <div v-if="selectedService?.ddl" class="code-section">
        <div class="section-header">
          <span class="section-title"># DDL</span>
        </div>
        <CodeBlock :code="selectedService.ddl" language="sql" />
      </div>
      <div v-if="selectedService?.yaml" class="code-section">
        <div class="section-header">
          <span class="section-title"># K8s YAML</span>
        </div>
        <CodeBlock :code="selectedService.yaml" language="yaml" />
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import CodeBlock from '../components/CodeBlock.vue'

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
      const data = await response.json()
      selectedService.value = {
        ...item,
        tableData: data.tableData,
        yaml: data.yaml || null,
        ddl: data.ddl || null
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
