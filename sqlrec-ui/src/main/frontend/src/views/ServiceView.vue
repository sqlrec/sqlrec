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
        <CodeBlock title="# DDL" :code="selectedService.ddl" language="sql" />
      </div>
      <div v-if="selectedService?.yaml" class="code-section">
        <CodeBlock title="# K8s YAML" :code="selectedService.yaml" language="yaml" />
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
import { encodePathSegment } from '../utils/url.js'

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
    const response = await fetch(`/ui/api/services/${encodePathSegment(item.name)}`)
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
  height: calc(100vh - var(--header-height));
  height: calc(100svh - var(--header-height));
}

.detail-wrapper {
  flex: 1;
  min-width: 0;
  background: var(--page-bg);
  overflow-y: auto;
  text-align: left;
}

.code-section {
  width: 100%;
  max-width: var(--content-max-width);
  margin: 0 auto;
  padding: 0 var(--page-padding) var(--page-padding);
}

</style>
