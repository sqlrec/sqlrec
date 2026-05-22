<template>
  <div class="view-container">
    <Sidebar 
      title="API列表" 
      :items="apis"
      :selected-id="selectedApi?.id"
      @select="handleSelect"
    />
    <div class="detail-wrapper">
      <DetailPanel :item="selectedApi" />
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

const selectedApi = ref(null)
const apis = ref([])

const fetchApis = async () => {
  try {
    const response = await fetch('/ui/api/apis')
    if (response.ok) {
      apis.value = await response.json()
      if (route.params.id) {
        const api = apis.value.find(a => a.name === route.params.id)
        if (api) {
          await loadApiDetail(api)
        }
      }
    }
  } catch (error) {
    console.error('Failed to fetch apis:', error)
  }
}

const loadApiDetail = async (item) => {
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

const handleSelect = async (item) => {
  await loadApiDetail(item)
  router.push({ name: 'ApiDetail', params: { id: item.name } })
}

watch(() => route.params.id, async (newId) => {
  if (newId && apis.value.length > 0) {
    const api = apis.value.find(a => a.name === newId)
    if (api) {
      await loadApiDetail(api)
    }
  }
})

onMounted(() => {
  fetchApis()
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
