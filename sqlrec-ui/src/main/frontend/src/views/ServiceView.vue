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
        <div class="code-block" tabindex="0" @keydown="handleKeydown">
          <table class="code-table">
            <tbody>
              <tr v-for="(line, index) in ddlLines" :key="index">
                <td class="line-number">{{ index + 1 }}</td>
                <td class="line-content"><span v-html="line"></span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-if="selectedService?.yaml" class="code-section">
        <div class="section-header">
          <span class="section-title"># K8s YAML</span>
        </div>
        <div class="code-block yaml-block" tabindex="0" @keydown="handleKeydown">
          <table class="code-table">
            <tbody>
              <tr v-for="(line, index) in yamlLines" :key="index">
                <td class="line-number">{{ index + 1 }}</td>
                <td class="line-content"><span v-html="line"></span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
import yaml from 'highlight.js/lib/languages/yaml'
import 'highlight.js/styles/github.css'

hljs.registerLanguage('sql', sql)
hljs.registerLanguage('yaml', yaml)

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

const highlightedDdl = computed(() => {
  if (!selectedService.value?.ddl) return ''
  try {
    return hljs.highlight(selectedService.value.ddl, { language: 'sql' }).value
  } catch (e) {
    return selectedService.value.ddl
  }
})

const ddlLines = computed(() => {
  if (!highlightedDdl.value) return []
  return highlightedDdl.value.split('\n')
})

const highlightedYaml = computed(() => {
  if (!selectedService.value?.yaml) return ''
  try {
    return hljs.highlight(selectedService.value.yaml, { language: 'yaml' }).value
  } catch (e) {
    return selectedService.value.yaml
  }
})

const yamlLines = computed(() => {
  if (!highlightedYaml.value) return []
  return highlightedYaml.value.split('\n')
})

const handleKeydown = (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === 'a') {
    event.preventDefault()
    const codeBlock = event.currentTarget
    const selection = window.getSelection()
    const range = document.createRange()
    range.selectNodeContents(codeBlock)
    selection.removeAllRanges()
    selection.addRange(range)
  }
}
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

.code-block {
  background: #f6f8fa;
  border-radius: 0 0 8px 8px;
  border: 1px solid #e0e0e0;
  border-top: none;
  overflow: auto;
  max-height: 800px;
  cursor: text;
  outline: none;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.code-block:focus {
  border-color: #667eea;
}

.code-table {
  border-collapse: collapse;
  width: 100%;
  font-family: 'SFMono-Regular', 'Consolas', 'Liberation Mono', 'Menlo', ui-monospace, monospace;
  font-size: 12px;
  line-height: 1.5;
  text-align: left;
}

.code-table tr {
  border: none;
}

.line-number {
  padding: 0 12px;
  text-align: right;
  color: #bbb;
  user-select: none;
  vertical-align: top;
  white-space: nowrap;
  width: 1%;
  border-right: 1px solid #e8e8e8;
}

.line-content {
  padding: 0 16px;
  color: #24292e;
  white-space: pre;
  text-align: left;
}

.code-block::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

.code-block::-webkit-scrollbar-track {
  background: transparent;
}

.code-block::-webkit-scrollbar-thumb {
  background: #d0d0d0;
  border-radius: 3px;
}

.code-block::-webkit-scrollbar-thumb:hover {
  background: #b0b0b0;
}
</style>
