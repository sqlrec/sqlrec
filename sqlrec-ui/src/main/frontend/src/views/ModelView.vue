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
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import CheckpointList from '../components/CheckpointList.vue'
import CheckpointDrawer from '../components/CheckpointDrawer.vue'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
import 'highlight.js/styles/github.css'

hljs.registerLanguage('sql', sql)

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

const highlightedDdl = computed(() => {
  if (!selectedModel.value?.ddl) return ''
  try {
    return hljs.highlight(selectedModel.value.ddl, { language: 'sql' }).value
  } catch (e) {
    return selectedModel.value.ddl
  }
})

const ddlLines = computed(() => {
  if (!highlightedDdl.value) return []
  return highlightedDdl.value.split('\n')
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
