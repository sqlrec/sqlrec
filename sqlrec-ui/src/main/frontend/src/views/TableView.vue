<template>
  <div class="view-container">
    <aside class="sidebar">
      <div class="db-list">
        <div
          v-for="db in databases"
          :key="db.name"
          class="collapse-header"
          :class="{ active: expandedDatabase === db.name }"
          @click="toggleDatabase(db.name)"
        >
          <span class="collapse-title">{{ db.name }}</span>
          <span class="collapse-arrow">{{ expandedDatabase === db.name ? '▼' : '▶' }}</span>
        </div>
      </div>
      <div v-if="expandedDatabase" class="table-list">
        <div
          v-for="table in tables"
          :key="table.name"
          class="item"
          :class="{ active: selectedTable?.name === table.name }"
          @click="handleTableSelect(table)"
        >
          <div class="item-name">{{ table.name }}</div>
        </div>
      </div>
    </aside>
    <div class="detail-wrapper">
      <DetailPanel :item="selectedTable" />
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import DetailPanel from '../components/DetailPanel.vue'

const route = useRoute()
const router = useRouter()

const databases = ref([])
const tables = ref([])
const expandedDatabase = ref(null)
const selectedTable = ref(null)

const fetchDatabases = async () => {
  try {
    const response = await fetch('/ui/api/tables/databases')
    if (response.ok) {
      databases.value = await response.json()
      if (route.params.id) {
        const parts = route.params.id.split('.')
        if (parts.length >= 1) {
          await expandDatabase(parts[0])
        }
        if (parts.length >= 2) {
          const tableName = parts.slice(1).join('.')
          const table = tables.value.find(t => t.name === tableName)
          if (table) {
            await loadTableDetail(table)
          }
        }
      }
    }
  } catch (error) {
    console.error('Failed to fetch databases:', error)
  }
}

const expandDatabase = async (dbName) => {
  expandedDatabase.value = dbName
  selectedTable.value = null
  try {
    const response = await fetch(`/ui/api/tables/${dbName}/`)
    if (response.ok) {
      tables.value = await response.json()
    }
  } catch (error) {
    console.error('Failed to fetch tables:', error)
  }
}

const toggleDatabase = async (dbName) => {
  if (expandedDatabase.value === dbName) {
    expandedDatabase.value = null
    tables.value = []
    selectedTable.value = null
  } else {
    await expandDatabase(dbName)
  }
}

const loadTableDetail = async (item) => {
  try {
    const response = await fetch(`/ui/api/tables/${expandedDatabase.value}/${item.name}`)
    if (response.ok) {
      const data = await response.json()
      selectedTable.value = {
        ...item,
        tableData: data.tableData
      }
    }
  } catch (error) {
    console.error('Failed to fetch table details:', error)
  }
}

const handleTableSelect = async (item) => {
  await loadTableDetail(item)
  router.push({ name: 'TableDetail', params: { id: `${expandedDatabase.value}.${item.name}` } })
}

watch(() => route.params.id, async (newId) => {
  if (newId && databases.value.length > 0) {
    const parts = newId.split('.')
    const dbName = parts[0]
    const tableName = parts.slice(1).join('.')
    if (expandedDatabase.value !== dbName) {
      await expandDatabase(dbName)
    }
    if (tableName) {
      const table = tables.value.find(t => t.name === tableName)
      if (table) {
        await loadTableDetail(table)
      }
    }
  }
})

onMounted(() => {
  fetchDatabases()
})
</script>

<style scoped>
.view-container {
  display: flex;
  height: calc(100vh - 60px);
}

.sidebar {
  width: 280px;
  background: #ffffff;
  border-right: 1px solid #e8e8e8;
  display: flex;
  flex-direction: column;
}

.db-list {
  flex-shrink: 0;
  overflow-y: auto;
}

.collapse-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 20px;
  border-bottom: 1px solid #f0f0f0;
  cursor: pointer;
  transition: background 0.2s ease;
  user-select: none;
}

.collapse-header:hover {
  background: #f5f5f5;
}

.collapse-header.active {
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.08) 0%, rgba(118, 75, 162, 0.08) 100%);
}

.collapse-arrow {
  font-size: 10px;
  color: #8c8c8c;
  flex-shrink: 0;
  width: 12px;
  text-align: center;
}

.collapse-title {
  font-size: 14px;
  font-weight: 600;
  color: #262626;
}

.table-list {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  border-top: 1px solid #e8e8e8;
}

.item {
  padding: 12px 20px 12px 38px;
  border-bottom: 1px solid #f0f0f0;
  border-left: 3px solid transparent;
  cursor: pointer;
  transition: background 0.2s ease;
}

.item:hover {
  background: #f5f5f5;
}

.item.active {
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
  border-left: 3px solid #667eea;
}

.item-name {
  font-size: 14px;
  font-weight: 600;
  color: #262626;
}

.detail-wrapper {
  flex: 1;
  background: #fafafa;
  overflow-y: auto;
  text-align: left;
}
</style>
