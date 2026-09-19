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
import { encodePathSegment } from '../utils/url.js'

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
    const response = await fetch(`/ui/api/tables/${encodePathSegment(dbName)}/`)
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
    const response = await fetch(
      `/ui/api/tables/${encodePathSegment(expandedDatabase.value)}/${encodePathSegment(item.name)}`
    )
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
  height: calc(100vh - var(--header-height));
  height: calc(100svh - var(--header-height));
}

.sidebar {
  width: var(--sidebar-width);
  flex: 0 0 var(--sidebar-width);
  background: var(--surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
}

.db-list {
  flex-shrink: 0;
  padding: 6px 0;
  overflow-y: auto;
}

.collapse-header {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 44px;
  margin: 0 8px;
  padding: 10px 12px;
  border-radius: var(--radius-control);
  cursor: pointer;
  transition: background 0.2s ease;
  user-select: none;
}

.collapse-header:hover {
  background: #f5f6fa;
}

.collapse-header.active {
  background: var(--brand-soft);
}

.collapse-header.active .collapse-title,
.collapse-header.active .collapse-arrow {
  color: var(--brand-hover);
}

.collapse-arrow {
  position: absolute;
  right: 12px;
  font-size: 10px;
  color: var(--text-muted);
  flex-shrink: 0;
  width: 12px;
  text-align: center;
}

.collapse-title {
  font-size: 14px;
  line-height: 24px;
  font-weight: 500;
  color: var(--text-h);
}

.table-list {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  padding: 4px 8px 8px;
  background: #fafbfc;
}

.item {
  min-height: 44px;
  padding: 10px 12px;
  border-radius: var(--radius-control);
  cursor: pointer;
  transition: background 0.2s ease;
}

.item:hover {
  background: #f2f4f8;
}

.item.active {
  background: var(--brand-soft);
}

.item-name {
  font-size: 14px;
  line-height: 24px;
  font-weight: 500;
  color: var(--text-h);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.item.active .item-name {
  color: var(--brand-hover);
  font-weight: 600;
}

.detail-wrapper {
  flex: 1;
  min-width: 0;
  background: var(--page-bg);
  overflow-y: auto;
  text-align: left;
}
</style>
