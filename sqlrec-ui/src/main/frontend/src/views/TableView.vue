<template>
  <div class="view-container">
    <aside class="sidebar" aria-label="Tables list">
      <div class="mobile-toggle-row">
        <button type="button" class="sidebar-toggle" :aria-expanded="mobileOpen"
          :aria-label="mobileOpen ? 'Hide list' : 'Show list'"
          @click="mobileOpen = !mobileOpen">{{ mobileOpen ? 'Hide list' : 'Show list' }}</button>
      </div>
      <div class="sidebar-content" :class="{ open: mobileOpen }">
        <label class="search-field">
          <span class="sr-only">Search databases or current tables</span>
          <input v-model.trim="query" type="search" placeholder="Search databases or current tables" />
        </label>
        <p v-if="databaseLoading" class="sidebar-message" role="status">Loading databases...</p>
        <div v-else-if="databaseError" class="sidebar-message" role="alert">
          {{ databaseError }} <button type="button" @click="fetchDatabases">Retry</button>
        </div>
        <div v-else class="db-list">
          <button v-for="db in filteredDatabases" :key="db.name" class="collapse-header"
            :class="{ active: expandedDatabase === db.name }" type="button"
            :aria-expanded="expandedDatabase === db.name" @click="toggleDatabase(db.name)"
          >
            <span class="collapse-title">{{ db.name }}</span>
            <span class="collapse-arrow">{{ expandedDatabase === db.name ? '▼' : '▶' }}</span>
          </button>
          <p v-if="filteredDatabases.length === 0" class="sidebar-message">
            {{ query ? 'No matching databases' : 'No databases available' }}
          </p>
        </div>
        <div v-if="expandedDatabase" class="table-list">
          <p v-if="tableLoading" class="sidebar-message" role="status">Loading tables...</p>
          <div v-else-if="tableError" class="sidebar-message" role="alert">
            {{ tableError }} <button type="button" @click="retryTables">Retry</button>
          </div>
          <p v-else-if="filteredTables.length === 0" class="sidebar-message">
            {{ query ? 'No matching tables' : 'No tables available' }}
          </p>
          <button v-for="table in filteredTables" :key="table.name" class="item"
            :class="{ active: selectedTable?.name === table.name }" type="button"
            :aria-current="selectedTable?.name === table.name ? 'page' : undefined"
            :title="table.name" @click="handleTableSelect(table)"
          >
            <span class="item-name">{{ table.name }}</span>
          </button>
        </div>
      </div>
    </aside>
    <main class="detail-wrapper">
      <ContentState v-if="databaseLoading" message="Loading databases..." />
      <ContentState v-else-if="databaseError" :message="databaseError" action="Retry" @action="fetchDatabases" />
      <ContentState v-else-if="!route.params.id" plain message="Select a table from the sidebar to view its details" />
      <ContentState v-else-if="tableLoading || detailLoading" message="Loading table details..." />
      <ContentState v-else-if="tableError || detailError" :message="tableError || detailError" action="Retry" @action="syncRoute" />
      <DetailPanel v-else-if="selectedTable" :item="selectedTable" />
    </main>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import DetailPanel from '../components/DetailPanel.vue'
import ContentState from '../components/ContentState.vue'
import { encodePathSegment } from '../utils/url.js'

const route = useRoute()
const router = useRouter()

const databases = ref([])
const tables = ref([])
const expandedDatabase = ref(null)
const selectedTable = ref(null)
const query = ref('')
const mobileOpen = ref(false)
const databaseLoading = ref(false)
const tableLoading = ref(false)
const detailLoading = ref(false)
const databaseError = ref('')
const tableError = ref('')
const detailError = ref('')
const filteredDatabases = computed(() => databases.value.filter(db =>
  db.name.toLocaleLowerCase().includes(query.value.toLocaleLowerCase()) ||
  (db.name === expandedDatabase.value && tables.value.some(table =>
    table.name.toLocaleLowerCase().includes(query.value.toLocaleLowerCase())))
))
const filteredTables = computed(() => tables.value.filter(table =>
  table.name.toLocaleLowerCase().includes(query.value.toLocaleLowerCase())
))
let databaseController
let tableController
let detailController

const fetchDatabases = async () => {
  databaseController?.abort()
  const controller = new AbortController()
  databaseController = controller
  databaseLoading.value = true
  databaseError.value = ''
  try {
    const response = await fetch('/ui/api/tables/databases', { signal: controller.signal })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const data = await response.json()
    if (!controller.signal.aborted) databases.value = data
  } catch (error) {
    if (!controller.signal.aborted) databaseError.value = 'Failed to load databases. Please retry.'
  } finally {
    if (databaseController === controller) {
      databaseLoading.value = false
      syncRoute()
    }
  }
}

const loadTables = async dbName => {
  tableController?.abort()
  detailController?.abort()
  const controller = new AbortController()
  tableController = controller
  expandedDatabase.value = dbName
  tables.value = []
  selectedTable.value = null
  tableLoading.value = true
  tableError.value = ''
  detailError.value = ''
  try {
    const response = await fetch(`/ui/api/tables/${encodePathSegment(dbName)}/`, { signal: controller.signal })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const data = await response.json()
    if (!controller.signal.aborted) tables.value = data
  } catch (error) {
    if (!controller.signal.aborted) tableError.value = 'Failed to load tables. Please retry.'
  } finally {
    if (tableController === controller) tableLoading.value = false
  }
}

const toggleDatabase = dbName => {
  if (expandedDatabase.value === dbName) {
    tableController?.abort()
    detailController?.abort()
    expandedDatabase.value = null
    tables.value = []
    selectedTable.value = null
  } else {
    loadTables(dbName)
  }
  router.push({ name: 'Table' })
}

const loadTableDetail = async (dbName, item) => {
  detailController?.abort()
  const controller = new AbortController()
  detailController = controller
  selectedTable.value = null
  detailLoading.value = true
  detailError.value = ''
  try {
    const response = await fetch(
      `/ui/api/tables/${encodePathSegment(dbName)}/${encodePathSegment(item.name)}`,
      { signal: controller.signal }
    )
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const data = await response.json()
    if (!controller.signal.aborted) selectedTable.value = { ...item, tableData: data.tableData }
  } catch (error) {
    if (!controller.signal.aborted) detailError.value = 'Failed to load table details. Please retry.'
  } finally {
    if (detailController === controller) detailLoading.value = false
  }
}

const handleTableSelect = item => {
  mobileOpen.value = false
  router.push({ name: 'TableDetail', params: { id: `${expandedDatabase.value}.${item.name}` } })
}

const retryTables = async () => {
  if (!expandedDatabase.value) return
  await loadTables(expandedDatabase.value)
  if (route.params.id && !tableError.value) syncRoute()
}

const syncRoute = async () => {
  detailController?.abort()
  selectedTable.value = null
  detailLoading.value = false
  detailError.value = ''
  if (!route.params.id || databaseLoading.value || databaseError.value) return
  const [dbName, ...parts] = String(route.params.id).split('.')
  const tableName = parts.join('.')
  if (!databases.value.some(db => db.name === dbName)) {
    detailError.value = 'Database not found.'
    return
  }
  if (expandedDatabase.value !== dbName || tables.value.length === 0) {
    await loadTables(dbName)
  }
  if (dbName !== expandedDatabase.value || route.params.id !== `${dbName}.${tableName}` || tableError.value) return
  const table = tables.value.find(item => item.name === tableName)
  if (!table) {
    detailError.value = 'Table not found.'
    return
  }
  loadTableDetail(dbName, table)
}

watch(() => route.params.id, syncRoute)

onMounted(fetchDatabases)
onUnmounted(() => {
  databaseController?.abort()
  tableController?.abort()
  detailController?.abort()
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
  min-height: 0;
}

.mobile-toggle-row { display: none; }
.sidebar-toggle { display: none; }
.sidebar-content { min-height: 0; display: flex; flex: 1; flex-direction: column; }
.search-field { padding: 12px 8px 6px; }
.search-field input { width: 100%; padding: 8px 10px; border: 1px solid var(--border); border-radius: var(--radius-control); background: var(--surface); color: var(--text-h); font: inherit; font-size: 13px; }
.search-field input:focus-visible { outline: 2px solid var(--brand); outline-offset: 1px; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
.sidebar-message { padding: 12px 16px; color: var(--text-muted); font-size: 13px; text-align: left; }
.sidebar-message button { border: 0; background: none; color: var(--brand); cursor: pointer; }

.db-list {
  flex-shrink: 0;
  max-height: 40%;
  padding: 6px 0;
  overflow-y: auto;
}

.collapse-header {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: flex-start;
  text-align: left;
  min-height: 44px;
  width: calc(100% - 16px);
  margin: 0 8px;
  padding: 10px 12px;
  border-radius: var(--radius-control);
  cursor: pointer;
  transition: background 0.2s ease;
  user-select: none;
  border: 0;
  background: transparent;
  font: inherit;
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
  min-width: 0;
  margin-right: 16px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
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
  background: var(--surface);
  border-top: 1px solid var(--border);
}

.item {
  position: relative;
  display: block;
  width: 100%;
  text-align: left;
  min-height: 44px;
  padding: 10px 12px;
  border-radius: var(--radius-control);
  cursor: pointer;
  transition: background 0.2s ease;
  border: 0;
  background: transparent;
  font: inherit;
}

.collapse-header:focus-visible, .item:focus-visible { outline: 2px solid var(--brand); outline-offset: -2px; }

.item:hover {
  background: #f2f4f8;
}

.item.active {
  background: var(--brand-soft);
}

.collapse-header.active::before,
.item.active::before {
  content: '';
  position: absolute;
  top: 9px;
  bottom: 9px;
  left: 0;
  width: 3px;
  border-radius: 3px;
  background: var(--brand);
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
  position: relative;
  flex: 1;
  min-width: 0;
  background: var(--page-bg);
  overflow-y: auto;
  text-align: left;
}

@media (max-width: 720px) {
  .view-container { flex-direction: column; }
  .sidebar { width: 100%; flex: 0 0 auto; border-right: 0; border-bottom: 1px solid var(--border); }
  .mobile-toggle-row { display: flex; justify-content: flex-end; padding: 8px 12px; border-bottom: 1px solid var(--border); }
  .sidebar-toggle { display: inline-block; border: 0; background: transparent; color: var(--brand); font: inherit; cursor: pointer; }
  .sidebar-content { display: none; }
  .sidebar-content.open { display: flex; max-height: 40svh; }
  .detail-wrapper { min-height: 0; }
}
</style>
