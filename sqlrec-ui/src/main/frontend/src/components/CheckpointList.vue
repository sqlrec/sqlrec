<template>
  <div class="checkpoint-list">
    <div v-if="checkpoints.length > 0 || loading" class="checkpoint-content">
      <div class="checkpoint-card">
        <div class="section-header">
          <span class="section-title"># Checkpoints</span>
          <span class="total-count">{{ total }} records</span>
        </div>

        <div v-if="loading" class="loading">Loading...</div>

        <table v-else class="formatted-table">
          <thead>
            <tr>
              <th>Checkpoint Name</th>
              <th>Type</th>
              <th>Status</th>
              <th>Created At</th>
              <th>Updated At</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="checkpoint in checkpoints" :key="checkpoint.checkpointName" @click="handleClick(checkpoint.checkpointName)" class="clickable-row">
              <td class="col-name">{{ checkpoint.checkpointName }}</td>
              <td>
                <span :class="['status-tag', `type-${checkpoint.checkpointType}`]">
                  {{ checkpoint.checkpointType || '-' }}
                </span>
              </td>
              <td>
                <span :class="['status-tag', `status-${checkpoint.status}`]">
                  {{ checkpoint.status || '-' }}
                </span>
              </td>
              <td>{{ checkpoint.createdAt }}</td>
              <td>{{ checkpoint.updatedAt }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      
      <div v-if="!loading && total > 0" class="pagination">
        <button 
          class="page-btn" 
          :disabled="page === 1" 
          @click="changePage(page - 1)"
        >
          Previous
        </button>
        <span class="page-info">
          Page {{ page }} / {{ totalPages }}
        </span>
        <button 
          class="page-btn" 
          :disabled="page >= totalPages" 
          @click="changePage(page + 1)"
        >
          Next
        </button>
        <select v-model="localPageSize" class="page-size-select" @change="handlePageSizeChange">
          <option :value="10">10/page</option>
          <option :value="20">20/page</option>
          <option :value="50">50/page</option>
          <option :value="100">100/page</option>
        </select>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted } from 'vue'
import { encodePathSegment } from '../utils/url.js'

const props = defineProps({
  modelName: {
    type: String,
    required: true
  }
})

const checkpoints = ref([])
const loading = ref(false)
const page = ref(1)
const pageSize = ref(10)
const localPageSize = ref(10)
const total = ref(0)
const totalPages = ref(0)

const emit = defineEmits(['checkpoint-click'])

const fetchCheckpoints = async () => {
  if (!props.modelName) return
  
  loading.value = true
  try {
    const response = await fetch(
      `/ui/api/models/${encodePathSegment(props.modelName)}/checkpoints?page=${page.value}&pageSize=${pageSize.value}`
    )
    if (response.ok) {
      const data = await response.json()
      checkpoints.value = data.items
      total.value = data.total
      totalPages.value = data.totalPages
    }
  } catch (error) {
    console.error('Failed to fetch checkpoints:', error)
  } finally {
    loading.value = false
  }
}

const changePage = (newPage) => {
  if (newPage >= 1 && newPage <= totalPages.value) {
    page.value = newPage
    fetchCheckpoints()
  }
}

const handlePageSizeChange = () => {
  pageSize.value = localPageSize.value
  page.value = 1
  fetchCheckpoints()
}

const handleClick = (checkpointName) => {
  emit('checkpoint-click', checkpointName)
}

watch(() => props.modelName, () => {
  page.value = 1
  fetchCheckpoints()
})

onMounted(() => {
  fetchCheckpoints()
})
</script>

<style scoped>
.checkpoint-list {
  background: var(--page-bg);
}

.checkpoint-content {
  width: 100%;
  max-width: var(--content-max-width);
  margin: 0 auto;
  padding: 0 var(--page-padding) var(--page-padding);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  min-height: 40px;
  padding: 0 16px;
  background: var(--surface-subtle);
  border-bottom: 1px solid var(--border);
  margin-bottom: 0;
}

.checkpoint-card {
  overflow: hidden;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
}

.section-title {
  font-weight: 600;
  font-size: 13px;
  color: var(--text);
}

.total-count {
  font-size: 12px;
  color: var(--text-muted);
}

.loading {
  padding: 40px;
  text-align: center;
  color: var(--text-muted);
  background: var(--surface);
}

.formatted-table {
  width: 100%;
  border-collapse: collapse;
  background: var(--surface);
}

.formatted-table th,
.formatted-table td {
  padding: 10px 16px;
  text-align: left;
  border-bottom: 1px solid var(--border);
  font-size: 14px;
}

.formatted-table th {
  background: var(--surface-subtle);
  font-weight: 600;
  color: var(--text-h);
}

.formatted-table td {
  color: var(--text);
}

.formatted-table tbody tr:last-child td {
  border-bottom: 0;
}

.col-name {
  font-weight: 500;
  color: var(--text-h);
}

.clickable-row {
  cursor: pointer;
  transition: background-color 0.2s;
}

.clickable-row:hover {
  background-color: #f5f6fa;
}

.status-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 5px;
  font-size: 12px;
}

.status-active,
.status-ready {
  background: #f6ffed;
  color: #52c41a;
}

.status-inactive,
.status-error {
  background: #fff2f0;
  color: #ff4d4f;
}

.status-pending {
  background: #fffbe6;
  color: #faad14;
}

.type-full {
  background: #e6f7ff;
  color: var(--brand);
}

.type-incremental {
  background: #f9f0ff;
  color: #722ed1;
}

.pagination {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  gap: 12px;
  margin-top: 16px;
}

.page-btn {
  min-height: 32px;
  padding: 0 12px;
  border: 1px solid var(--border);
  border-radius: var(--radius-control);
  background: var(--surface);
  color: var(--text);
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
}

.page-btn:hover:not(:disabled) {
  border-color: var(--brand);
  background: var(--brand-soft);
  color: var(--brand);
}

.page-btn:disabled {
  background: #f8f9fb;
  color: var(--text-muted);
  cursor: not-allowed;
}

.page-btn:focus-visible {
  outline: 2px solid rgba(82, 100, 195, 0.28);
  outline-offset: 2px;
}

.page-info {
  font-size: 14px;
  color: var(--text);
}

.page-size-select {
  min-height: 32px;
  padding: 0 8px;
  border: 1px solid var(--border);
  border-radius: var(--radius-control);
  background: var(--surface);
  font-size: 14px;
  color: var(--text);
  cursor: pointer;
}

.page-size-select:focus {
  outline: 2px solid rgba(82, 100, 195, 0.2);
  outline-offset: 1px;
  border-color: var(--brand);
}
</style>
