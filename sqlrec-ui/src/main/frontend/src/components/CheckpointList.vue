<template>
  <div class="checkpoint-list">
    <div v-if="checkpoints.length > 0 || loading" class="checkpoint-content">
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
      `/ui/api/models/${props.modelName}/checkpoints?page=${page.value}&pageSize=${pageSize.value}`
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
  background: #fafafa;
}

.checkpoint-content {
  padding: 0 32px 32px 32px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px;
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.05) 0%, rgba(118, 75, 162, 0.05) 100%);
  border-radius: 8px 8px 0 0;
  margin-bottom: 0;
}

.section-title {
  font-weight: 700;
  font-size: 15px;
  color: #667eea;
}

.total-count {
  font-size: 14px;
  color: #8c8c8c;
}

.loading {
  padding: 40px;
  text-align: center;
  color: #8c8c8c;
  background: white;
}

.formatted-table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border-radius: 0 0 8px 8px;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.formatted-table th,
.formatted-table td {
  padding: 12px 16px;
  text-align: left;
  border-bottom: 1px solid #e8e8e8;
  font-size: 14px;
}

.formatted-table th {
  background: #fafafa;
  font-weight: 600;
  color: #262626;
}

.formatted-table td {
  color: #595959;
}

.col-name {
  font-weight: 500;
  color: #262626;
}

.clickable-row {
  cursor: pointer;
  transition: background-color 0.2s;
}

.clickable-row:hover {
  background-color: #f5f5f5;
}

.status-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
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
  color: #1890ff;
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
  padding: 6px 16px;
  border: 1px solid #d9d9d9;
  border-radius: 4px;
  background: white;
  color: #595959;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
}

.page-btn:hover:not(:disabled) {
  border-color: #1890ff;
  color: #1890ff;
}

.page-btn:disabled {
  color: #d9d9d9;
  cursor: not-allowed;
}

.page-info {
  font-size: 14px;
  color: #595959;
}

.page-size-select {
  padding: 6px 8px;
  border: 1px solid #d9d9d9;
  border-radius: 4px;
  background: white;
  font-size: 14px;
  color: #595959;
  cursor: pointer;
}

.page-size-select:focus {
  outline: none;
  border-color: #1890ff;
}
</style>
