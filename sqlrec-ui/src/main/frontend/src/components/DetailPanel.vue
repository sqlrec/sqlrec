<template>
  <div class="detail-panel">
    <div v-if="tableData.length > 0" class="detail-content">
      <table class="formatted-table">
        <tbody>
          <tr v-for="(row, index) in tableData" :key="index" :class="getRowClass(row)">
            <td class="col-name">{{ row.col_name }}</td>
            <td class="data-type">
              <template v-if="isLinkRow(row)">
                <router-link :to="getLinkPath(row)" class="detail-link">
                  {{ row.data_type }}
                </router-link>
              </template>
              <template v-else>
                {{ row.data_type }}
              </template>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'

const props = defineProps({
  item: {
    type: Object,
    default: null
  }
})

const tableData = ref([])

watch(() => props.item, (newItem) => {
  if (newItem && newItem.tableData) {
    tableData.value = newItem.tableData
  } else {
    tableData.value = []
  }
}, { immediate: true })

const getRowClass = (row) => {
  if (row.col_name.startsWith('#')) {
    return 'section-header'
  }
  if (row.col_name === '' && row.data_type === '') {
    return 'separator'
  }
  return ''
}

const isLinkRow = (row) => {
  return row.col_name === 'Function Name:' || row.col_name === 'Model Name:'
}

const getLinkPath = (row) => {
  if (row.col_name === 'Function Name:') {
    return { name: 'FunctionDetail', params: { id: row.data_type } }
  }
  if (row.col_name === 'Model Name:') {
    return { name: 'ModelDetail', params: { id: row.data_type } }
  }
  return '#'
}
</script>

<style scoped>
.detail-panel {
  background: #fafafa;
}

.detail-content {
  padding: 32px;
}

.formatted-table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border-radius: 8px;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.formatted-table td {
  padding: 12px 16px;
  text-align: left;
  border-bottom: 1px solid #e8e8e8;
  font-size: 14px;
  color: #595959;
}

.col-name {
  font-weight: 500;
  color: #262626;
  width: 40%;
}

.data-type {
  color: #595959;
  word-break: break-word;
}

.section-header {
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.05) 0%, rgba(118, 75, 162, 0.05) 100%);
}

.section-header .col-name {
  font-weight: 700;
  font-size: 15px;
  color: #667eea;
}

.separator {
  background: #fafafa;
}

.separator td {
  padding: 8px 16px;
}

.detail-link {
  color: #1890ff;
  text-decoration: none;
}

.detail-link:hover {
  color: #40a9ff;
  text-decoration: underline;
}
</style>
