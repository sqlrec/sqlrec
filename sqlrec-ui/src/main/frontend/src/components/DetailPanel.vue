<template>
  <div class="detail-panel">
    <div v-if="tableData.length > 0" class="detail-content">
      <div class="detail-card">
        <table class="formatted-table">
          <tbody>
            <tr v-for="(row, index) in tableData" :key="index" :class="getRowClass(row)">
              <td v-if="isSectionRow(row)" class="section-title-cell" colspan="2">
                {{ row.col_name }}
              </td>
              <template v-else>
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
              </template>
            </tr>
          </tbody>
        </table>
      </div>
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
  if (isSectionRow(row)) {
    return 'section-header'
  }
  if (row.col_name === '' && row.data_type === '') {
    return 'separator'
  }
  return ''
}

const isSectionRow = (row) => {
  return row.col_name.startsWith('#')
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
  background: var(--page-bg);
}

.detail-content {
  width: 100%;
  max-width: var(--content-max-width);
  margin: 0 auto;
  padding: var(--page-padding);
}

.detail-card {
  width: 100%;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
  overflow: hidden;
}

.formatted-table {
  width: 100%;
  border-collapse: collapse;
  background: var(--surface);
}

.formatted-table td {
  padding: 10px 16px;
  text-align: left;
  border-bottom: 1px solid var(--border);
  font-size: 14px;
  color: var(--text);
}

.formatted-table tr:last-child td {
  border-bottom: 0;
}

.col-name {
  font-weight: 600;
  color: var(--text-h);
  width: 32%;
}

.data-type {
  color: var(--text);
  word-break: break-word;
}

.section-header {
  background: var(--surface-subtle);
}

.section-title-cell {
  height: 40px;
  padding: 0 16px !important;
  font-weight: 600;
  font-size: 13px;
  color: var(--text) !important;
}

.separator {
  background: var(--page-bg);
}

.separator td {
  padding: 6px 16px;
}

.detail-link {
  color: var(--brand);
  text-decoration: none;
}

.detail-link:hover {
  color: var(--brand-hover);
  text-decoration: underline;
}
</style>
