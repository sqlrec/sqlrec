<template>
  <div class="sql-panel">
    <CodeBlock v-if="sql" title="SQL" :code="sql" language="sql" />
    <div v-else class="no-sql">No SQL statements</div>
  </div>
</template>

<script setup>
import CodeBlock from './CodeBlock.vue'

const props = defineProps({
  sql: {
    type: String,
    default: ''
  }
})
</script>

<style scoped>
.sql-panel {
  text-align: left;
}

.no-sql {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
  color: var(--text-muted);
  font-size: 14px;
  text-align: left;
}
</style>
