<template>
  <div class="sql-panel">
    <CodeBlock v-if="sql" :code="sql" language="sql" />
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
  background: #fff;
  border-radius: 8px;
  border: 1px solid #e0e0e0;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
  overflow: hidden;
}

.no-sql {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  color: #999;
  font-size: 14px;
  text-align: left;
}
</style>
