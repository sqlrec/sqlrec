<template>
  <div class="sql-panel">
    <div v-if="sql" class="sql-content">
      <pre><code class="hljs language-sql" v-html="highlightedSql"></code></pre>
    </div>
    <div v-else class="no-sql">No SQL statements</div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
import 'highlight.js/styles/github.css'

hljs.registerLanguage('sql', sql)

const props = defineProps({
  sql: {
    type: String,
    default: ''
  }
})

const highlightedSql = computed(() => {
  if (!props.sql) {
    return ''
  }
  try {
    return hljs.highlight(props.sql, { language: 'sql' }).value
  } catch (e) {
    return props.sql
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

.sql-content {
  overflow: auto;
}

.sql-content pre {
  margin: 0;
  padding: 16px 20px;
  background: #f6f8fa;
  overflow-x: auto;
}

.sql-content pre code {
  display: block;
  padding: 0;
  background: transparent;
  font-family: 'SFMono-Regular', 'Consolas', 'Liberation Mono', 'Menlo', ui-monospace, monospace;
  font-size: 13px;
  line-height: 1.5;
  white-space: pre;
  word-wrap: normal;
  border-radius: 0;
  color: #24292e;
  text-align: left;
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
