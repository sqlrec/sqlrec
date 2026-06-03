<template>
  <div class="code-block" tabindex="0" @keydown="handleKeydown">
    <pre><code
      ref="codeRef"
      class="hljs"
      :class="language ? `language-${language}` : ''"
      v-html="highlightedCode"
    ></code></pre>
  </div>
</template>

<script setup>
import { computed, ref } from 'vue'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
import java from 'highlight.js/lib/languages/java'
import yaml from 'highlight.js/lib/languages/yaml'
import 'highlight.js/styles/github.css'

hljs.registerLanguage('sql', sql)
hljs.registerLanguage('java', java)
hljs.registerLanguage('yaml', yaml)

const props = defineProps({
  code: {
    type: String,
    required: true
  },
  language: {
    type: String,
    default: ''
  }
})

const codeRef = ref(null)

const highlightedCode = computed(() => {
  if (!props.code) return ''
  if (!props.language) return escapeHtml(props.code)
  try {
    return hljs.highlight(props.code, { language: props.language }).value
  } catch (e) {
    return escapeHtml(props.code)
  }
})

const escapeHtml = (str) => {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

const handleKeydown = (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === 'a') {
    event.preventDefault()
    const codeEl = codeRef.value
    if (!codeEl) return
    const selection = window.getSelection()
    const range = document.createRange()
    range.selectNodeContents(codeEl)
    selection.removeAllRanges()
    selection.addRange(range)
  }
}
</script>

<style scoped>
.code-block {
  background: #f6f8fa;
  border-radius: 6px;
  border: 1px solid #e0e0e0;
  overflow: auto;
  max-height: 300px;
  cursor: text;
  outline: none;
}

.code-block:focus {
  border-color: #667eea;
}

.code-block pre {
  margin: 0;
  padding: 16px 20px;
  background: transparent;
  overflow: visible;
}

.code-block pre code {
  display: block;
  padding: 0;
  background: transparent;
  font-family: 'SFMono-Regular', 'Consolas', 'Liberation Mono', 'Menlo', ui-monospace, monospace;
  font-size: 12px;
  line-height: 1.5;
  white-space: pre;
  word-wrap: normal;
  border-radius: 0;
  color: #24292e;
  text-align: left;
}

.code-block::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

.code-block::-webkit-scrollbar-track {
  background: transparent;
}

.code-block::-webkit-scrollbar-thumb {
  background: #d0d0d0;
  border-radius: 3px;
}

.code-block::-webkit-scrollbar-thumb:hover {
  background: #b0b0b0;
}
</style>
