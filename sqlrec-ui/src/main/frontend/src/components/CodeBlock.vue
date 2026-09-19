<template>
  <div class="code-block" :style="{ '--code-max-height': maxHeight }">
    <div class="code-toolbar">
      <span class="code-title">{{ displayTitle }}</span>
      <button
        type="button"
        class="copy-button"
        :class="{ copied }"
        :aria-label="copied ? 'Code copied' : `Copy ${displayTitle}`"
        @click="copyCode"
      >
        {{ copied ? 'Copied' : 'Copy' }}
      </button>
    </div>
    <div
      class="code-scroll"
      tabindex="0"
      :aria-label="`${displayTitle} code`"
      @keydown="handleKeydown"
    >
      <pre><code
        ref="codeRef"
        class="hljs"
        :class="language ? `language-${language}` : ''"
        v-html="highlightedCode"
      ></code></pre>
    </div>
  </div>
</template>

<script setup>
import { computed, onUnmounted, ref } from 'vue'
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
  },
  title: {
    type: String,
    default: ''
  },
  maxHeight: {
    type: String,
    default: '640px'
  }
})

const codeRef = ref(null)
const copied = ref(false)
let copiedTimer = null

const displayTitle = computed(() => {
  return props.title || (props.language ? props.language.toUpperCase() : 'Code')
})

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

const fallbackCopy = () => {
  const textArea = document.createElement('textarea')
  textArea.value = props.code
  textArea.style.position = 'fixed'
  textArea.style.opacity = '0'
  document.body.appendChild(textArea)
  textArea.select()
  const copiedSuccessfully = document.execCommand('copy')
  document.body.removeChild(textArea)
  return copiedSuccessfully
}

const copyCode = async () => {
  let copiedSuccessfully = false

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(props.code)
      copiedSuccessfully = true
    } else {
      copiedSuccessfully = fallbackCopy()
    }
  } catch {
    copiedSuccessfully = fallbackCopy()
  }

  if (!copiedSuccessfully) return

  copied.value = true
  window.clearTimeout(copiedTimer)
  copiedTimer = window.setTimeout(() => {
    copied.value = false
  }, 1500)
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

onUnmounted(() => {
  window.clearTimeout(copiedTimer)
})
</script>

<style scoped>
.code-block {
  background: #f8f9fb;
  border-radius: 8px;
  border: 1px solid var(--border);
  overflow: hidden;
  outline: none;
}

.code-toolbar {
  min-height: 40px;
  padding: 0 12px 0 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  background: #f8f9fc;
  border-bottom: 1px solid var(--border);
}

.code-title {
  overflow: hidden;
  color: var(--text);
  font-size: 13px;
  line-height: 20px;
  font-weight: 600;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.copy-button {
  flex: 0 0 auto;
  padding: 4px 8px;
  border: 1px solid transparent;
  border-radius: 5px;
  background: transparent;
  color: var(--text-muted);
  font: inherit;
  font-size: 12px;
  line-height: 18px;
  cursor: pointer;
  transition: background-color 0.18s ease, color 0.18s ease;
}

.copy-button:hover,
.copy-button:focus-visible {
  outline: none;
  background: var(--brand-soft);
  color: var(--brand-hover);
}

.copy-button.copied {
  color: var(--brand);
}

.code-scroll {
  max-height: var(--code-max-height);
  overflow: auto;
  cursor: text;
  outline: none;
}

.code-scroll:focus-visible {
  box-shadow: inset 3px 0 0 rgba(82, 100, 195, 0.45);
}

.code-block pre {
  margin: 0;
  padding: 14px 16px;
  background: transparent;
  overflow: visible;
}

.code-block pre code {
  display: block;
  padding: 0;
  background: transparent;
  font-family: 'SFMono-Regular', 'Consolas', 'Liberation Mono', 'Menlo', ui-monospace, monospace;
  font-size: 13px;
  line-height: 1.55;
  tab-size: 2;
  white-space: pre;
  word-wrap: normal;
  border-radius: 0;
  color: #24292e;
  text-align: left;
}

.code-scroll::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

.code-scroll::-webkit-scrollbar-track {
  background: transparent;
}

.code-scroll::-webkit-scrollbar-thumb {
  background: #d0d0d0;
  border-radius: 3px;
}

.code-scroll::-webkit-scrollbar-thumb:hover {
  background: #b0b0b0;
}
</style>
