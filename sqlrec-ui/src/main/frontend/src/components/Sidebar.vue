<template>
  <aside class="sidebar">
    <div class="sidebar-content">
      <ul class="item-list">
        <li 
          v-for="item in items" 
          :key="item.id"
          class="item"
          :class="{ active: selectedId ? item.id === selectedId : selectedItem?.id === item.id }"
          @click="selectItem(item)"
        >
          <div class="item-name">{{ item.displayName || item.name }}</div>
        </li>
      </ul>
    </div>
  </aside>
</template>

<script setup>
import { ref, watch } from 'vue'

const props = defineProps({
  title: {
    type: String,
    required: true
  },
  items: {
    type: Array,
    required: true
  },
  selectedId: {
    type: String,
    default: null
  }
})

const emit = defineEmits(['select'])

const selectedItem = ref(null)

watch(() => props.selectedId, (newId) => {
  if (newId) {
    const item = props.items.find(i => i.id === newId)
    if (item) {
      selectedItem.value = item
    }
  }
}, { immediate: true })

const selectItem = (item) => {
  selectedItem.value = item
  emit('select', item)
}
</script>

<style scoped>
.sidebar {
  width: var(--sidebar-width);
  flex: 0 0 var(--sidebar-width);
  background: var(--surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
}

.sidebar-content {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.item-list {
  list-style: none;
  margin: 0;
  padding: 6px 8px;
  overflow-y: auto;
  flex: 1;
}

.item {
  min-height: 44px;
  padding: 10px 12px;
  border-radius: var(--radius-control);
  cursor: pointer;
  transition: background-color 0.18s ease, color 0.18s ease;
}

.item:hover {
  background: #f5f6fa;
}

.item.active {
  background: var(--brand-soft);
}

.item-name {
  overflow: hidden;
  font-size: 14px;
  line-height: 24px;
  font-weight: 500;
  color: var(--text-h);
  text-align: center;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.item.active .item-name {
  color: var(--brand-hover);
  font-weight: 600;
}
</style>
