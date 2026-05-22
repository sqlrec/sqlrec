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
  width: 280px;
  background: #ffffff;
  border-right: 1px solid #e8e8e8;
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
  padding: 0;
  overflow-y: auto;
  flex: 1;
}

.item {
  padding: 16px 20px;
  border-bottom: 1px solid #f0f0f0;
  cursor: pointer;
  transition: all 0.2s ease;
}

.item:hover {
  background: #f5f5f5;
}

.item.active {
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
  border-left: 3px solid #667eea;
}

.item-name {
  font-size: 15px;
  font-weight: 600;
  color: #262626;
}
</style>
