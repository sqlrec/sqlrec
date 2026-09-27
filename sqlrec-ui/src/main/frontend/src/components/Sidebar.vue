<template>
  <aside class="sidebar" :aria-label="`${searchLabel} list`">
    <div class="mobile-toggle-row">
      <button class="sidebar-toggle" type="button" :aria-expanded="mobileOpen"
        :aria-label="mobileOpen ? 'Hide list' : 'Show list'" @click="mobileOpen = !mobileOpen"
      >{{ mobileOpen ? 'Hide list' : 'Show list' }}</button>
    </div>
    <div class="sidebar-content" :class="{ open: mobileOpen }">
      <label class="search-field">
        <span class="sr-only">Search {{ searchLabel }}</span>
        <input v-model.trim="query" type="search" :placeholder="`Search ${searchLabel}`" />
      </label>
      <p v-if="loading" class="sidebar-message" role="status">Loading...</p>
      <div v-else-if="error" class="sidebar-message" role="alert">
        <p>{{ error }}</p>
        <button type="button" @click="$emit('retry')">Retry</button>
      </div>
      <p v-else-if="filteredItems.length === 0" class="sidebar-message">
        {{ query ? 'No matching results' : 'No items available' }}
      </p>
      <ul v-else class="item-list">
        <li v-for="item in filteredItems" :key="item.id || item.name">
          <button class="item" :class="{ active: item.name === selectedId }" type="button"
            :title="item.displayName || item.name"
            :aria-current="item.name === selectedId ? 'page' : undefined"
            @click="selectItem(item)"
          >{{ item.displayName || item.name }}</button>
        </li>
      </ul>
    </div>
  </aside>
</template>

<script setup>
import { computed, ref } from 'vue'

const props = defineProps({
  searchLabel: { type: String, required: true },
  items: { type: Array, required: true },
  selectedId: { type: String, default: '' },
  loading: { type: Boolean, default: false },
  error: { type: String, default: '' }
})
const emit = defineEmits(['select', 'retry'])
const query = ref('')
const mobileOpen = ref(false)
const filteredItems = computed(() => props.items.filter(item =>
  (item.displayName || item.name).toLocaleLowerCase().includes(query.value.toLocaleLowerCase())
))

const selectItem = item => {
  mobileOpen.value = false
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
  min-height: 0;
}
.mobile-toggle-row { display: none; }
.sidebar-toggle { display: none; }
.sidebar-content { min-height: 0; display: flex; flex: 1; flex-direction: column; }
.search-field { padding: 12px 8px 6px; }
.search-field input {
  width: 100%; padding: 8px 10px; border: 1px solid var(--border);
  border-radius: var(--radius-control); color: var(--text-h);
  background: var(--surface); font: inherit; font-size: 13px;
}
.search-field input:focus-visible { outline: 2px solid var(--brand); outline-offset: 1px; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
.sidebar-message { padding: 16px; color: var(--text-muted); font-size: 13px; text-align: left; }
.sidebar-message button { margin-top: 8px; border: 0; background: none; color: var(--brand); cursor: pointer; }
.item-list { list-style: none; margin: 0; padding: 6px 8px; overflow-y: auto; flex: 1; min-height: 0; }
.item { position: relative; display: block; width: 100%; min-height: 44px; padding: 10px 12px; border: 0; border-radius: var(--radius-control); background: transparent; color: var(--text-h); cursor: pointer; font: inherit; font-size: 14px; font-weight: 500; line-height: 24px; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; transition: background-color 0.18s ease, color 0.18s ease; }
.item:hover { background: var(--surface-subtle); }
.item:focus-visible { outline: 2px solid var(--brand); outline-offset: -2px; }
.item.active { background: var(--brand-soft); color: var(--brand-hover); font-weight: 600; }
.item.active::before { content: ''; position: absolute; top: 9px; bottom: 9px; left: 0; width: 3px; border-radius: 3px; background: var(--brand); }

@media (max-width: 720px) {
  .sidebar { width: 100%; flex: 0 0 auto; border-right: 0; border-bottom: 1px solid var(--border); }
  .mobile-toggle-row { display: flex; justify-content: flex-end; padding: 8px 12px; border-bottom: 1px solid var(--border); }
  .sidebar-toggle { display: inline-block; border: 0; background: transparent; color: var(--brand); font: inherit; cursor: pointer; }
  .sidebar-content { display: none; }
  .sidebar-content.open { display: flex; max-height: 36svh; }
}
</style>
