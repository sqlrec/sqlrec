<template>
  <div class="view-container">
    <Sidebar search-label="models" :items="items" :selected-id="selectedItem?.name"
      :loading="listLoading" :error="listError" @select="selectItem" @retry="loadList" />
    <main class="detail-wrapper">
      <ContentState v-if="listLoading" message="Loading models..." />
      <ContentState v-else-if="listError" :message="listError" action="Retry" @action="loadList" />
      <ContentState v-else-if="!selectedItem && !detailError" plain message="Select a model from the sidebar to view its details" />
      <ContentState v-else-if="detailLoading" message="Loading model details..." />
      <ContentState v-else-if="detailError" :message="detailError" action="Retry" @action="loadDetail" />
      <template v-else-if="detail">
        <DetailPanel :item="detail" />
        <div v-if="detail.ddl" class="code-section">
          <CodeBlock title="DDL" :code="detail.ddl" language="sql" />
        </div>
        <CheckpointList :model-name="detail.name" @checkpoint-click="handleCheckpointClick" />
      </template>
    </main>
    <CheckpointDrawer :visible="drawerVisible" :model-name="detail?.name"
      :checkpoint-name="selectedCheckpointName" @close="drawerVisible = false" />
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import ContentState from '../components/ContentState.vue'
import CheckpointList from '../components/CheckpointList.vue'
import CheckpointDrawer from '../components/CheckpointDrawer.vue'
import CodeBlock from '../components/CodeBlock.vue'
import { useCatalogDetail } from '../composables/useCatalogDetail.js'

const route = useRoute()
const drawerVisible = ref(false)
const selectedCheckpointName = ref('')
const {
  items, detail, selectedItem, listLoading, detailLoading,
  listError, detailError, loadList, loadDetail, selectItem
} = useCatalogDetail({
  listUrl: '/ui/api/models',
  detailUrl: name => `/ui/api/models/${name}`,
  routeName: 'ModelDetail',
  mapDetail: (item, data) => ({ ...item, tableData: data.tableData, ddl: data.ddl || null })
})

const handleCheckpointClick = checkpointName => {
  selectedCheckpointName.value = checkpointName
  drawerVisible.value = true
}
watch(() => route.params.id, () => { drawerVisible.value = false })
</script>

<style scoped>
.view-container { display: flex; height: calc(100svh - var(--header-height)); }
.detail-wrapper { position: relative; flex: 1; min-width: 0; overflow-y: auto; background: var(--page-bg); text-align: left; }
.code-section { width: 100%; max-width: var(--content-max-width); margin: 0 auto; padding: 0 var(--page-padding) var(--section-gap); }
@media (max-width: 720px) {
  .view-container { flex-direction: column; }
  .detail-wrapper { min-height: 0; }
}
</style>
