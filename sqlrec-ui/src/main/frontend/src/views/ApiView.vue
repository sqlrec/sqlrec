<template>
  <div class="view-container">
    <Sidebar search-label="APIs" :items="items" :selected-id="selectedItem?.name"
      :loading="listLoading" :error="listError" @select="selectItem" @retry="loadList" />
    <main class="detail-wrapper">
      <ContentState v-if="listLoading" message="Loading APIs..." />
      <ContentState v-else-if="listError" :message="listError" action="Retry" @action="loadList" />
      <ContentState v-else-if="!selectedItem && !detailError" plain message="Select an API from the sidebar to view its details" />
      <ContentState v-else-if="detailLoading" message="Loading API details..." />
      <ContentState v-else-if="detailError" :message="detailError" action="Retry" @action="loadDetail" />
      <DetailPanel v-else :item="detail" />
    </main>
  </div>
</template>

<script setup>
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import ContentState from '../components/ContentState.vue'
import { useCatalogDetail } from '../composables/useCatalogDetail.js'

const {
  items, detail, selectedItem, listLoading, detailLoading,
  listError, detailError, loadList, loadDetail, selectItem
} = useCatalogDetail({
  listUrl: '/ui/api/apis',
  detailUrl: name => `/ui/api/apis/${name}`,
  routeName: 'ApiDetail',
  mapDetail: (item, data) => ({ ...item, tableData: data.tableData })
})
</script>

<style scoped>
.view-container { display: flex; height: calc(100svh - var(--header-height)); }
.detail-wrapper { position: relative; flex: 1; min-width: 0; overflow-y: auto; background: var(--page-bg); }
@media (max-width: 720px) {
  .view-container { flex-direction: column; }
  .detail-wrapper { min-height: 0; }
}
</style>
