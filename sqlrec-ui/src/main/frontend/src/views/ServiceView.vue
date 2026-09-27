<template>
  <div class="view-container">
    <Sidebar search-label="services" :items="items" :selected-id="selectedItem?.name"
      :loading="listLoading" :error="listError" @select="selectItem" @retry="loadList" />
    <main class="detail-wrapper">
      <ContentState v-if="listLoading" message="Loading services..." />
      <ContentState v-else-if="listError" :message="listError" action="Retry" @action="loadList" />
      <ContentState v-else-if="!selectedItem && !detailError" plain message="Select a service from the sidebar to view its details" />
      <ContentState v-else-if="detailLoading" message="Loading service details..." />
      <ContentState v-else-if="detailError" :message="detailError" action="Retry" @action="loadDetail" />
      <template v-else-if="detail">
        <DetailPanel :item="detail" />
        <div v-if="detail.ddl" class="code-section">
          <CodeBlock title="DDL" :code="detail.ddl" language="sql" />
        </div>
        <div v-if="detail.yaml" class="code-section">
          <CodeBlock title="K8s YAML" :code="detail.yaml" language="yaml" />
        </div>
      </template>
    </main>
  </div>
</template>

<script setup>
import Sidebar from '../components/Sidebar.vue'
import DetailPanel from '../components/DetailPanel.vue'
import ContentState from '../components/ContentState.vue'
import CodeBlock from '../components/CodeBlock.vue'
import { useCatalogDetail } from '../composables/useCatalogDetail.js'

const {
  items, detail, selectedItem, listLoading, detailLoading,
  listError, detailError, loadList, loadDetail, selectItem
} = useCatalogDetail({
  listUrl: '/ui/api/services',
  detailUrl: name => `/ui/api/services/${name}`,
  routeName: 'ServiceDetail',
  mapDetail: (item, data) => ({
    ...item, tableData: data.tableData, yaml: data.yaml || null, ddl: data.ddl || null
  })
})
</script>

<style scoped>
.view-container { display: flex; height: calc(100svh - var(--header-height)); }
.detail-wrapper { position: relative; flex: 1; min-width: 0; overflow-y: auto; background: var(--page-bg); text-align: left; }
.code-section { width: 100%; max-width: var(--content-max-width); margin: 0 auto; padding: 0 var(--page-padding) var(--section-gap); }
.code-section:last-child { padding-bottom: var(--page-padding); }
@media (max-width: 720px) {
  .view-container { flex-direction: column; }
  .detail-wrapper { min-height: 0; }
}
</style>
