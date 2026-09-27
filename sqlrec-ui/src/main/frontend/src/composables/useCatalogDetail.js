import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { encodePathSegment } from '../utils/url.js'

export function useCatalogDetail({ listUrl, detailUrl, routeName, mapDetail }) {
  const route = useRoute()
  const router = useRouter()
  const items = ref([])
  const detail = ref(null)
  const listLoading = ref(false)
  const detailLoading = ref(false)
  const listError = ref('')
  const detailError = ref('')
  const selectedItem = computed(() => items.value.find(item => item.name === route.params.id) || null)
  let listController
  let detailController

  const loadDetail = async () => {
    detailController?.abort()
    detailController = undefined
    detail.value = null
    detailError.value = ''
    detailLoading.value = false

    if (!route.params.id || listLoading.value || listError.value) return
    const item = selectedItem.value
    if (!item) {
      detailError.value = 'Item not found.'
      return
    }

    const controller = new AbortController()
    detailController = controller
    detailLoading.value = true
    try {
      const response = await fetch(detailUrl(encodePathSegment(item.name)), { signal: controller.signal })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const data = await response.json()
      if (!controller.signal.aborted) detail.value = mapDetail(item, data)
    } catch (error) {
      if (!controller.signal.aborted) detailError.value = 'Failed to load details. Please retry.'
    } finally {
      if (detailController === controller) detailLoading.value = false
    }
  }

  const loadList = async () => {
    listController?.abort()
    const controller = new AbortController()
    listController = controller
    listLoading.value = true
    listError.value = ''
    try {
      const response = await fetch(listUrl, { signal: controller.signal })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const data = await response.json()
      if (controller.signal.aborted) return
      items.value = data
    } catch (error) {
      if (!controller.signal.aborted) listError.value = 'Failed to load the list. Please retry.'
    } finally {
      if (listController === controller) {
        listLoading.value = false
        loadDetail()
      }
    }
  }

  const selectItem = item => router.push({ name: routeName, params: { id: item.name } })

  watch(() => route.params.id, loadDetail)
  onMounted(loadList)
  onUnmounted(() => {
    listController?.abort()
    detailController?.abort()
  })

  return {
    items, detail, selectedItem, listLoading, detailLoading,
    listError, detailError, loadList, loadDetail, selectItem
  }
}
