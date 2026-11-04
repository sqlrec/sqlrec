import assert from 'node:assert/strict'
import test from 'node:test'
import { createRenderer, nextTick, reactive } from 'vue'
import { routeLocationKey, routerKey } from 'vue-router'
import { useCatalogDetail } from '../src/composables/useCatalogDetail.js'

// Only lifecycle hooks and injected routes are needed; no browser DOM is involved.
const { createApp } = createRenderer({
  createComment: () => ({}), insert() {}, remove() {}, parentNode: () => null,
  nextSibling: () => null
})
const flush = () => new Promise(resolve => setImmediate(resolve))

function mountCatalog(t, id) {
  const requests = []
  t.mock.method(globalThis, 'fetch', (url, { signal }) => new Promise((resolve, reject) => {
    // Deliberately allow aborted requests to finish to exercise stale-response guards.
    requests.push({ url, signal, reject, respond: data => resolve({ ok: true, json: async () => data }) })
  }))
  const route = reactive({ params: { id } })
  const push = t.mock.fn()
  let catalog
  const app = createApp({
    setup() {
      catalog = useCatalogDetail({
        listUrl: '/list', detailUrl: name => `/detail/${name}`, routeName: 'ModelDetail',
        mapDetail: (item, data) => ({ ...item, tableData: data.tableData })
      })
      return () => null
    }
  })
  app.provide(routeLocationKey, route)
  app.provide(routerKey, { push })
  app.mount({})
  t.after(() => app.unmount())
  return { catalog, route, requests, push, app }
}

test('loads the list and selected detail, encodes names and preserves navigation', async t => {
  const name = '模型 /?#%'
  const { catalog, requests, push } = mountCatalog(t, name)
  assert.equal(catalog.listLoading.value, true)
  requests[0].respond([{ name }])
  await flush()
  assert.equal(catalog.listLoading.value, false)
  assert.equal(catalog.detailLoading.value, true)
  assert.equal(requests[1].url, `/detail/${encodeURIComponent(name)}`)
  requests[1].respond({ tableData: [] })
  await flush()
  assert.deepEqual(catalog.detail.value, { name, tableData: [] })
  assert.equal(catalog.detailLoading.value, false)
  catalog.selectItem({ name })
  assert.deepEqual(push.mock.calls[0].arguments, [{ name: 'ModelDetail', params: { id: name } }])
})

test('no selection and unknown items do not send detail requests', async t => {
  const { catalog, route, requests } = mountCatalog(t)
  requests[0].respond([{ name: 'a' }])
  await flush()
  assert.equal(requests.length, 1)
  assert.equal(catalog.detailError.value, '')
  route.params.id = 'missing'
  await nextTick()
  assert.equal(catalog.detailError.value, 'Item not found.')
  assert.equal(requests.length, 1)
})

test('list and detail errors can be retried', async t => {
  const { catalog, requests } = mountCatalog(t, 'a')
  requests[0].reject(new Error('Network error'))
  await flush()
  assert.equal(catalog.listError.value, 'Failed to load the list. Please retry.')
  const retry = catalog.loadList()
  requests[1].respond([{ name: 'a' }])
  await retry
  assert.equal(catalog.listError.value, '')
  requests[2].reject(new SyntaxError('Invalid JSON'))
  await flush()
  assert.equal(catalog.detailError.value, 'Failed to load details. Please retry.')
  const detailRetry = catalog.loadDetail()
  requests[3].respond({ tableData: [] })
  await detailRetry
  assert.equal(catalog.detailError.value, '')
  assert.equal(catalog.detail.value.name, 'a')
})

test('rapid selection ignores an older detail response and keeps the latest loading state', async t => {
  const { catalog, route, requests } = mountCatalog(t, 'a')
  requests[0].respond([{ name: 'a' }, { name: 'b' }])
  await flush()
  route.params.id = 'b'
  await nextTick()
  assert.equal(requests[1].signal.aborted, true)
  requests[1].respond({ tableData: [{ col_name: 'old' }] })
  await flush()
  assert.equal(catalog.detail.value, null)
  assert.equal(catalog.detailLoading.value, true)
  requests[2].respond({ tableData: [] })
  await flush()
  assert.equal(catalog.detail.value.name, 'b')
  assert.equal(catalog.detailLoading.value, false)
})

test('an older list request cannot replace the latest list or loading state', async t => {
  const { catalog, requests } = mountCatalog(t)
  const retry = catalog.loadList()
  assert.equal(requests[0].signal.aborted, true)
  requests[0].respond([{ name: 'old' }])
  await flush()
  assert.deepEqual(catalog.items.value, [])
  assert.equal(catalog.listLoading.value, true)
  requests[1].respond([{ name: 'new' }])
  await retry
  assert.deepEqual(catalog.items.value, [{ name: 'new' }])
  assert.equal(catalog.listLoading.value, false)
})

test('unmount aborts a pending detail and ignores its late rejection', async t => {
  const { catalog, requests, app } = mountCatalog(t, 'a')
  requests[0].respond([{ name: 'a' }])
  await flush()
  app.unmount()
  requests[1].reject(new Error('Late failure'))
  await flush()
  assert.equal(requests[0].signal.aborted, true)
  assert.equal(requests[1].signal.aborted, true)
  assert.equal(catalog.detail.value, null)
  assert.equal(catalog.detailError.value, '')
})
