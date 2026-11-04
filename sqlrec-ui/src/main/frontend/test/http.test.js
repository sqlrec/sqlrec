import assert from 'node:assert/strict'
import test from 'node:test'
import { fetchJson } from '../src/utils/http.js'

test('fetchJson forwards the URL and abort signal and returns parsed data', async t => {
  const controller = new AbortController()
  const data = { tableData: [] }
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(url, '/ui/api/models/example')
    assert.equal(options.signal, controller.signal)
    return { ok: true, json: async () => data }
  })
  assert.equal(await fetchJson('/ui/api/models/example', controller.signal), data)
})

test('fetchJson rejects HTTP errors without parsing the response body', async t => {
  t.mock.method(globalThis, 'fetch', async () => ({
    ok: false,
    status: 503,
    json: () => assert.fail('An error response must not be parsed')
  }))
  await assert.rejects(fetchJson('/ui/api/models'), { message: 'HTTP 503' })
})

test('fetchJson preserves network, JSON and cancellation errors', async t => {
  for (const error of [new TypeError('Network error'), new SyntaxError('Invalid JSON'), new DOMException('Aborted', 'AbortError')]) {
    const fetchMock = t.mock.method(globalThis, 'fetch', async () => {
      if (error instanceof SyntaxError) return { ok: true, json: async () => { throw error } }
      throw error
    })
    await assert.rejects(fetchJson('/ui/api/models'), reason => reason === error)
    fetchMock.mock.restore()
  }
})
