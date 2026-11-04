import assert from 'node:assert/strict'
import test from 'node:test'
import { calculateZScores } from '../src/utils/statistics.js'

const score = values => calculateZScores(values.map(value => ({ value })), 'value', 'score')
  .map(node => node.score)

test('z-scores use the population standard deviation and include zero', () => {
  const result = score([0, 2, 4])
  assert.ok(Math.abs(result[0] + Math.sqrt(1.5)) < 1e-12)
  assert.equal(result[1], 0)
  assert.ok(Math.abs(result[2] - Math.sqrt(1.5)) < 1e-12)
})

test('empty, single-value and constant samples have zero scores', () => {
  for (const values of [[], [3], [3, 3, 3], [null, -1, 3]]) {
    assert.deepEqual(score(values), values.map(() => 0))
  }
})

test('missing and negative values do not affect valid scores', () => {
  assert.deepEqual(score([null, undefined, -1, 0, 2]), [0, 0, 0, -1, 1])
})

test('non-finite values retain the original calculation behavior', () => {
  assert.deepEqual(score([0, 2, NaN]), [-1, 1, NaN])
  assert.deepEqual(score([0, Infinity, null]), [NaN, NaN, 0])
})

test('execution time and data count are independent and input nodes are unchanged', () => {
  const nodes = Object.freeze([
    Object.freeze({ id: 'a', avgExecTimeMs: -1, avgDataCount: 10 }),
    Object.freeze({ id: 'b', avgExecTimeMs: 5, avgDataCount: 30 })
  ])
  const times = calculateZScores(nodes, 'avgExecTimeMs', 'zScore')
  const result = calculateZScores(times, 'avgDataCount', 'dataCountZScore')
  assert.deepEqual(result, [
    { ...nodes[0], zScore: 0, dataCountZScore: -1 },
    { ...nodes[1], zScore: 0, dataCountZScore: 1 }
  ])
  assert.notEqual(result[0], nodes[0])
  assert.equal('zScore' in nodes[0], false)
})
