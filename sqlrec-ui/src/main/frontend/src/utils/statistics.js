export function calculateZScores(nodes, valueKey, scoreKey) {
  const values = nodes.map(node => node[valueKey])
    .filter(value => value !== null && value !== undefined && value >= 0)

  if (values.length < 2) {
    return nodes.map(node => ({ ...node, [scoreKey]: 0 }))
  }

  const mean = values.reduce((sum, value) => sum + value, 0) / values.length
  const variance = values.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / values.length
  const stdDev = Math.sqrt(variance)

  return nodes.map(node => {
    const value = node[valueKey]
    const score = stdDev === 0 || value === null || value === undefined || value < 0
      ? 0
      : (value - mean) / stdDev
    return { ...node, [scoreKey]: score }
  })
}
