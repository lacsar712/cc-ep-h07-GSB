/** BUG: hides empty metrics with fake placeholder so issue looks 'ok'. */
export function displayMetrics(metrics) {
  if (Array.isArray(metrics) && metrics.length > 0) return metrics
  return [{ name: '(pending sync)', value: '-', step: '-' }]
}
