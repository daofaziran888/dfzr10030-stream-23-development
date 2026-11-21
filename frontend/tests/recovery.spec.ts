import { expect, request as playwrightRequest, test } from '@playwright/test'

/**
 * 恢复检查点状态转换端到端测试。
 *
 * 后端用 seed_recovery.bootstrap() 引导 PIPE-RECO-01 场景：
 * OP-A -> OP-B 两个有依赖次序的算子，初始 v1 在线 / v2 下线，三个检查点。
 * 用例串行执行；会产生新版本数据的崩溃恢复用例，通过 API 预置独立的
 * 全新 v1 检查点，避免与前面用例的版本转换互相干扰。
 */
const API = '/api/recovery'

async function freshV1Checkpoints(requestContext: import('@playwright/test').APIRequestContext, count: number): Promise<number[]> {
  const ids: number[] = []
  for (let i = 0; i < count; i += 1) {
    const code = `RECO-E2E-${Date.now()}-${i}`
    const resp = await requestContext.post(`${API}/checkpoints`, {
      data: {
        checkpoint_code: code, pipeline_id: 'PIPE-RECO-01', state_version: 'v1',
        positions: { p0: 10 + i, p1: 20 + i },
        payload: { order_id: `ORD-E2E-${i}`, amount: 1.5, channel: 'app', legacy_tag: 't' },
      },
    })
    const body = await resp.json()
    ids.push(body.data.id)
  }
  return ids
}

async function lastBatchId(page: import('@playwright/test').Page): Promise<string> {
  const row = page.locator('[data-testid^="batch-row-"]').last()
  return (await row.getAttribute('data-testid'))!.split('-').pop()!
}

test.describe('恢复检查点状态转换', () => {
  test('清单页加载并完成可恢复性判定', async ({ page }) => {
    const errors: string[] = []
    page.on('pageerror', (e) => errors.push(e.message))
    page.on('response', (r) => {
      if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`)
    })

    await page.goto('/recovery')
    await expect(page.getByTestId('tab-checkpoints')).toBeVisible()
    await expect(page.locator('[data-testid^="cp-row-"]').count()).resolves.toBeGreaterThanOrEqual(3)

    await page.getByTestId('assess-all').click()
    // 种子里两个 v1 检查点可恢复，一个 v0 检查点不可恢复。
    await expect(page.getByTestId('cp-recoverable-1')).toHaveText('是')
    await expect(page.getByTestId('cp-recoverable-3')).toHaveText('否')
    expect(errors).toEqual([])
  })

  test('旧恢复排队时结构切换被冲突闸门拒绝，排空后放行', async ({ page }) => {
    await page.goto('/recovery')
    await page.getByTestId('assess-all').click()
    await page.getByTestId('enqueue-1').click()

    await page.getByTestId('tab-ops').click()
    const queueId = (await page.locator('.data-table tbody tr td').first().textContent()) ?? ''
    await page.getByTestId('cutover-version').fill('v2')
    await page.getByTestId('cutover').click()
    await expect(page.getByTestId('message')).toContainText('版本切换与恢复时效冲突')

    await page.getByTestId('queue-entry-ids').fill(queueId)
    await page.getByTestId('process-recovery').click()
    await page.getByTestId('cutover').click()
    await expect(page.getByTestId('message')).toContainText('新结构开始接收数据')
  })

  test('依赖未就绪时批次校验拒绝，按次序切换算子后转换提交', async ({ page }) => {
    await page.goto('/recovery')
    await page.getByTestId('tab-batches').click()
    await page.getByTestId('batch-cp-ids').fill('1,2')
    await page.getByTestId('create-batch').click()

    const firstId = await lastBatchId(page)
    await page.getByTestId(`validate-${firstId}`).click()
    await expect(page.getByTestId(`batch-status-${firstId}`)).toContainText('已失败')

    await page.getByTestId('tab-ops').click()
    await page.getByTestId('switch-validate').click()
    await expect(page.getByTestId('switch-result')).toContainText('拒绝切换')

    await page.getByRole('button', { name: '切换 OP-A' }).click()
    await page.getByRole('button', { name: '切换 OP-B' }).click()
    await page.getByTestId('switch-validate').click()
    await expect(page.getByTestId('switch-result')).toContainText('允许切换')

    await page.getByTestId('tab-batches').click()
    await page.getByTestId('create-batch').click()
    const retryId = await lastBatchId(page)
    await page.getByTestId(`validate-${retryId}`).click()
    await page.getByTestId(`convert-${retryId}`).click()
    await expect(page.getByTestId(`batch-status-${retryId}`)).toHaveText('已提交')
  })

  test('半转换崩溃后重启恢复，再读回完整恢复批次', async ({ page }) => {
    // 预置两个全新的 v1 检查点，避免受前面用例版本转换影响。
    const requestContext = await playwrightRequest.newContext({ baseURL: page.url().replace('/recovery', '') })
    const checkpointIds = await freshV1Checkpoints(requestContext, 2)
    await requestContext.dispose()

    await page.goto('/recovery')
    await page.getByTestId('tab-batches').click()
    await page.getByTestId('batch-cp-ids').fill(checkpointIds.join(','))
    await page.getByTestId('create-batch').click()
    const batchId = await lastBatchId(page)
    await page.getByTestId(`validate-${batchId}`).click()

    await page.getByRole('button', { name: '模拟半转换崩溃' }).last().click()
    await expect(page.getByTestId('message')).toContainText('转换一半停止服务')
    await expect(page.getByTestId(`batch-status-${batchId}`)).toHaveText('转换中')

    await page.getByTestId('recover-inflight').click()
    await expect(page.getByTestId(`batch-status-${batchId}`)).toHaveText('已回滚')

    // 重新发起的完整批次可以整体提交，且读回的是完整恢复批次。
    await page.getByTestId('create-batch').click()
    const retryId = await lastBatchId(page)
    await page.getByTestId(`convert-${retryId}`).click()
    await expect(page.getByTestId(`batch-status-${retryId}`)).toHaveText('已提交')
  })

  test('活动位点与数据库导出输入范围核对', async ({ page }) => {
    await page.goto('/recovery')
    await page.getByTestId('tab-ops').click()
    await page.getByTestId('reconcile-ranges').fill(
      '[{"partition":"p0","start":0,"end":120},{"partition":"p1","start":0,"end":80}]')
    await page.getByTestId('reconcile').click()
    await expect(page.getByTestId('reconcile-result')).toBeVisible()
    // 种子活动位点 p0=100 相对上界 120 存在缺口，p1=80 全覆盖。
    await expect(page.getByTestId('reconcile-result')).toContainText('存在缺口')
    await expect(page.getByTestId('reconcile-result')).toContainText('已覆盖')
  })
})
