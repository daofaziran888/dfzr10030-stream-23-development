<template>
  <section class="page" data-module="recovery">
    <header class="page-head">
      <div>
        <h2>恢复检查点状态转换</h2>
        <p class="page-desc">
          旧检查点位点与结构演进各自登记，按状态格式转换批次管理结构切换；
          同批数据整体提交或整体回滚，恢复批只装载一套内部兼容状态。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="assess-all" type="button" @click="assessAll">重新判定可恢复性</button>
        <button class="btn" data-testid="reload" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <div class="tab-row">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="btn"
        :class="{ primary: activeTab === tab.key }"
        type="button"
        :data-testid="`tab-${tab.key}`"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
      </button>
    </div>

    <p v-if="message" class="error-text" data-testid="message">{{ message }}</p>

    <!-- 标签页一：恢复检查点清单 -->
    <template v-if="activeTab === 'checkpoints'">
      <table class="data-table">
        <thead>
          <tr>
            <th>检查点编号</th><th>流水线</th><th>状态版本</th><th>状态</th>
            <th>可恢复</th><th>格式路径</th><th>实际装载格式</th><th>阻塞原因</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in checkpoints" :key="row.id" :data-testid="`cp-row-${row.id}`">
            <td>{{ row.checkpoint_code }}</td>
            <td>{{ row.pipeline_id }}</td>
            <td>{{ row.state_version }}</td>
            <td>{{ row.status }}</td>
            <td :data-testid="`cp-recoverable-${row.id}`">{{ formatRecoverable(row.recoverable) }}</td>
            <td>{{ (row.recovery_path || []).join(' → ') || '—' }}</td>
            <td>{{ row.last_load_format || '—' }}</td>
            <td>{{ row.blocked_reason || '—' }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="assessOne(row.id)">判定</button>
              <button class="link" type="button" :data-testid="`enqueue-${row.id}`" @click="enqueue(row.id)">排队恢复</button>
            </td>
          </tr>
        </tbody>
      </table>
    </template>

    <!-- 标签页二：状态格式转换批次 -->
    <template v-if="activeTab === 'batches'">
      <form class="filter-bar" @submit.prevent="createBatch">
        <label class="filter-item">
          <span>检查点ID（逗号分隔，需同一源版本）</span>
          <input v-model="batchForm.checkpointIds" data-testid="batch-cp-ids" placeholder="如 1,2" required />
        </label>
        <button class="btn primary" data-testid="create-batch" type="submit">创建转换批次</button>
      </form>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次号</th><th>流水线</th><th>源→目标版本</th><th>状态</th>
            <th>生效范围</th><th>条目明细</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="batch.id" :data-testid="`batch-row-${batch.id}`">
            <td>{{ batch.batch_code }}</td>
            <td>{{ batch.pipeline_id }}</td>
            <td>{{ batch.from_version }} → {{ batch.to_version }}</td>
            <td :data-testid="`batch-status-${batch.id}`">{{ batch.status }}</td>
            <td>{{ (batch.effective_scope || []).join(', ') || '—' }}</td>
            <td>
              <span v-for="item in batch.items" :key="item.id" class="tag">
                #{{ item.checkpoint_id }}:{{ item.status }}
              </span>
            </td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`validate-${batch.id}`" @click="validateBatch(batch.id)">依赖校验</button>
              <button class="link" type="button" :data-testid="`convert-${batch.id}`" @click="convertBatch(batch.id)">转换提交</button>
              <button class="link" type="button" @click="crashConvert(batch.id)">模拟半转换崩溃</button>
              <button class="link" type="button" @click="rollbackBatch(batch.id)">整体回滚</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="page-actions">
        <button class="btn" data-testid="recover-inflight" type="button" @click="recoverInflight">重启恢复未提交批次</button>
      </div>
    </template>

    <!-- 标签页三：恢复队列、切换验证与位点核对 -->
    <template v-if="activeTab === 'ops'">
      <h3>恢复队列</h3>
      <table class="data-table">
        <thead>
          <tr><th>ID</th><th>检查点ID</th><th>流水线</th><th>钉住版本</th><th>状态</th><th>装载格式</th></tr>
        </thead>
        <tbody>
          <tr v-for="q in queue" :key="q.id">
            <td>{{ q.id }}</td><td>{{ q.checkpoint_id }}</td><td>{{ q.pipeline_id }}</td>
            <td>{{ q.pinned_version }}</td><td>{{ q.status }}</td><td>{{ q.load_format || '—' }}</td>
          </tr>
          <tr v-if="!queue.length"><td colspan="6" class="empty-state">队列为空</td></tr>
        </tbody>
      </table>
      <form class="filter-bar" @submit.prevent="processRecoveryBatch">
        <label class="filter-item">
          <span>队列条目ID（逗号分隔，需同一钉住版本）</span>
          <input v-model="queueForm.entryIds" data-testid="queue-entry-ids" placeholder="如 1,2" required />
        </label>
        <button class="btn primary" data-testid="process-recovery" type="submit">按一套兼容状态装载</button>
      </form>

      <h3>有序算子切换验证</h3>
      <form class="filter-bar" @submit.prevent="validateSwitch">
        <label class="filter-item"><span>上游算子</span><input v-model="switchForm.upstream" data-testid="switch-up" placeholder="OP-A" required /></label>
        <label class="filter-item"><span>下游算子</span><input v-model="switchForm.downstream" data-testid="switch-down" placeholder="OP-B" required /></label>
        <label class="filter-item"><span>新版本</span><input v-model="switchForm.version" data-testid="switch-version" placeholder="v2" required /></label>
        <button class="btn primary" data-testid="switch-validate" type="submit">验证依赖次序</button>
        <button class="btn" type="button" @click="executeSwitch('OP-A')">切换 OP-A</button>
        <button class="btn" type="button" @click="executeSwitch('OP-B')">切换 OP-B</button>
      </form>
      <p v-if="switchResult" data-testid="switch-result">
        结论：{{ switchResult.decision }}；顺序：{{ switchResult.switch_order.join(' → ') }}；
        阻塞：{{ switchResult.blockers.join('；') || '无' }}
      </p>

      <h3>结构切换与位点核对</h3>
      <form class="filter-bar" @submit.prevent="cutover">
        <label class="filter-item"><span>流水线</span><input v-model="cutoverForm.pipelineId" required /></label>
        <label class="filter-item"><span>接收数据的新版本</span><input v-model="cutoverForm.newVersion" data-testid="cutover-version" placeholder="v2" required /></label>
        <button class="btn primary" data-testid="cutover" type="submit">新结构开始接收数据</button>
      </form>
      <form class="filter-bar" @submit.prevent="reconcile">
        <label class="filter-item">
          <span>数据库导出输入范围 JSON</span>
          <input v-model="reconcileForm.ranges" data-testid="reconcile-ranges"
                 placeholder='[{"partition":"p0","start":0,"end":220}]' required style="min-width: 320px" />
        </label>
        <button class="btn primary" data-testid="reconcile" type="submit">核对活动位点</button>
      </form>
      <table v-if="reconcileResult" class="data-table" data-testid="reconcile-result">
        <thead><tr><th>分区</th><th>导出范围</th><th>活动位点</th><th>状态</th><th>缺口</th></tr></thead>
        <tbody>
          <tr v-for="p in reconcileResult.partitions" :key="p.partition">
            <td>{{ p.partition }}</td><td>{{ p.exported_range.join(' ~ ') }}</td>
            <td>{{ p.active_position ?? '—' }}</td><td>{{ p.status }}</td><td>{{ p.gap }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <footer class="page-foot">
      <span>恢复检查点 {{ checkpoints.length }} 个 · 转换批次 {{ batches.length }} 个 · 队列 {{ queue.length }} 条</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

const API = '/api/recovery'

type ApiResult<T = unknown> = { ok: boolean; message: string; data: T }

interface Checkpoint {
  id: number
  checkpoint_code: string
  pipeline_id: string
  state_version: string
  status: string
  recoverable: boolean | null
  recovery_path: string[]
  last_load_format: string | null
  blocked_reason: string | null
}
interface BatchItem { id: number; checkpoint_id: number; status: string }
interface Batch {
  id: number
  batch_code: string
  pipeline_id: string
  from_version: string
  to_version: string
  status: string
  effective_scope: number[]
  items: BatchItem[]
}
interface QueueEntry {
  id: number
  checkpoint_id: number
  pipeline_id: string
  pinned_version: string
  status: string
  load_format: string | null
}
interface Overview {
  checkpoint_total: number
  recoverable: number
  unrecoverable: number
  batch_total: number
  queue_pending: number
}
interface SwitchResult {
  decision: string
  switch_order: string[]
  blockers: string[]
}
interface ReconcilePartition {
  partition: string
  exported_range: number[]
  active_position: number | null
  status: string
  gap: number
}
interface ReconcileResult {
  all_covered: boolean
  partitions: ReconcilePartition[]
}

const checkpoints = ref<Checkpoint[]>([])
const batches = ref<Batch[]>([])
const queue = ref<QueueEntry[]>([])
const overview = ref<Overview>({ checkpoint_total: 0, recoverable: 0, unrecoverable: 0, batch_total: 0, queue_pending: 0 })
const message = ref('')
const activeTab = ref<'checkpoints' | 'batches' | 'ops'>('checkpoints')
const tabs = [
  { key: 'checkpoints' as const, label: '恢复检查点清单' },
  { key: 'batches' as const, label: '状态格式转换批次' },
  { key: 'ops' as const, label: '恢复队列与切换核对' },
]

const batchForm = ref({ checkpointIds: '1,2' })
const queueForm = ref({ entryIds: '' })
const switchForm = ref({ upstream: 'OP-A', downstream: 'OP-B', version: 'v2' })
const switchResult = ref<SwitchResult | null>(null)
const cutoverForm = ref({ pipelineId: 'PIPE-RECO-01', newVersion: 'v2' })
const reconcileForm = ref({ ranges: '[{"partition":"p0","start":0,"end":220},{"partition":"p1","start":0,"end":150}]' })
const reconcileResult = ref<ReconcileResult | null>(null)

const stats = computed(() => [
  { label: '检查点总数', value: overview.value.checkpoint_total },
  { label: '可恢复', value: overview.value.recoverable },
  { label: '不可恢复', value: overview.value.unrecoverable },
  { label: '转换批次', value: overview.value.batch_total },
  { label: '排队/处理中', value: overview.value.queue_pending },
])

function formatRecoverable(value: boolean | null): string {
  if (value === true) return '是'
  if (value === false) return '否'
  return '待判定'
}

async function call<T>(path: string, init?: RequestInit, successMessage?: string): Promise<T | null> {
  message.value = ''
  try {
    const response = await request(`${API}${path}`, init)
    if (!response.ok) throw new Error(`接口返回 ${response.status}`)
    const payload = (await response.json()) as ApiResult<T>
    if (!payload.ok) throw new Error(payload.message)
    if (successMessage) message.value = successMessage
    await reloadAll()
    return payload.data
  } catch (error) {
    message.value = error instanceof Error ? error.message : '操作失败'
    return null
  }
}

function post<T = unknown>(path: string, body?: unknown, successMessage?: string): Promise<T | null> {
  return call<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined }, successMessage)
}

async function reloadAll() {
  const [cp, ba, qu, ov] = await Promise.all([
    fetchJson<ApiResult<Checkpoint[]>>(`${API}/checkpoints`),
    fetchJson<ApiResult<Batch[]>>(`${API}/batches`),
    fetchJson<ApiResult<QueueEntry[]>>(`${API}/queue`),
    fetchJson<Overview>(`${API}/overview`),
  ])
  checkpoints.value = cp.data ?? []
  batches.value = ba.data ?? []
  queue.value = qu.data ?? []
  overview.value = ov
}

function assessAll() {
  void post('/assess-all', undefined, '可恢复性判定已刷新')
}
function assessOne(id: number) {
  void post(`/checkpoints/${id}/assess`)
}
function enqueue(id: number) {
  void post(`/queue/${id}`)
}
function createBatch() {
  const ids = batchForm.value.checkpointIds.split(',').map((s) => Number(s.trim())).filter(Boolean)
  void post('/batches', { pipeline_id: 'PIPE-RECO-01', evolution_id: 1, checkpoint_ids: ids }, '批次已创建')
}
function validateBatch(id: number) {
  void post(`/batches/${id}/validate`)
}
function convertBatch(id: number) {
  void post(`/batches/${id}/convert`, undefined, '批次已整体转换提交')
}
function crashConvert(id: number) {
  void call(`/batches/${id}/convert?crash_after_items=1`, { method: 'POST' })
}
function rollbackBatch(id: number) {
  void post(`/batches/${id}/rollback`, { reason: '页面触发整体回滚' })
}
function recoverInflight() {
  void post('/batches/recover-inflight', undefined, '未提交批次已整体回滚')
}
function processRecoveryBatch() {
  const ids = queueForm.value.entryIds.split(',').map((s) => Number(s.trim())).filter(Boolean)
  void post('/recovery-batches', { queue_entry_ids: ids }, '恢复批已按一套兼容状态装载')
}
async function validateSwitch() {
  switchResult.value = await post<SwitchResult>('/switch/validate', {
    upstream_id: switchForm.value.upstream,
    downstream_id: switchForm.value.downstream,
    new_version: switchForm.value.version,
  })
}
function executeSwitch(operatorId: string) {
  void post('/switch/execute', { operator_id: operatorId, new_version: switchForm.value.version })
}
function cutover() {
  void post('/cutover', { pipeline_id: cutoverForm.value.pipelineId, new_version: cutoverForm.value.newVersion })
}
async function reconcile() {
  let ranges: unknown
  try {
    ranges = JSON.parse(reconcileForm.value.ranges)
  } catch {
    message.value = '输入范围 JSON 格式不正确'
    return
  }
  reconcileResult.value = await post<ReconcileResult>(
    '/reconcile',
    { pipeline_id: cutoverForm.value.pipelineId, exported_ranges: ranges },
  )
}

onMounted(reloadAll)
</script>
