# 管道工坊 · 实时数仓与流式管道平台

面向实时数据接入、流式转换、变更捕获、窗口聚合、状态管理、断流恢复与多维数据集下发的实时数仓与流式管道开发平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 源端绑定 | `source_binding` | 数据源端 | 源端编号、源端类型、连接地址 |
| 变更捕获 | `cdc_task` | CDC任务 | 任务编号、源数据库、监控表 |
| 流式转换 | `stream_transform` | 转换算子 | 算子编号、输入流、输出流 |
| 窗口聚合 | `window_agg` | 时间窗口 | 窗口编号、聚合指标、窗口类型 |
| 状态管理 | `state_store` | 算子状态 | 状态编号、关联算子、存储后端 |
| 规则引擎 | `rule_engine` | 业务规则 | 规则编号、规则条件、触发动作 |
| 水位管理 | `watermark_mgr` | 事件水位 | 水位编号、关联流、水位类型 |
| 数据落盘 | `sink_writer` | 输出槽位 | 槽位编号、输出类型、目标地址 |
| 背压控制 | `backpressure` | 背压策略 | 策略编号、关联算子、队列上限 |
| 断流恢复 | `checkpoint` | 检查点 | 检查点编号、关联流水线、位点时间 |
| 流式质检 | `data_quality` | 质检规则 | 规则编号、关联流、检查项 |
| Schema演进 | `schema_evolve` | 结构变更 | 变更编号、关联流、变更类型 |
| 管道编排 | `pipeline_orch` | 管道DAG | 管道编号、管道名称、节点列表 |
| 去重管理 | `dedup_mgr` | 去重策略 | 策略编号、关联流、去重键 |
| 指标计算 | `metric_calc` | 实时指标 | 指标编号、指标名称、计算公式 |
| 事件路由 | `event_router` | 路由规则 | 规则编号、事件类型、匹配条件 |
| 历史回放 | `replay_batch` | 回放任务 | 任务编号、数据源、起始时间 |
| 分组分区 | `grp_partition` | 分组策略 | 策略编号、关联流、分区键 |

## 恢复检查点状态转换子系统

旧检查点位点与结构演进记录原先各自登记，缺少判断“哪些旧位点还能恢复”的数据。
`app/recovery/` 在既有模块之上补了一套围绕**状态格式转换批次**的恢复控制面，
前端入口为「恢复检查点转换」页面（路由 `/recovery`），接口前缀 `/api/recovery`。

| 需求 | 落地位置 |
| --- | --- |
| 旧位点与结构演进各自登记，并判定可恢复性 | `registry_service.py`：沿演进链 `from→to` 计算格式路径，结合算子版本在线情况标记「可恢复/不可恢复」 |
| 新增恢复检查点状态转换 | `errors.py` 集中状态机；转换结果回写 `_recovery_checkpoints` 清单 |
| 用转换批次确定每次结构切换后能读的数据 | `conversion_service.py`：批次暂存转换、整体提交后才对外可读 |
| 校验流水线上下游依赖、兼容存量位点与字段映射 | `pipeline.py`（DAG、拓扑序、依赖链可用性）+ `field_mapping.py`（keep/rename/default/drop） |
| 回滚只能整体切回完整版本 | `rollback()` 清空全部暂存、丢弃半转换内容；位点在转换中原样保留不漂移 |
| 版本切换与恢复时效冲突 | `coordinator_service.begin_cutover()`：恢复队列未排空时拒绝新结构接收数据 |
| 每个恢复批只装载一套内部兼容状态 | `process_recovery_batch()`：校验整批同一钉住版本，混装/依赖离线整批拒绝 |
| 已完成旧恢复按实际装载格式追查 | 状态快照 `format_history` 与检查点 `last_load_format` 全程留痕 |
| 演进消费批次校验实际生效范围 | `consume_batch_for_evolution()`：批次范围超出演进声明即拒绝 |
| 有依赖次序的两个算子切换验证 | `validate_ordered_switch()`：上游不先就绪则拒绝，输出允许/拒绝结论 |
| 转换一半停止服务，重启读回完整批次 | `run_conversion(crash_after_items=)` 模拟崩溃，`recover_inflight()` 重启整体回滚后可重发 |
| 核对活动位点与数据库导出输入范围 | `reconcile_positions()`：识别缺口、倒退、缺失与全覆盖 |

子系统测试：

```bash
cd backend && .venv/bin/python -m pytest tests/test_recovery_*.py   # 38 个领域用例
cd frontend && npx playwright test tests/recovery.spec.ts           # 5 个端到端场景
```

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
