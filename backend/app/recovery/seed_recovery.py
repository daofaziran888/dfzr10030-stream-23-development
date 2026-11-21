"""恢复子系统示例数据：构造一个可演示全部约束的 v1 -> v2 场景。

场景设计（确定性数据，测试也直接依赖）：

* 流水线 PIPE-RECO-01 有两个有依赖次序的算子：OP-A（上游） -> OP-B（下游）；
* 结构演进 EVO-0001 把状态格式 v1 转成 v2，字段映射 rename/default/drop 都覆盖；
* 三个旧检查点：两个 v1 可恢复（链路可达在线版本），一个 v0 不可恢复；
* 活动位点与导出范围用于上线后的核对演示。
"""
from __future__ import annotations

from app.recovery import repository as repo

FIELD_MAPPING_V1_TO_V2 = [
    {"op": "rename", "source": "order_id", "target": "order_no"},
    {"op": "keep", "source": "amount", "target": "amount"},
    {"op": "default", "source": "channel", "target": "channel", "value": "unknown"},
    {"op": "drop", "source": "legacy_tag"},
]


def bootstrap() -> None:
    """幂等引导：内部表已有数据时不重复灌入。"""
    if repo.table("_recovery_operators"):
        return

    # 两个算子，OP-B 依赖 OP-A；初始只有 v1 在线，v2 待切换。
    repo.insert("_recovery_operators", {
        "operator_id": "OP-A", "pipeline_id": "PIPE-RECO-01", "depends_on": [],
        "versions": {"v1": "在线", "v2": "已下线"}})
    repo.insert("_recovery_operators", {
        "operator_id": "OP-B", "pipeline_id": "PIPE-RECO-01", "depends_on": ["OP-A"],
        "versions": {"v1": "在线", "v2": "已下线"}})

    # 结构演进与旧检查点各自登记。
    evolution = repo.insert("_recovery_evolutions", {
        "evolution_code": "EVO-0001", "pipeline_id": "PIPE-RECO-01",
        "from_version": "v1", "to_version": "v2", "change_type": "字段改名+补默认值+删冗余",
        "field_mapping": FIELD_MAPPING_V1_TO_V2,
        "status": "已登记", "effective_scope": [], "consumed_batch_id": None})

    checkpoints = [
        ("RECO-0001", "v1", {"p0": 100, "p1": 80},
         {"order_id": "ORD-1", "amount": 128.5, "channel": "app", "legacy_tag": "x"}),
        ("RECO-0002", "v1", {"p0": 120, "p1": 90},
         {"order_id": "ORD-2", "amount": 56.0, "channel": "小程序", "legacy_tag": "y"}),
        # v0 没有任何演进记录能通向在线版本，用于演示不可恢复判定。
        ("RECO-0003", "v0", {"p0": 60, "p1": 40},
         {"oid": "OLD-9", "sum": 12.0}),
    ]
    for code, version, positions, payload in checkpoints:
        checkpoint = repo.insert("_recovery_checkpoints", {
            "checkpoint_code": code, "pipeline_id": "PIPE-RECO-01",
            "state_version": version, "status": "已登记",
            "position_time": "2026-11-10T08:00:00Z",
            "recoverable": None, "recovery_path": [], "blocked_reason": None,
            "active_batch_id": None, "last_load_format": None})
        repo.insert("_recovery_states", {
            "checkpoint_id": checkpoint["id"], "state_version": version,
            "positions": positions, "payload": payload, "staged_payload": None,
            "format_history": [{"version": version, "format": "源格式"}]})

    # 上线后的活动位点（v1 阶段），与数据库导出范围核对用。
    repo.insert("_recovery_active", {
        "pipeline_id": "PIPE-RECO-01", "version": "v1",
        "positions": {"p0": 100, "p1": 80}, "position_time": "2026-11-10T09:00:00Z"})

    return {"evolution_id": evolution["id"]}
