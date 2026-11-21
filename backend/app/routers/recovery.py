"""恢复检查点状态转换接口。

把领域服务的能力暴露成 REST 接口；业务规则全部在 services 里，
路由层只负责参数透传与把 :class:`RecoveryError` 翻译成统一动作契约。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.recovery.errors import RecoveryError
from app.recovery.service import bootstrap, conversion, coordinator, registry

router = APIRouter(prefix="/api/recovery", tags=["恢复检查点状态转换"])


class Ok(BaseModel):
    ok: bool
    message: str
    data: dict[str, Any] | list[Any] | None = None


def _run(message: str, fn) -> Ok:
    try:
        return Ok(ok=True, message=message, data=fn())
    except RecoveryError as exc:
        return Ok(ok=False, message=str(exc))


# ---- 引导与总览 ---------------------------------------------------------
@router.post("/bootstrap", response_model=Ok)
def bootstrap_demo() -> Ok:
    info = bootstrap()
    return Ok(ok=True, message="恢复子系统示例数据已就绪", data=info or {})


@router.get("/overview")
def overview() -> dict[str, Any]:
    checkpoints = registry.list_checkpoints()
    batches = conversion.list_batches()
    queue = coordinator.list_queue()
    return {
        "checkpoint_total": len(checkpoints),
        "recoverable": sum(1 for row in checkpoints if row["recoverable"] is True),
        "unrecoverable": sum(1 for row in checkpoints if row["recoverable"] is False),
        "batch_total": len(batches),
        "queue_pending": sum(1 for row in queue if row["status"] in ("排队中", "处理中")),
    }


# ---- 登记与可恢复性 ------------------------------------------------------
@router.get("/checkpoints", response_model=Ok)
def list_checkpoints(pipeline_id: str | None = None) -> Ok:
    return Ok(ok=True, message="恢复检查点清单", data=registry.list_checkpoints(pipeline_id))


@router.post("/checkpoints", response_model=Ok)
def register_checkpoint(payload: dict[str, Any]) -> Ok:
    return _run("恢复检查点已登记", lambda: registry.register_checkpoint(payload))


@router.get("/checkpoints/{checkpoint_id}", response_model=Ok)
def get_checkpoint(checkpoint_id: int) -> Ok:
    return _run("检查点明细", lambda: registry.get_checkpoint(checkpoint_id))


@router.post("/checkpoints/{checkpoint_id}/assess", response_model=Ok)
def assess_checkpoint(checkpoint_id: int) -> Ok:
    return _run("可恢复性判定完成", lambda: registry.assess_checkpoint(checkpoint_id))


@router.post("/assess-all", response_model=Ok)
def assess_all() -> Ok:
    return Ok(ok=True, message="全量可恢复性判定完成", data=registry.assess_all())


@router.get("/evolutions", response_model=Ok)
def list_evolutions(pipeline_id: str | None = None) -> Ok:
    return Ok(ok=True, message="结构演进记录", data=registry.list_evolutions(pipeline_id))


@router.post("/evolutions", response_model=Ok)
def register_evolution(payload: dict[str, Any]) -> Ok:
    return _run("结构演进已登记", lambda: registry.register_evolution(payload))


@router.get("/operators", response_model=Ok)
def list_operators(pipeline_id: str | None = None) -> Ok:
    return Ok(ok=True, message="算子版本与依赖", data=registry.list_operators(pipeline_id))


# ---- 状态格式转换批次 ----------------------------------------------------
@router.post("/batches", response_model=Ok)
def create_batch(payload: dict[str, Any]) -> Ok:
    return _run(
        "转换批次已创建",
        lambda: conversion.create_batch(
            pipeline_id=payload["pipeline_id"],
            evolution_id=int(payload["evolution_id"]),
            checkpoint_ids=[int(value) for value in payload["checkpoint_ids"]]),
    )


@router.get("/batches", response_model=Ok)
def list_batches(pipeline_id: str | None = None) -> Ok:
    return Ok(ok=True, message="转换批次列表", data=conversion.list_batches(pipeline_id))


@router.get("/batches/{batch_id}", response_model=Ok)
def get_batch(batch_id: int) -> Ok:
    return _run("批次明细", lambda: conversion.get_batch(batch_id))


@router.post("/batches/{batch_id}/validate", response_model=Ok)
def validate_batch(batch_id: int) -> Ok:
    return _run("流水线依赖与字段映射校验完成", lambda: conversion.validate_dependencies(batch_id))


@router.post("/batches/{batch_id}/convert", response_model=Ok)
def convert_batch(batch_id: int, crash_after_items: int | None = None) -> Ok:
    return _run("批次已整体转换并提交",
                lambda: conversion.run_conversion(batch_id, crash_after_items))


@router.post("/batches/{batch_id}/rollback", response_model=Ok)
def rollback_batch(batch_id: int, payload: dict[str, Any] | None = None) -> Ok:
    reason = (payload or {}).get("reason", "人工触发整体回滚")
    return _run("批次已整体回滚至源格式完整版本",
                lambda: conversion.rollback(batch_id, reason=reason))


@router.post("/batches/recover-inflight", response_model=Ok)
def recover_inflight() -> Ok:
    return Ok(ok=True, message="未提交批次已在重启时整体回滚", data=conversion.recover_inflight())


@router.post("/evolutions/{evolution_id}/consume/{batch_id}", response_model=Ok)
def consume_batch(evolution_id: int, batch_id: int) -> Ok:
    return _run("结构演进已消费批次，实际生效范围校验通过",
                lambda: conversion.consume_batch_for_evolution(evolution_id, batch_id))


# ---- 恢复队列、切换冲突与装载 --------------------------------------------
@router.post("/queue/{checkpoint_id}", response_model=Ok)
def enqueue(checkpoint_id: int) -> Ok:
    return _run("旧恢复已排队并钉住当前版本", lambda: coordinator.enqueue(checkpoint_id))


@router.get("/queue", response_model=Ok)
def list_queue(pipeline_id: str | None = None) -> Ok:
    return Ok(ok=True, message="恢复队列", data=coordinator.list_queue(pipeline_id))


@router.post("/recovery-batches", response_model=Ok)
def process_recovery_batch(payload: dict[str, Any]) -> Ok:
    ids = [int(value) for value in payload.get("queue_entry_ids", [])]
    return _run("恢复批已按一套兼容状态整批装载",
                lambda: coordinator.process_recovery_batch(ids))


@router.post("/cutover", response_model=Ok)
def begin_cutover(payload: dict[str, Any]) -> Ok:
    return _run("新结构开始接收数据",
                lambda: coordinator.begin_cutover(payload["pipeline_id"], payload["new_version"]))


@router.post("/switch/validate", response_model=Ok)
def validate_switch(payload: dict[str, Any]) -> Ok:
    return _run("有序算子切换验证完成", lambda: coordinator.validate_ordered_switch(
        payload["upstream_id"], payload["downstream_id"], payload["new_version"]))


@router.post("/switch/execute", response_model=Ok)
def execute_switch(payload: dict[str, Any]) -> Ok:
    return _run("算子版本已切换",
                lambda: coordinator.switch_operator_version(payload["operator_id"], payload["new_version"]))


# ---- 活动位点核对 --------------------------------------------------------
@router.post("/active-positions", response_model=Ok)
def record_active(payload: dict[str, Any]) -> Ok:
    return _run("活动位点已登记", lambda: coordinator.record_active_positions(
        payload["pipeline_id"], dict(payload["positions"]), payload["position_time"]))


@router.post("/reconcile", response_model=Ok)
def reconcile(payload: dict[str, Any]) -> Ok:
    return _run("活动位点与导出输入范围核对完成",
                lambda: coordinator.reconcile_positions(
                    payload["pipeline_id"], list(payload["exported_ranges"])))
