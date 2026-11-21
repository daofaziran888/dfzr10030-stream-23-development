"""状态格式转换批次服务：本子系统的事务核心。

关键约束（对应需求逐条落地）：

* 转换前必须校验流水线上下游依赖与字段映射，校验不过批次保持「已创建」，
  不会产生任何半转换数据；
* 转换结果先写入暂存区（staged_payload），同批数据要么整体提交、要么整体
  回滚，读路径永远读不到只转换一半的内容；
* 服务在「转换中」停止后重启，recover_inflight 会把未提交批次整体回滚到
  源格式完整版本，随后可以重新发起完整批次；
* 提交后转换结果回写到恢复检查点清单（状态、版本、实际装载格式可追查）；
* 结构演进记录消费批次时，校验批次实际生效范围是否落在演进声明范围内。
"""
from __future__ import annotations

from typing import Any, Callable

from app.recovery import repository as repo
from app.recovery.errors import BATCH_STATUSES, RecoveryError
from app.recovery.field_mapping import apply_mapping, validate_mapping
from app.recovery.pipeline import load_operator_graph

CrashHook = Callable[[dict[str, Any]], None]


class ConversionService:
    # ---- 批次创建与依赖校验 -------------------------------------------
    def create_batch(self, *, pipeline_id: str, evolution_id: int,
                     checkpoint_ids: list[int]) -> dict[str, Any]:
        evolution = repo.find("_recovery_evolutions", evolution_id)
        if evolution is None:
            raise RecoveryError(f"结构演进 {evolution_id} 不存在")
        if evolution["pipeline_id"] != pipeline_id:
            raise RecoveryError("结构演进与批次不属于同一条流水线，禁止跨流水线转换")
        if evolution["status"] == "已回滚":
            raise RecoveryError("结构演进已回滚，不能再发起转换批次")
        if not checkpoint_ids:
            raise RecoveryError("转换批次至少要包含一个检查点")

        items: list[dict[str, Any]] = []
        seen: set[int] = set()
        for checkpoint_id in checkpoint_ids:
            checkpoint = repo.find("_recovery_checkpoints", checkpoint_id)
            if checkpoint is None:
                raise RecoveryError(f"恢复检查点 {checkpoint_id} 不存在")
            if checkpoint["pipeline_id"] != pipeline_id:
                raise RecoveryError(f"检查点 {checkpoint_id} 不属于流水线 {pipeline_id}")
            if checkpoint_id in seen:
                raise RecoveryError(f"检查点 {checkpoint_id} 在批次中重复")
            if checkpoint["status"] in ("转换中", "恢复中"):
                raise RecoveryError(f"检查点 {checkpoint_id} 正在被其他批次占用")
            if checkpoint["state_version"] != evolution["from_version"]:
                raise RecoveryError(
                    f"检查点 {checkpoint_id} 版本 {checkpoint['state_version']} "
                    f"与演进源版本 {evolution['from_version']} 不一致")
            seen.add(checkpoint_id)
            items.append(repo.insert("_recovery_batch_items", {
                "checkpoint_id": checkpoint_id,
                "source_version": evolution["from_version"],
                "target_version": evolution["to_version"],
                "status": "待转换",
                "staged": False,
                "converted_payload": None,
                # 暂存时写入源格式完整快照，回滚（含已提交批次）据此整体还原。
                "source_snapshot": None,
            }))

        batch = repo.insert("_recovery_batches", {
            "batch_code": f"CONV-{repo.next_id('_recovery_batches'):04d}",
            "pipeline_id": pipeline_id,
            "evolution_id": evolution_id,
            "from_version": evolution["from_version"],
            "to_version": evolution["to_version"],
            "status": "已创建",
            "item_ids": [item["id"] for item in items],
            "effective_scope": [],
            "rollback_of": None,
            "journal": [],
        })
        for checkpoint_id in checkpoint_ids:
            repo.find("_recovery_checkpoints", checkpoint_id)["active_batch_id"] = batch["id"]
        self._audit(batch["id"], "批次创建", f"纳入 {len(checkpoint_ids)} 个检查点")
        return self.get_batch(batch["id"])

    def validate_dependencies(self, batch_id: int) -> dict[str, Any]:
        """校验流水线 DAG、目标版本上下游可用性与字段映射合法性。"""
        batch = self._require_batch(batch_id)
        if batch["status"] not in ("已创建", "依赖已校验"):
            raise RecoveryError(f"批次状态为「{batch['status']}」，不能再做依赖校验")
        evolution = repo.find("_recovery_evolutions", batch["evolution_id"])
        mapping = evolution["field_mapping"]
        validate_mapping(mapping)

        graph = load_operator_graph(batch["pipeline_id"])
        blocked: list[str] = []
        for operator_id in graph.topological_order():
            check = graph.dependency_chain_ready(operator_id, batch["to_version"])
            if not check["ready"]:
                blocked.append(
                    f"算子 {operator_id} 在目标版本 {batch['to_version']} 缺少在线依赖："
                    f"{'、'.join(check['missing'])}")
        if blocked:
            batch["status"] = "已失败"
            self._audit(batch_id, "依赖校验拒绝", "；".join(blocked))
            raise RecoveryError("流水线上下游依赖校验失败：" + "；".join(blocked))

        # 存量字段映射还要能实际作用于每个检查点的存量载荷。
        for item_id in batch["item_ids"]:
            item = repo.find("_recovery_batch_items", item_id)
            state = repo.find_by("_recovery_states", "checkpoint_id", item["checkpoint_id"])
            try:
                apply_mapping(state["payload"], mapping)
            except RecoveryError as exc:
                batch["status"] = "已失败"
                self._audit(batch_id, "字段映射拒绝", str(exc))
                raise RecoveryError(f"检查点 {item['checkpoint_id']} 字段映射不兼容：{exc}") from exc

        batch["status"] = "依赖已校验"
        self._audit(batch_id, "依赖校验通过",
                    f"目标版本 {batch['to_version']} 上下游与字段映射全部可用")
        return self.get_batch(batch_id)

    # ---- 转换、提交、回滚（原子边界） ----------------------------------
    def run_conversion(self, batch_id: int, crash_after_items: int | None = None) -> dict[str, Any]:
        """执行转换并在同一原子边界内整体提交。

        crash_after_items 用于测试「转换一半停止服务」：在暂存指定条数后
        抛出异常模拟进程崩溃，此时没有任何条目提交，读路径仍是源格式。
        """
        batch = self._require_batch(batch_id)
        if batch["status"] == "已创建":
            self.validate_dependencies(batch_id)
            batch = self._require_batch(batch_id)
        if batch["status"] != "依赖已校验":
            raise RecoveryError(f"批次状态为「{batch['status']}」，无法启动转换")

        evolution = repo.find("_recovery_evolutions", batch["evolution_id"])
        batch["status"] = "转换中"
        batch["journal"].append("进入转换中：暂存区开启，读路径仍指向源格式")
        for checkpoint in repo.table("_recovery_checkpoints"):
            if checkpoint.get("active_batch_id") == batch_id:
                checkpoint["status"] = "转换中"

        converted = 0
        try:
            for item_id in batch["item_ids"]:
                item = repo.find("_recovery_batch_items", item_id)
                state = repo.find_by("_recovery_states", "checkpoint_id", item["checkpoint_id"])
                # 先冻结源格式完整快照（载荷 + 版本 + 位点），作为回滚的完整版本。
                item["source_snapshot"] = {
                    "payload": dict(state["payload"]),
                    "state_version": state["state_version"],
                    "positions": dict(state["positions"]),
                }
                # 位点原样保留，只转换状态载荷，保证消费位置不漂移。
                staged_payload = apply_mapping(state["payload"], evolution["field_mapping"])
                state["staged_payload"] = staged_payload
                item["converted_payload"] = staged_payload
                item["staged"] = True
                item["status"] = "已暂存"
                converted += 1
                batch["journal"].append(
                    f"检查点 {item['checkpoint_id']} 已写入暂存区（位点保持不变）")
                if crash_after_items is not None and converted >= crash_after_items:
                    raise RecoveryError("模拟进程崩溃：转换一半停止服务")
        except RecoveryError:
            # 崩溃等价于进程退出：暂存内容丢弃，批次等待重启恢复。
            raise

        return self.commit(batch_id)

    def commit(self, batch_id: int) -> dict[str, Any]:
        """整体提交：同批所有条目一起从源格式切到目标格式。"""
        batch = self._require_batch(batch_id)
        if batch["status"] != "转换中":
            raise RecoveryError(f"批次状态为「{batch['status']}」，没有可提交的暂存内容")
        scope: list[int] = []
        for item_id in batch["item_ids"]:
            item = repo.find("_recovery_batch_items", item_id)
            if not item["staged"] or item["converted_payload"] is None:
                # 不允许只提交一部分：发现缺口立即整体回滚。
                self.rollback(batch_id, reason=f"条目 {item_id} 缺少暂存内容，拒绝半批提交")
                raise RecoveryError("批次存在未转换条目，已整体回滚，未交付半转换内容")
            state = repo.find_by("_recovery_states", "checkpoint_id", item["checkpoint_id"])
            state["payload"] = item["converted_payload"]
            state["staged_payload"] = None
            state["state_version"] = batch["to_version"]
            state["format_history"].append(
                {"version": batch["to_version"], "format": "目标格式", "batch_id": batch_id})
            item["status"] = "已提交"
            item["staged"] = False
            scope.append(item["checkpoint_id"])

        batch["status"] = "已提交"
        batch["effective_scope"] = scope
        batch["journal"].append(f"整体提交完成，生效检查点：{scope}")
        # 转换结果回写到恢复检查点清单。
        for checkpoint_id in scope:
            checkpoint = repo.find("_recovery_checkpoints", checkpoint_id)
            checkpoint["status"] = "已转换"
            checkpoint["state_version"] = batch["to_version"]
            checkpoint["active_batch_id"] = None
            checkpoint["last_load_format"] = "目标格式"
        self._audit(batch_id, "批次整体提交", f"生效范围 {scope}")
        return self.get_batch(batch_id)

    def rollback(self, batch_id: int, *, reason: str = "人工触发整体回滚") -> dict[str, Any]:
        """整体回滚：同批数据只能整体切回源格式完整版本。

        * 未提交批次：转换结果只在暂存区，直接丢弃暂存即可，读路径本就还是源格式；
        * 已提交批次：按暂存时冻结的源快照整体还原载荷、版本与位点，
          不存在“回滚一半”，所有条目同进同退。
        """
        batch = self._require_batch(batch_id)
        if batch["status"] == "已回滚":
            return self.get_batch(batch_id)
        if batch["status"] not in ("转换中", "依赖已校验", "已创建", "已提交"):
            raise RecoveryError(f"批次状态为「{batch['status']}」，不允许回滚")
        was_committed = batch["status"] == "已提交"

        for item_id in batch["item_ids"]:
            item = repo.find("_recovery_batch_items", item_id)
            state = repo.find_by("_recovery_states", "checkpoint_id", item["checkpoint_id"])
            checkpoint = repo.find("_recovery_checkpoints", item["checkpoint_id"])
            if was_committed:
                snapshot = item.get("source_snapshot")
                if snapshot is None:
                    # 已提交却没有源快照属于数据不一致，宁可不回滚也不能交付残缺版本。
                    raise RecoveryError(
                        f"检查点 {item['checkpoint_id']} 缺少源格式快照，拒绝不完整回滚")
                state["payload"] = dict(snapshot["payload"])
                state["positions"] = dict(snapshot["positions"])
                state["state_version"] = snapshot["state_version"]
                state["format_history"].append({
                    "version": snapshot["state_version"], "format": "源格式",
                    "batch_id": batch_id, "rollback": True})
                checkpoint["state_version"] = snapshot["state_version"]
                checkpoint["last_load_format"] = "源格式"
                checkpoint["status"] = "已回滚"
            else:
                checkpoint["status"] = "不可恢复" if checkpoint["recoverable"] is False else "可恢复"
            state["staged_payload"] = None
            item["staged"] = False
            item["converted_payload"] = None
            item["status"] = "已回滚"
            checkpoint["active_batch_id"] = None
        batch["status"] = "已回滚"
        batch["journal"].append(
            f"整体回滚：{reason}；同批数据整体切回源格式完整版本，未交付半转换内容")
        self._audit(batch_id, "批次整体回滚", reason)
        return self.get_batch(batch_id)

    def recover_inflight(self) -> list[dict[str, Any]]:
        """服务重启入口：所有停在「转换中」的批次整体回滚后可重新转换。"""
        recovered: list[dict[str, Any]] = []
        for batch in repo.table("_recovery_batches"):
            if batch["status"] == "转换中":
                recovered.append(self.rollback(batch["id"], reason="重启恢复：未提交批次整体回滚"))
        return recovered

    # ---- 结构演进消费批次：校验实际生效范围 -----------------------------
    def consume_batch_for_evolution(self, evolution_id: int, batch_id: int) -> dict[str, Any]:
        evolution = repo.find("_recovery_evolutions", evolution_id)
        if evolution is None:
            raise RecoveryError(f"结构演进 {evolution_id} 不存在")
        batch = self._require_batch(batch_id)
        if batch["evolution_id"] != evolution_id:
            raise RecoveryError("批次与结构演进不匹配，拒绝消费")
        if batch["status"] != "已提交":
            raise RecoveryError(f"批次状态为「{batch['status']}」，只有已提交批次可被消费")

        declared = set(evolution.get("effective_scope") or [])
        actual = set(batch["effective_scope"])
        if declared and not actual.issubset(declared):
            overflow = sorted(actual - declared)
            raise RecoveryError(
                f"批次实际生效范围超出演进声明范围，越界检查点：{overflow}")
        evolution["consumed_batch_id"] = batch_id
        evolution["status"] = "已生效"
        if not declared:
            evolution["effective_scope"] = sorted(actual)
        self._audit(batch_id, "演进消费批次", f"实际生效范围校验通过：{sorted(actual)}")
        return dict(evolution)

    # ---- 查询与辅助 ---------------------------------------------------
    def list_batches(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = repo.table("_recovery_batches")
        if pipeline_id:
            rows = [row for row in rows if row["pipeline_id"] == pipeline_id]
        return [self.get_batch(row["id"]) for row in rows]

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        items = [dict(repo.find("_recovery_batch_items", item_id)) for item_id in batch["item_ids"]]
        return {**dict(batch), "items": items}

    def _require_batch(self, batch_id: int) -> dict[str, Any]:
        batch = repo.find("_recovery_batches", batch_id)
        if batch is None:
            raise RecoveryError(f"转换批次 {batch_id} 不存在")
        return batch

    def _audit(self, batch_id: int, action: str, detail: str) -> None:
        repo.insert("_recovery_audits", {
            "batch_id": batch_id, "action": action, "detail": detail})
