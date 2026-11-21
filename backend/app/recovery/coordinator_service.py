"""恢复协调服务：排队、版本切换冲突、装载、切换验证与位点核对。

把转换批次（数据面）与流水线运行面（控制面）衔接起来：

* 旧恢复先排队并钉住版本；新结构开始接收数据（cutover）前必须排空队列，
  否则按冲突拒绝，避免版本切换与恢复时效互相踩踏；
* 每个恢复批只能装载内部兼容的一套状态（同一钉住版本），不允许新旧混装；
* 已完成的旧恢复保留实际装载格式与格式历史，随时可追查；
* 对有依赖次序的两个算子做切换验证：上游不先就绪，下游切换一律拒绝；
* 上线后用活动位点与数据库导出的实际输入范围做覆盖核对。
"""
from __future__ import annotations

from typing import Any

from app.recovery import repository as repo
from app.recovery.errors import RecoveryError
from app.recovery.pipeline import load_operator_graph


class RecoveryCoordinator:
    # ---- 恢复队列：旧恢复排队并钉住版本 --------------------------------
    def enqueue(self, checkpoint_id: int) -> dict[str, Any]:
        checkpoint = repo.find("_recovery_checkpoints", checkpoint_id)
        if checkpoint is None:
            raise RecoveryError(f"恢复检查点 {checkpoint_id} 不存在")
        if checkpoint["recoverable"] is not True:
            raise RecoveryError(
                f"检查点 {checkpoint_id} 不可恢复：{checkpoint.get('blocked_reason') or '未通过可恢复性判定'}")
        existing = repo.find_by("_recovery_queue", "checkpoint_id", checkpoint_id)
        if existing and existing["status"] in ("排队中", "处理中"):
            raise RecoveryError("该检查点已在恢复队列中，禁止重复排队")
        entry = repo.insert("_recovery_queue", {
            "checkpoint_id": checkpoint_id,
            "pipeline_id": checkpoint["pipeline_id"],
            "pinned_version": checkpoint["state_version"],
            "status": "排队中",
            "load_format": None,
        })
        checkpoint["status"] = "排队中"
        return dict(entry)

    def list_queue(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = repo.table("_recovery_queue")
        if pipeline_id:
            rows = [row for row in rows if row["pipeline_id"] == pipeline_id]
        return [dict(row) for row in rows]

    # ---- 版本切换与恢复时效冲突 ----------------------------------------
    def begin_cutover(self, pipeline_id: str, new_version: str) -> dict[str, Any]:
        """新结构开始接收数据前的冲突闸门：队列里还有旧恢复则拒绝切换。"""
        pending = [
            row for row in repo.table("_recovery_queue")
            if row["pipeline_id"] == pipeline_id and row["status"] in ("排队中", "处理中")
        ]
        if pending:
            codes = [
                repo.find("_recovery_checkpoints", row["checkpoint_id"])["checkpoint_code"]
                for row in pending
            ]
            raise RecoveryError(
                "版本切换与恢复时效冲突：仍有旧恢复排队/处理中 "
                f"（{'、'.join(codes)}），请先排空队列再切换到 {new_version}")
        active = repo.find_by("_recovery_active", "pipeline_id", pipeline_id)
        if active is None:
            active = repo.insert("_recovery_active", {
                "pipeline_id": pipeline_id, "version": new_version,
                "positions": {}, "position_time": None})
        active["version"] = new_version
        repo.insert("_recovery_audits", {
            "batch_id": None, "action": "结构切换",
            "detail": f"流水线 {pipeline_id} 开始接收 {new_version} 数据"})
        return dict(active)

    # ---- 恢复批装载：只允许一套内部兼容状态 -----------------------------
    def process_recovery_batch(self, queue_entry_ids: list[int]) -> dict[str, Any]:
        if not queue_entry_ids:
            raise RecoveryError("恢复批不能为空")
        entries = [repo.find("_recovery_queue", qid) for qid in queue_entry_ids]
        if any(entry is None for entry in entries):
            raise RecoveryError("恢复批包含不存在的队列条目")
        pinned_versions = {entry["pinned_version"] for entry in entries}
        if len(pinned_versions) != 1:
            raise RecoveryError(
                f"一个恢复批只能装载一套内部兼容状态，发现多个钉住版本：{sorted(pinned_versions)}")
        pinned_version = pinned_versions.pop()
        pipeline_ids = {entry["pipeline_id"] for entry in entries}
        if len(pipeline_ids) != 1:
            raise RecoveryError("恢复批不能跨流水线混装")
        pipeline_id = pipeline_ids.pop()

        graph = load_operator_graph(pipeline_id)
        # 整批同一套版本：任一算子在该版本依赖链不在线，整批拒绝，不做部分装载。
        unavailable: list[str] = []
        for operator_id in graph.topological_order():
            check = graph.dependency_chain_ready(operator_id, pinned_version)
            if not check["ready"]:
                unavailable.append(operator_id)
        if unavailable:
            raise RecoveryError(
                f"钉住版本 {pinned_version} 下算子依赖未全部在线，整批拒绝装载："
                f"{'、'.join(unavailable)}")

        loaded: list[dict[str, Any]] = []
        for entry in entries:
            entry["status"] = "处理中"
            checkpoint = repo.find("_recovery_checkpoints", entry["checkpoint_id"])
            state = repo.find_by("_recovery_states", "checkpoint_id", entry["checkpoint_id"])
            if state["state_version"] != pinned_version:
                raise RecoveryError(
                    f"检查点 {checkpoint['checkpoint_code']} 实际版本与批次钉住版本不一致，"
                    "禁止新旧混装")
            checkpoint["status"] = "恢复中"
            # 实际装载格式取该状态最后一次格式历史，旧恢复也能按此追查。
            load_format = state["format_history"][-1]["format"]
            checkpoint["last_load_format"] = load_format
            checkpoint["status"] = "已恢复"
            entry["status"] = "已完成"
            entry["load_format"] = load_format
            loaded.append({
                "checkpoint_id": checkpoint["id"],
                "checkpoint_code": checkpoint["checkpoint_code"],
                "loaded_version": pinned_version,
                "load_format": load_format,
                "positions": state["positions"],
            })
        repo.insert("_recovery_audits", {
            "batch_id": None, "action": "恢复批装载",
            "detail": f"流水线 {pipeline_id} 按 {pinned_version} 整批装载 {len(loaded)} 个检查点"})
        return {"pipeline_id": pipeline_id, "pinned_version": pinned_version, "loaded": loaded}

    # ---- 有依赖次序的两个算子的切换验证 --------------------------------
    def validate_ordered_switch(self, upstream_id: str, downstream_id: str,
                                new_version: str) -> dict[str, Any]:
        up_op = repo.find_by("_recovery_operators", "operator_id", upstream_id)
        down_op = repo.find_by("_recovery_operators", "operator_id", downstream_id)
        if up_op is None or down_op is None:
            raise RecoveryError("参与切换验证的算子未登记完整")
        if up_op["pipeline_id"] != down_op["pipeline_id"]:
            raise RecoveryError("两个算子不在同一条流水线，无法验证依赖次序")
        graph = load_operator_graph(up_op["pipeline_id"])
        order = graph.topological_order()
        if order.index(upstream_id) >= order.index(downstream_id):
            raise RecoveryError(
                f"依赖次序错误：{upstream_id} 必须先于 {downstream_id} 切换")
        if upstream_id not in graph.edges.get(downstream_id, []):
            raise RecoveryError(f"{downstream_id} 并不直接依赖 {upstream_id}，无法做成对切换验证")

        blockers: list[str] = []
        for operator_id in (upstream_id, downstream_id):
            check = graph.dependency_chain_ready(operator_id, new_version)
            if not check["ready"]:
                blockers.append(
                    f"{operator_id} 缺少在线依赖：{'、'.join(check['missing'])}")
        decision = "拒绝切换" if blockers else "允许切换"
        result = {
            "upstream": upstream_id,
            "downstream": downstream_id,
            "new_version": new_version,
            "switch_order": [upstream_id, downstream_id],
            "decision": decision,
            "blockers": blockers,
        }
        repo.insert("_recovery_audits", {
            "batch_id": None, "action": "算子切换验证",
            "detail": f"{upstream_id}->{downstream_id} @ {new_version}：{decision}"})
        return result

    def switch_operator_version(self, operator_id: str, new_version: str) -> dict[str, Any]:
        """按验证结论执行算子版本切换：旧版本下线、新版本上线。"""
        op = repo.find_by("_recovery_operators", "operator_id", operator_id)
        if op is None:
            raise RecoveryError(f"算子「{operator_id}」未登记")
        if new_version not in op["versions"]:
            raise RecoveryError(f"算子「{operator_id}」没有版本 {new_version} 的部署登记")
        for version in op["versions"]:
            op["versions"][version] = "在线" if version == new_version else "已下线"
        repo.insert("_recovery_audits", {
            "batch_id": None, "action": "算子版本切换",
            "detail": f"{operator_id} 切换至 {new_version}"})
        return dict(op)

    # ---- 活动位点登记与输入范围核对 -------------------------------------
    def record_active_positions(self, pipeline_id: str, positions: dict[str, int],
                                position_time: str) -> dict[str, Any]:
        active = repo.find_by("_recovery_active", "pipeline_id", pipeline_id)
        if active is None:
            active = repo.insert("_recovery_active", {
                "pipeline_id": pipeline_id, "version": None,
                "positions": {}, "position_time": None})
        active["positions"] = dict(positions)
        active["position_time"] = position_time
        return dict(active)

    def reconcile_positions(self, pipeline_id: str,
                            exported_ranges: list[dict[str, Any]]) -> dict[str, Any]:
        """核对上线后活动位点与数据库导出的实际输入范围。

        每个分区的活动位点必须到达导出范围上界（覆盖全部已导出输入），
        且不能倒退到下界之前；否则给出缺口或越界明细。
        """
        active = repo.find_by("_recovery_active", "pipeline_id", pipeline_id)
        if active is None:
            raise RecoveryError(f"流水线 {pipeline_id} 尚无活动位点，无法核对")
        active_positions: dict[str, int] = active["positions"]
        partitions: list[dict[str, Any]] = []
        all_covered = True
        for exported in exported_ranges:
            partition = exported["partition"]
            start = int(exported["start"])
            end = int(exported["end"])
            current = active_positions.get(partition)
            if current is None:
                status, gap = "缺失位点", end - start
            elif int(current) < start:
                status, gap = "位点倒退", end - int(current)
            elif int(current) < end:
                status, gap = "存在缺口", end - int(current)
            else:
                status, gap = "已覆盖", 0
            if status != "已覆盖":
                all_covered = False
            partitions.append({
                "partition": partition,
                "exported_range": [start, end],
                "active_position": current,
                "status": status,
                "gap": max(gap, 0),
            })
        result = {
            "pipeline_id": pipeline_id,
            "active_version": active["version"],
            "position_time": active["position_time"],
            "all_covered": all_covered,
            "partitions": partitions,
        }
        repo.insert("_recovery_audits", {
            "batch_id": None, "action": "位点核对",
            "detail": f"结论：{'全部覆盖' if all_covered else '存在缺口/异常'}"})
        return result
