"""登记与可恢复性判定服务。

旧检查点位点与结构演进记录在各自的登记表里维护，本服务补上两者之间
缺失的判定数据：根据演进链上各版本的可用性，判断每个旧位点是否还能恢复，
并给出可走的格式路径与不可恢复原因。
"""
from __future__ import annotations

from typing import Any

from app.recovery import repository as repo
from app.recovery.errors import CHECKPOINT_STATUSES, EVOLUTION_STATUSES, RecoveryError


class RegistryService:
    # ---- 结构演进记录 -------------------------------------------------
    def register_evolution(self, values: dict[str, Any]) -> dict[str, Any]:
        for required in ("evolution_code", "pipeline_id", "from_version", "to_version"):
            if not str(values.get(required) or "").strip():
                raise RecoveryError(f"结构演进缺少必填字段：{required}")
        if repo.find_by("_recovery_evolutions", "evolution_code", values["evolution_code"]):
            raise RecoveryError(f"结构演进编号「{values['evolution_code']}」已存在")
        row = repo.insert("_recovery_evolutions", {
            "evolution_code": values["evolution_code"],
            "pipeline_id": values["pipeline_id"],
            "from_version": values["from_version"],
            "to_version": values["to_version"],
            "change_type": values.get("change_type", "字段调整"),
            "field_mapping": list(values.get("field_mapping", [])),
            "status": "已登记",
            "effective_scope": values.get("effective_scope", []),
            "consumed_batch_id": None,
        })
        return row

    def list_evolutions(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = repo.table("_recovery_evolutions")
        if pipeline_id:
            rows = [row for row in rows if row["pipeline_id"] == pipeline_id]
        return [dict(row) for row in rows]

    # ---- 恢复检查点 ---------------------------------------------------
    def register_checkpoint(self, values: dict[str, Any]) -> dict[str, Any]:
        for required in ("checkpoint_code", "pipeline_id", "state_version"):
            if not str(values.get(required) or "").strip():
                raise RecoveryError(f"恢复检查点缺少必填字段：{required}")
        if repo.find_by("_recovery_checkpoints", "checkpoint_code", values["checkpoint_code"]):
            raise RecoveryError(f"检查点编号「{values['checkpoint_code']}」已存在")
        checkpoint = repo.insert("_recovery_checkpoints", {
            "checkpoint_code": values["checkpoint_code"],
            "pipeline_id": values["pipeline_id"],
            "state_version": values["state_version"],
            "status": "已登记",
            "position_time": values.get("position_time"),
            "recoverable": None,
            "recovery_path": [],
            "blocked_reason": None,
            "active_batch_id": None,
            "last_load_format": None,
        })
        # 每个检查点携带一份状态快照（位点 + 状态载荷），按登记格式保存。
        repo.insert("_recovery_states", {
            "checkpoint_id": checkpoint["id"],
            "state_version": values["state_version"],
            "positions": dict(values.get("positions", {})),
            "payload": dict(values.get("payload", {})),
            "staged_payload": None,
            "format_history": [{"version": values["state_version"], "format": "源格式"}],
        })
        return self.assess_checkpoint(checkpoint["id"])

    def list_checkpoints(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = repo.table("_recovery_checkpoints")
        if pipeline_id:
            rows = [row for row in rows if row["pipeline_id"] == pipeline_id]
        return [dict(row) for row in rows]

    def get_checkpoint(self, checkpoint_id: int) -> dict[str, Any]:
        row = repo.find("_recovery_checkpoints", checkpoint_id)
        if row is None:
            raise RecoveryError(f"恢复检查点 {checkpoint_id} 不存在")
        return dict(row)

    # ---- 可恢复性判定 -------------------------------------------------
    def _evolution_chain(self, pipeline_id: str, start_version: str) -> list[dict[str, Any]]:
        """从登记版本出发，沿 from->to 串起该流水线的演进链。"""
        chain: list[dict[str, Any]] = []
        current = start_version
        evolutions = [row for row in repo.table("_recovery_evolutions") if row["pipeline_id"] == pipeline_id]
        guard = 0
        while guard <= len(evolutions):
            guard += 1
            step = next((row for row in evolutions if row["from_version"] == current), None)
            if step is None:
                break
            chain.append(step)
            current = step["to_version"]
        return chain

    def assess_checkpoint(self, checkpoint_id: int) -> dict[str, Any]:
        """判定单个检查点是否可恢复：登记版本在线，或存在在线的演进目标版本。"""
        checkpoint = repo.find("_recovery_checkpoints", checkpoint_id)
        if checkpoint is None:
            raise RecoveryError(f"恢复检查点 {checkpoint_id} 不存在")
        pipeline_id = checkpoint["pipeline_id"]
        start_version = checkpoint["state_version"]

        chain = self._evolution_chain(pipeline_id, start_version)
        path = [start_version] + [step["to_version"] for step in chain]
        # 登记版本自身可直接装载也算可恢复（不经过任何转换）。
        online_versions = self._online_versions(pipeline_id)
        reachable = [version for version in path if version in online_versions]

        if reachable:
            checkpoint["recoverable"] = True
            checkpoint["recovery_path"] = path
            checkpoint["blocked_reason"] = None
            if checkpoint["status"] == "已登记":
                checkpoint["status"] = "可恢复"
        else:
            checkpoint["recoverable"] = False
            checkpoint["recovery_path"] = path
            checkpoint["blocked_reason"] = (
                f"演进路径 {' -> '.join(path)} 上没有任何版本的算子依赖在线")
            if checkpoint["status"] == "已登记":
                checkpoint["status"] = "不可恢复"
        return dict(checkpoint)

    def assess_all(self) -> list[dict[str, Any]]:
        return [self.assess_checkpoint(row["id"]) for row in repo.table("_recovery_checkpoints")]

    def _online_versions(self, pipeline_id: str) -> set[str]:
        online: set[str] = set()
        for op in repo.table("_recovery_operators"):
            if op["pipeline_id"] != pipeline_id:
                continue
            for version, availability in op.get("versions", {}).items():
                if availability == "在线":
                    online.add(version)
        return online

    # ---- 算子版本登记 -------------------------------------------------
    def register_operator(self, values: dict[str, Any]) -> dict[str, Any]:
        for required in ("operator_id", "pipeline_id", "versions"):
            if not str(values.get(required) or "").strip():
                raise RecoveryError(f"算子缺少必填字段：{required}")
        existing = repo.find_by("_recovery_operators", "operator_id", values["operator_id"])
        if existing:
            raise RecoveryError(f"算子「{values['operator_id']}」已登记")
        return repo.insert("_recovery_operators", {
            "operator_id": values["operator_id"],
            "pipeline_id": values["pipeline_id"],
            "depends_on": list(values.get("depends_on", [])),
            "versions": dict(values["versions"]),
        })

    def list_operators(self, pipeline_id: str | None = None) -> list[dict[str, Any]]:
        rows = repo.table("_recovery_operators")
        if pipeline_id:
            rows = [row for row in rows if row["pipeline_id"] == pipeline_id]
        return [dict(row) for row in rows]
