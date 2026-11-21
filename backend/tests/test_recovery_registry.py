"""恢复子系统测试（一）：登记、可恢复性判定、字段映射与依赖图。

对应需求：
- 旧检查点位点与结构演进各自登记，并补齐可恢复性判定数据；
- 转换兼容存量位点和已有字段映射；
- 流水线按新旧依赖可用性决定是否装入对应位点。
"""
import pytest

from app.recovery.errors import RecoveryError
from app.recovery.field_mapping import apply_mapping, validate_mapping
from app.recovery.pipeline import decide_load, load_operator_graph
from app.recovery.service import bootstrap, conversion, coordinator, registry


class TestRegistryAndRecoverability:
    def test_seed_checkpoints_registered_independently(self):
        checkpoints = registry.list_checkpoints()
        evolutions = registry.list_evolutions()
        assert len(checkpoints) == 3
        assert len(evolutions) == 1
        # 两张登记表各自独立，检查点本身不携带演进结论。
        assert all(row["recoverable"] is None for row in checkpoints)

    def test_assess_marks_recoverable_via_evolution_chain(self):
        first = registry.list_checkpoints()[0]
        result = registry.assess_checkpoint(first["id"])
        assert result["recoverable"] is True
        assert result["status"] == "可恢复"
        # v1 -> v2 的格式路径被显式记录，回答“哪些旧位点还能恢复”。
        assert result["recovery_path"] == ["v1", "v2"]

    def test_assess_marks_orphan_version_unrecoverable(self):
        orphan = next(c for c in registry.list_checkpoints() if c["state_version"] == "v0")
        result = registry.assess_checkpoint(orphan["id"])
        assert result["recoverable"] is False
        assert result["status"] == "不可恢复"
        assert "没有任何版本" in result["blocked_reason"]

    def test_assess_all_covers_every_checkpoint(self):
        results = registry.assess_all()
        flags = sorted(row["recoverable"] for row in results)
        assert flags == [False, True, True]

    def test_duplicate_code_rejected(self):
        with pytest.raises(RecoveryError):
            registry.register_checkpoint({
                "checkpoint_code": "RECO-0001", "pipeline_id": "PIPE-RECO-01",
                "state_version": "v1"})


class TestFieldMapping:
    def test_rename_keep_default_drop(self):
        payload = {"order_id": "ORD-9", "amount": 10.0, "legacy_tag": "z"}
        mapping = [
            {"op": "rename", "source": "order_id", "target": "order_no"},
            {"op": "keep", "source": "amount", "target": "amount"},
            {"op": "default", "source": "channel", "target": "channel", "value": "unknown"},
            {"op": "drop", "source": "legacy_tag"},
        ]
        out = apply_mapping(payload, mapping)
        assert out == {"order_no": "ORD-9", "amount": 10.0, "channel": "unknown"}
        assert "legacy_tag" not in out

    def test_default_keeps_existing_value(self):
        mapping = [{"op": "default", "source": "channel", "target": "channel", "value": "unknown"}]
        assert apply_mapping({"channel": "app"}, mapping) == {"channel": "app"}

    def test_unknown_op_rejected(self):
        with pytest.raises(RecoveryError):
            validate_mapping([{"op": "explode", "source": "a", "target": "b"}])

    def test_duplicate_target_rejected(self):
        with pytest.raises(RecoveryError):
            validate_mapping([
                {"op": "keep", "source": "a", "target": "x"},
                {"op": "keep", "source": "b", "target": "x"},
            ])

    def test_missing_source_field_rejected(self):
        with pytest.raises(RecoveryError):
            apply_mapping({"amount": 1}, [{"op": "rename", "source": "order_id", "target": "order_no"}])


class TestPipelineDependencies:
    def test_topological_order_upstream_first(self):
        graph = load_operator_graph("PIPE-RECO-01")
        order = graph.topological_order()
        assert order.index("OP-A") < order.index("OP-B")

    def test_cycle_is_rejected(self):
        from app.recovery.pipeline import PipelineGraph
        graph = PipelineGraph([
            {"operator_id": "X", "depends_on": ["Y"], "versions": {"v1": "在线"}},
            {"operator_id": "Y", "depends_on": ["X"], "versions": {"v1": "在线"}},
        ])
        with pytest.raises(RecoveryError):
            graph.ensure_dag()

    def test_decide_load_falls_back_when_target_offline(self):
        # 初始状态 v2 下线、v1 在线：应回退源格式而不是拒绝。
        decision = decide_load("OP-B", preferred_version="v2", fallback_version="v1")
        assert decision["load"] is True
        assert decision["format"] == "源格式"
        assert decision["version"] == "v1"

    def test_decide_load_uses_target_when_online(self):
        coordinator.switch_operator_version("OP-A", "v2")
        coordinator.switch_operator_version("OP-B", "v2")
        decision = decide_load("OP-B", preferred_version="v2", fallback_version="v1")
        assert decision["load"] is True
        assert decision["format"] == "目标格式"

    def test_decide_load_rejects_when_both_unavailable(self):
        # 两个版本在算子登记里都不存在/不在线：拒绝装载而不是交付半截依赖。
        decision = decide_load("OP-B", preferred_version="v9", fallback_version="v8")
        assert decision["load"] is False
        assert decision["version"] is None
