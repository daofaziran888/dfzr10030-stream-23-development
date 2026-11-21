"""恢复子系统测试（二）：状态格式转换批次的事务边界。

对应需求：
- 转换流程校验流水线上下游依赖，兼容存量位点与字段映射；
- 回滚只能整体切回完整版本，不交付半转换内容；
- 转换一半停止服务，重启后能读回完整恢复批次；
- 演进消费批次时校验实际生效范围，结果回写检查点清单。
"""
import pytest

from app.recovery import repository as repo
from app.recovery.errors import RecoveryError
from app.recovery.service import conversion, coordinator, registry


def _v1_checkpoint_ids():
    return [c["id"] for c in registry.list_checkpoints() if c["state_version"] == "v1"]


def _evolution_id():
    return registry.list_evolutions()[0]["id"]


def _state(checkpoint_id):
    return repo.find_by("_recovery_states", "checkpoint_id", checkpoint_id)


class TestBatchDependencyValidation:
    def test_create_batch_rejects_version_mismatch(self):
        orphan = next(c for c in registry.list_checkpoints() if c["state_version"] == "v0")
        with pytest.raises(RecoveryError):
            conversion.create_batch(
                pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
                checkpoint_ids=[orphan["id"]])

    def test_validate_blocked_when_target_dependencies_offline(self):
        # 种子里 v2 全部下线，依赖校验必须拒绝且批次标记失败。
        batch = conversion.create_batch(
            pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
            checkpoint_ids=_v1_checkpoint_ids())
        with pytest.raises(RecoveryError):
            conversion.validate_dependencies(batch["id"])
        assert conversion.get_batch(batch["id"])["status"] == "已失败"
        # 失败批次没有产生任何暂存内容。
        for item in conversion.get_batch(batch["id"])["items"]:
            assert item["staged"] is False

    def test_validate_passes_after_dependencies_online(self):
        for op_id in ("OP-A", "OP-B"):
            coordinator.switch_operator_version(op_id, "v2")
        batch = conversion.create_batch(
            pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
            checkpoint_ids=_v1_checkpoint_ids())
        result = conversion.validate_dependencies(batch["id"])
        assert result["status"] == "依赖已校验"


class TestAtomicConversionAndRollback:
    def _online_batch(self):
        for op_id in ("OP-A", "OP-B"):
            coordinator.switch_operator_version(op_id, "v2")
        return conversion.create_batch(
            pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
            checkpoint_ids=_v1_checkpoint_ids())

    def test_full_commit_converts_whole_batch_and_keeps_positions(self):
        checkpoint_ids = _v1_checkpoint_ids()
        batch = self._online_batch()
        result = conversion.run_conversion(batch["id"])
        assert result["status"] == "已提交"
        assert sorted(result["effective_scope"]) == sorted(checkpoint_ids)
        for checkpoint_id in checkpoint_ids:
            state = _state(checkpoint_id)
            # 位点原样保留，只有载荷按映射转换。
            assert state["positions"] in ({"p0": 100, "p1": 80}, {"p0": 120, "p1": 90})
            assert state["state_version"] == "v2"
            assert "order_no" in state["payload"]
            assert "order_id" not in state["payload"]
            assert "legacy_tag" not in state["payload"]
            assert state["staged_payload"] is None
            checkpoint = registry.get_checkpoint(checkpoint_id)
            assert checkpoint["status"] == "已转换"
            assert checkpoint["last_load_format"] == "目标格式"

    def test_half_converted_crash_leaves_no_partial_data(self):
        batch = self._online_batch()
        with pytest.raises(RecoveryError):
            conversion.run_conversion(batch["id"], crash_after_items=1)
        # 崩溃后读路径仍是完整源格式：暂存内容不对外可见。
        stuck = conversion.get_batch(batch["id"])
        assert stuck["status"] == "转换中"
        for checkpoint_id in _v1_checkpoint_ids():
            state = _state(checkpoint_id)
            assert state["state_version"] == "v1"
            assert "order_id" in state["payload"]

    def test_restart_recovers_inflight_to_full_source_batch(self):
        batch = self._online_batch()
        with pytest.raises(RecoveryError):
            conversion.run_conversion(batch["id"], crash_after_items=1)
        rolled_back = conversion.recover_inflight()
        assert len(rolled_back) == 1
        assert rolled_back[0]["status"] == "已回滚"
        # 重启后每个检查点都能读回完整的源格式恢复批次。
        for checkpoint_id in _v1_checkpoint_ids():
            state = _state(checkpoint_id)
            assert state["state_version"] == "v1"
            assert state["staged_payload"] is None
            assert state["payload"]["order_id"].startswith("ORD-")
        # 重新发起的完整批次可以正常转换提交。
        new_batch = conversion.create_batch(
            pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
            checkpoint_ids=_v1_checkpoint_ids())
        committed = conversion.run_conversion(new_batch["id"])
        assert committed["status"] == "已提交"

    def test_rollback_restores_complete_version_only(self):
        batch = self._online_batch()
        conversion.run_conversion(batch["id"])
        rolled = conversion.rollback(batch["id"], reason="上线后异常回退")
        assert rolled["status"] == "已回滚"
        for item in rolled["items"]:
            assert item["status"] == "已回滚"
            assert item["converted_payload"] is None
        for checkpoint_id in _v1_checkpoint_ids():
            checkpoint = registry.get_checkpoint(checkpoint_id)
            assert checkpoint["status"] == "已回滚"
            state = _state(checkpoint_id)
            # 已提交批次回滚后真正切回源格式完整版本，而非只改状态标记。
            assert state["state_version"] == "v1"
            assert "order_id" in state["payload"]
            assert "order_no" not in state["payload"]
            assert state["staged_payload"] is None
            # 格式历史完整保留，已完成的转换仍可追查实际装载格式。
            assert {step["format"] for step in state["format_history"]} == {"源格式", "目标格式"}
            assert any(step.get("rollback") for step in state["format_history"])

    def test_partial_commit_is_impossible(self):
        batch = self._online_batch()
        conversion.validate_dependencies(batch["id"])
        # 手动制造“只暂存了一半”的状态，提交必须整体回滚而非半批生效。
        batch_row = repo.find("_recovery_batches", batch["id"])
        batch_row["status"] = "转换中"
        first_item = repo.find("_recovery_batch_items", batch_row["item_ids"][0])
        first_item["staged"] = True
        first_item["converted_payload"] = {"order_no": "X"}
        with pytest.raises(RecoveryError):
            conversion.commit(batch["id"])
        assert conversion.get_batch(batch["id"])["status"] == "已回滚"


class TestEvolutionConsumesBatch:
    def test_consume_validates_effective_scope_and_writes_back(self):
        checkpoint_ids = _v1_checkpoint_ids()
        for op_id in ("OP-A", "OP-B"):
            coordinator.switch_operator_version(op_id, "v2")
        batch = conversion.create_batch(
            pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
            checkpoint_ids=checkpoint_ids)
        conversion.run_conversion(batch["id"])
        evolution = conversion.consume_batch_for_evolution(_evolution_id(), batch["id"])
        assert evolution["status"] == "已生效"
        assert evolution["consumed_batch_id"] == batch["id"]
        # 实际生效范围回写，且与提交范围一致。
        assert sorted(evolution["effective_scope"]) == sorted(checkpoint_ids)

    def test_consume_rejects_scope_overflow(self):
        for op_id in ("OP-A", "OP-B"):
            coordinator.switch_operator_version(op_id, "v2")
        only_first = _v1_checkpoint_ids()[:1]
        batch = conversion.create_batch(
            pipeline_id="PIPE-RECO-01", evolution_id=_evolution_id(),
            checkpoint_ids=only_first)
        conversion.run_conversion(batch["id"])
        evolution = repo.find("_recovery_evolutions", _evolution_id())
        # 演进声明的生效范围不包含该检查点。
        evolution["effective_scope"] = [999]
        with pytest.raises(RecoveryError):
            conversion.consume_batch_for_evolution(_evolution_id(), batch["id"])
