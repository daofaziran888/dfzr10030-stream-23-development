"""恢复子系统测试（三）：恢复队列、版本切换冲突、装载与位点核对。

对应需求：
- 旧恢复排队而新结构要接收数据时的版本切换/恢复时效冲突；
- 每个恢复批只能装载内部兼容的一套状态，旧恢复按实际装载格式追查；
- 给有依赖次序的两个算子做切换验证；
- 核对上线后活动位点与数据库导出的实际输入范围。
"""
import pytest

from app.recovery import repository as repo
from app.recovery.errors import RecoveryError
from app.recovery.service import conversion, coordinator, registry


def _recoverable_checkpoints():
    registry.assess_all()
    return [c for c in registry.list_checkpoints() if c["recoverable"] is True]


class TestQueueAndCutoverConflict:
    def test_unrecoverable_checkpoint_cannot_enqueue(self):
        orphan = next(c for c in registry.list_checkpoints() if c["state_version"] == "v0")
        registry.assess_checkpoint(orphan["id"])
        with pytest.raises(RecoveryError):
            coordinator.enqueue(orphan["id"])

    def test_cutover_rejected_while_old_recovery_queued(self):
        checkpoint = _recoverable_checkpoints()[0]
        coordinator.enqueue(checkpoint["id"])
        with pytest.raises(RecoveryError) as exc:
            coordinator.begin_cutover("PIPE-RECO-01", "v2")
        assert "版本切换与恢复时效冲突" in str(exc.value)

    def test_cutover_allowed_after_queue_drained(self):
        checkpoint = _recoverable_checkpoints()[0]
        entry = coordinator.enqueue(checkpoint["id"])
        coordinator.process_recovery_batch([entry["id"]])
        active = coordinator.begin_cutover("PIPE-RECO-01", "v2")
        assert active["version"] == "v2"

    def test_duplicate_enqueue_rejected(self):
        checkpoint = _recoverable_checkpoints()[0]
        coordinator.enqueue(checkpoint["id"])
        with pytest.raises(RecoveryError):
            coordinator.enqueue(checkpoint["id"])


class TestSingleCompatibleLoad:
    def test_recovery_batch_loads_one_compatible_state_set(self):
        checkpoints = _recoverable_checkpoints()
        entries = [coordinator.enqueue(c["id"]) for c in checkpoints]
        # 两个检查点都钉在 v1，属于一套内部兼容状态。
        result = coordinator.process_recovery_batch([entry["id"] for entry in entries])
        assert result["pinned_version"] == "v1"
        assert len(result["loaded"]) == 2
        for loaded in result["loaded"]:
            assert loaded["load_format"] == "源格式"
        for checkpoint in checkpoints:
            refreshed = registry.get_checkpoint(checkpoint["id"])
            assert refreshed["status"] == "已恢复"
            # 已完成的旧恢复保留实际装载格式，可按格式历史追查。
            state = repo.find_by("_recovery_states", "checkpoint_id", checkpoint["id"])
            assert state["format_history"][-1]["format"] == "源格式"

    def test_mixed_versions_in_one_batch_rejected(self):
        checkpoints = _recoverable_checkpoints()
        first_entry = coordinator.enqueue(checkpoints[0]["id"])
        # 再排一个并直接改写存储行，模拟新旧版本混装进同一恢复批。
        second = coordinator.enqueue(checkpoints[1]["id"])
        stored_second = repo.find("_recovery_queue", second["id"])
        stored_second["pinned_version"] = "v2"
        with pytest.raises(RecoveryError) as exc:
            coordinator.process_recovery_batch([first_entry["id"], second["id"]])
        assert "一套内部兼容状态" in str(exc.value)

    def test_batch_rejected_when_pinned_version_offline(self):
        checkpoint = _recoverable_checkpoints()[0]
        entry = coordinator.enqueue(checkpoint["id"])
        coordinator.switch_operator_version("OP-A", "v2")
        coordinator.switch_operator_version("OP-B", "v2")
        # 钉住的是 v1，但 v1 依赖已下线：整批拒绝，不做部分装载。
        with pytest.raises(RecoveryError):
            coordinator.process_recovery_batch([entry["id"]])
        refreshed = registry.get_checkpoint(checkpoint["id"])
        assert refreshed["status"] == "排队中"


class TestOrderedOperatorSwitch:
    def test_switch_order_validation(self):
        result = coordinator.validate_ordered_switch("OP-A", "OP-B", "v2")
        # v2 尚未上线时先拒绝，并指出阻塞算子。
        assert result["decision"] == "拒绝切换"
        assert result["switch_order"] == ["OP-A", "OP-B"]
        assert result["blockers"]

    def test_switch_allowed_once_chain_online(self):
        coordinator.switch_operator_version("OP-A", "v2")
        coordinator.switch_operator_version("OP-B", "v2")
        result = coordinator.validate_ordered_switch("OP-A", "OP-B", "v2")
        assert result["decision"] == "允许切换"
        assert result["blockers"] == []

    def test_wrong_order_rejected(self):
        with pytest.raises(RecoveryError):
            coordinator.validate_ordered_switch("OP-B", "OP-A", "v2")

    def test_non_dependent_pair_rejected(self):
        # 再造一个独立算子，验证“必须真实存在依赖边”。
        registry.register_operator({
            "operator_id": "OP-C", "pipeline_id": "PIPE-RECO-01",
            "depends_on": [], "versions": {"v1": "在线", "v2": "在线"}})
        with pytest.raises(RecoveryError):
            coordinator.validate_ordered_switch("OP-A", "OP-C", "v2")


class TestPositionReconciliation:
    def test_reconcile_detects_gap_and_full_coverage(self):
        exported = [
            {"partition": "p0", "start": 0, "end": 120},
            {"partition": "p1", "start": 0, "end": 80},
        ]
        # 种子活动位点 p0=100（有缺口）、p1=80（全覆盖）。
        result = coordinator.reconcile_positions("PIPE-RECO-01", exported)
        assert result["all_covered"] is False
        by_partition = {row["partition"]: row for row in result["partitions"]}
        assert by_partition["p0"]["status"] == "存在缺口"
        assert by_partition["p0"]["gap"] == 20
        assert by_partition["p1"]["status"] == "已覆盖"

        # 活动位点推进到导出上界后应全部覆盖。
        coordinator.record_active_positions(
            "PIPE-RECO-01", {"p0": 120, "p1": 80}, "2026-11-10T10:00:00Z")
        healed = coordinator.reconcile_positions("PIPE-RECO-01", exported)
        assert healed["all_covered"] is True

    def test_reconcile_flags_missing_and_regressed_partition(self):
        exported = [
            {"partition": "p2", "start": 0, "end": 10},
            {"partition": "p0", "start": 200, "end": 300},
        ]
        result = coordinator.reconcile_positions("PIPE-RECO-01", exported)
        statuses = {row["partition"]: row["status"] for row in result["partitions"]}
        assert statuses["p2"] == "缺失位点"
        assert statuses["p0"] == "位点倒退"
