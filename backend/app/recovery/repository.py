"""恢复子系统仓储：在全局内存 Store 上维护内部表。

真实项目里这些表对应数据库事务表；这里沿用平台只依赖标准库的约定，
用 Store 内部表模拟，但所有写入都收敛到本仓储，服务层不直接拼行。
内部表统一以 ``_recovery_`` 开头，不会出现在业务模块概览里。
"""
from __future__ import annotations

from typing import Any

from app.store import store

# 各内部表的行结构见 seed_recovery.py 中的示例数据与服务层用法。
TABLES = (
    "_recovery_checkpoints",   # 恢复检查点清单（转换结果回写到这里）
    "_recovery_states",        # 检查点状态快照：版本、位点、状态载荷
    "_recovery_evolutions",    # 结构演进记录（与旧检查点各自登记）
    "_recovery_batches",       # 状态格式转换批次
    "_recovery_batch_items",   # 批次内条目（一个检查点状态一行，原子提交单位）
    "_recovery_queue",         # 恢复队列（旧恢复排队）
    "_recovery_operators",     # 流水线算子及其各版本可用性
    "_recovery_active",        # 上线后的活动位点
    "_recovery_audits",        # 切换/装载/核对的审计记录
)


def table(name: str) -> list[dict[str, Any]]:
    if name not in TABLES:
        raise KeyError(f"恢复子系统未声明内部表：{name}")
    return store.rows(name)


def next_id(name: str) -> int:
    return max((int(row.get("id", 0)) for row in table(name)), default=0) + 1


def find(name: str, row_id: int) -> dict[str, Any] | None:
    return store.find(name, row_id)


def find_by(name: str, key: str, value: Any) -> dict[str, Any] | None:
    for row in table(name):
        if row.get(key) == value:
            return row
    return None


def where(name: str, key: str, value: Any) -> list[dict[str, Any]]:
    return [row for row in table(name) if row.get(key) == value]


def insert(name: str, row: dict[str, Any]) -> dict[str, Any]:
    row.setdefault("id", next_id(name))
    table(name).append(row)
    return row


def reset_tables() -> None:
    """清空全部恢复内部表；测试隔离与重新引导示例数据时使用。"""
    for name in TABLES:
        store.rows(name).clear()
