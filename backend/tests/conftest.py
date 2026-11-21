"""测试配置：每个测试前重置 store 与恢复子系统，保证用例互相隔离。"""
import pytest
from app.store import store as _store
from app.recovery import repository as recovery_repo
from app.recovery.seed_recovery import bootstrap as bootstrap_recovery


@pytest.fixture(autouse=True)
def fresh_store():
    _store._tables.clear()
    from app.seed import SEED_ROWS
    for name, rows in SEED_ROWS.items():
        _store._tables[name] = [dict(r) for r in rows]
    recovery_repo.reset_tables()
    bootstrap_recovery()
    yield
