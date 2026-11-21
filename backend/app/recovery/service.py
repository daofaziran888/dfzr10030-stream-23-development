"""恢复子系统门面：对外只暴露这一组服务对象，路由层不直接碰仓储。"""
from __future__ import annotations

from app.recovery.conversion_service import ConversionService
from app.recovery.coordinator_service import RecoveryCoordinator
from app.recovery.registry_service import RegistryService
from app.recovery.seed_recovery import bootstrap

registry = RegistryService()
conversion = ConversionService()
coordinator = RecoveryCoordinator()

__all__ = ["registry", "conversion", "coordinator", "bootstrap"]
