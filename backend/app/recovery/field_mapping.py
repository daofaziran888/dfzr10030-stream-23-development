"""字段映射引擎：结构演进携带的存量字段映射如何作用于状态载荷。

映射是声明式的，每条规则一个操作：

* ``keep``    保留字段（可同时改名，改名以 rename 表达）
* ``rename``  字段改名，值原样搬运
* ``default`` 目标字段填默认值（源字段缺失时使用；若源字段存在则保留原值）
* ``drop``    目标结构删除该字段

设计原则：转换只搬运/补齐/删除字段，绝不静默伪造业务值；无法解释的
映射或丢失必填字段都会抛 :class:`RecoveryError`，由批次整体拒绝。
"""
from __future__ import annotations

from typing import Any

from app.recovery.errors import RecoveryError

SUPPORTED_OPS = ("keep", "rename", "default", "drop")


def validate_mapping(mapping: list[dict[str, Any]]) -> None:
    """校验一份字段映射的结构合法性与目标字段唯一性。"""
    if not isinstance(mapping, list):
        raise RecoveryError("字段映射必须是规则列表")
    targets: set[str] = set()
    for index, rule in enumerate(mapping):
        if not isinstance(rule, dict):
            raise RecoveryError(f"第 {index + 1} 条映射规则不是对象")
        op = rule.get("op")
        if op not in SUPPORTED_OPS:
            raise RecoveryError(
                f"第 {index + 1} 条映射操作「{op}」不支持，允许：{'、'.join(SUPPORTED_OPS)}")
        source = rule.get("source")
        target = rule.get("target")
        if op in ("keep", "rename") and not source:
            raise RecoveryError(f"第 {index + 1} 条 {op} 规则缺少 source 字段")
        if op != "drop" and not target:
            raise RecoveryError(f"第 {index + 1} 条 {op} 规则缺少 target 字段")
        if op == "rename" and not target:
            raise RecoveryError(f"第 {index + 1} 条 rename 规则缺少 target 字段")
        if op != "drop" and target in targets:
            raise RecoveryError(f"目标字段「{target}」被多条规则映射，无法确定唯一来源")
        if op != "drop":
            targets.add(target)


def apply_mapping(payload: dict[str, Any], mapping: list[dict[str, Any]]) -> dict[str, Any]:
    """把源格式状态载荷按映射转换成目标格式。

    位点等运行时字段不在映射里处理，由批次服务单独原样保留，
    保证转换前后消费位置不漂移。
    """
    validate_mapping(mapping)
    result: dict[str, Any] = {}
    for rule in mapping:
        op = rule["op"]
        source = rule.get("source")
        target = rule.get("target")
        if op == "drop":
            # 显式删除：不出现在结果里即完成删除。
            continue
        if op == "keep":
            if source not in payload:
                raise RecoveryError(f"keep 规则的源字段「{source}」在存量状态中缺失")
            result[target] = payload[source]
        elif op == "rename":
            if source not in payload:
                raise RecoveryError(f"rename 规则的源字段「{source}」在存量状态中缺失")
            result[target] = payload[source]
        elif op == "default":
            # 源字段仍存在时优先保留存量值，兼容旧数据；缺失才补默认值。
            result[target] = payload[source] if source in payload else rule.get("value")
    return result


def mapping_covers_source_fields(mapping: list[dict[str, Any]], payload: dict[str, Any]) -> list[str]:
    """返回存量载荷里没有被任何映射规则覆盖的字段（审计用，不直接拦截）。"""
    covered = {rule.get("source") for rule in mapping if rule.get("op") in ("keep", "rename", "drop", "default")}
    return sorted(field for field in payload if field not in covered)
