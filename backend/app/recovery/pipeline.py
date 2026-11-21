"""流水线上下游依赖：DAG 校验、版本可用性与位点装载决策。

算子（stream_transform 等）在结构切换后会同时存在新旧两套状态格式版本，
是否能装入某个检查点位点，取决于该算子所依赖的上下游在对应版本下是否在线。
"""
from __future__ import annotations

from typing import Any

from app.recovery import repository as repo
from app.recovery.errors import RecoveryError


class PipelineGraph:
    """由算子表的 depends_on 字段构造的有向无环图。"""

    def __init__(self, operators: list[dict[str, Any]]):
        self.operators = {op["operator_id"]: op for op in operators}
        self.edges: dict[str, list[str]] = {
            op["operator_id"]: list(op.get("depends_on", [])) for op in operators
        }

    def ensure_dag(self) -> None:
        """DFS 着色检测环；有环的流水线无法确定切换顺序，直接拒绝。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {node: WHITE for node in self.edges}

        def visit(node: str, stack: list[str]) -> None:
            color[node] = GRAY
            for upstream in self.edges.get(node, []):
                if upstream not in self.edges:
                    raise RecoveryError(f"算子「{node}」依赖的上游「{upstream}」不存在")
                if color[upstream] == GRAY:
                    cycle = " -> ".join([*stack, upstream])
                    raise RecoveryError(f"流水线依赖存在环：{cycle}")
                if color[upstream] == WHITE:
                    visit(upstream, [*stack, upstream])
            color[node] = BLACK

        for node in self.edges:
            if color[node] == WHITE:
                visit(node, [node])

    def topological_order(self) -> list[str]:
        """上游在前的切换顺序：被依赖者先切换，依赖者后切换。"""
        self.ensure_dag()
        ordered: list[str] = []
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visited:
                return
            for upstream in self.edges.get(node, []):
                visit(upstream)
            visited.add(node)
            ordered.append(node)

        for node in self.edges:
            visit(node)
        return ordered

    def version_available(self, operator_id: str, version: str) -> bool:
        op = self.operators.get(operator_id)
        if op is None:
            return False
        return bool(op.get("versions", {}).get(version) == "在线")

    def dependency_chain_ready(self, operator_id: str, version: str) -> dict[str, Any]:
        """检查某算子在指定版本下，其上下游整条依赖链是否都可用。"""
        missing: list[str] = []
        chain = list(self.edges.get(operator_id, []))
        # 再向下找一层下游，覆盖“上下游依赖”两侧。
        for other_id, upstreams in self.edges.items():
            if operator_id in upstreams and other_id not in chain:
                chain.append(other_id)
        for node in [operator_id, *chain]:
            if not self.version_available(node, version):
                missing.append(node)
        return {"ready": not missing, "missing": sorted(set(missing)), "chain": sorted(set(chain))}


def load_operator_graph(pipeline_id: str) -> PipelineGraph:
    operators = [row for row in repo.table("_recovery_operators") if row.get("pipeline_id") == pipeline_id]
    if not operators:
        raise RecoveryError(f"流水线「{pipeline_id}」没有登记任何算子，无法校验依赖")
    graph = PipelineGraph(operators)
    graph.ensure_dag()
    return graph


def decide_load(operator_id: str, preferred_version: str, fallback_version: str) -> dict[str, Any]:
    """按新旧依赖可用性决定是否装入位点。

    优先尝试目标（新）版本；目标版本链路未就绪时回退源（旧）版本；
    两套都不可用则拒绝装载，避免装入半截依赖的位点。
    """
    op = repo.find_by("_recovery_operators", "operator_id", operator_id)
    if op is None:
        raise RecoveryError(f"算子「{operator_id}」未登记")
    graph = load_operator_graph(op["pipeline_id"])
    target_check = graph.dependency_chain_ready(operator_id, preferred_version)
    if target_check["ready"]:
        return {
            "load": True,
            "version": preferred_version,
            "format": "目标格式",
            "reason": "目标版本上下游依赖全部在线",
        }
    source_check = graph.dependency_chain_ready(operator_id, fallback_version)
    if source_check["ready"]:
        return {
            "load": True,
            "version": fallback_version,
            "format": "源格式",
            "reason": f"目标版本缺少在线依赖（{'、'.join(target_check['missing'])}），回退源版本",
        }
    return {
        "load": False,
        "version": None,
        "format": None,
        "reason": (
            f"目标版本缺少：{'、'.join(target_check['missing'])}；"
            f"源版本缺少：{'、'.join(source_check['missing'])}"
        ),
    }
