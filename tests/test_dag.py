"""DAG validation and ordering.

Exit criterion for phase 1 (CLAUDE.md): the DAG validator rejects cycles,
dangling ``depends_on``, and duplicate step ids -- and ordering helpers are
deterministic.
"""

from __future__ import annotations

import pytest

from saga.dag import ancestors, descendants, reverse_topological_levels, topological_levels, validate
from saga.errors import DagError
from saga.models import Step, WorkflowSpec

# A diamond: a -> b, a -> c, {b, c} -> d
DIAMOND = {"a": frozenset(), "b": frozenset({"a"}), "c": frozenset({"a"}), "d": frozenset({"b", "c"})}


async def _noop(ctx):  # pragma: no cover - never actually invoked in phase 1
    return None


def _step(step_id: str, depends_on: frozenset[str] = frozenset()) -> Step:
    return Step(id=step_id, handler=_noop, depends_on=depends_on)


# --- validate() ---------------------------------------------------------------


def test_validate_accepts_diamond() -> None:
    validate(DIAMOND)  # must not raise


def test_validate_rejects_self_edge() -> None:
    with pytest.raises(DagError):
        validate({"a": frozenset({"a"})})


def test_validate_rejects_dangling_dependency() -> None:
    with pytest.raises(DagError):
        validate({"a": frozenset({"ghost"})})


def test_validate_rejects_two_cycle() -> None:
    with pytest.raises(DagError):
        validate({"a": frozenset({"b"}), "b": frozenset({"a"})})


def test_validate_rejects_three_cycle() -> None:
    with pytest.raises(DagError):
        validate({"a": frozenset({"c"}), "b": frozenset({"a"}), "c": frozenset({"b"})})


def test_validate_accepts_empty_graph() -> None:
    validate({})


# --- topological_levels / reverse_topological_levels --------------------------


def test_topological_levels_diamond() -> None:
    assert topological_levels(DIAMOND) == [["a"], ["b", "c"], ["d"]]


def test_reverse_topological_levels_diamond() -> None:
    assert reverse_topological_levels(DIAMOND) == [["d"], ["b", "c"], ["a"]]


def test_topological_levels_break_ties_by_sorted_id() -> None:
    # Independent roots must come back sorted, not in insertion order.
    graph = {"z": frozenset(), "a": frozenset(), "m": frozenset()}
    assert topological_levels(graph) == [["a", "m", "z"]]


def test_topological_levels_linear_chain() -> None:
    graph = {"a": frozenset(), "b": frozenset({"a"}), "c": frozenset({"b"})}
    assert topological_levels(graph) == [["a"], ["b"], ["c"]]


# --- ancestors / descendants ---------------------------------------------------


def test_ancestors_of_leaf_is_full_upstream_set() -> None:
    assert ancestors(DIAMOND, "d") == {"a", "b", "c"}


def test_ancestors_of_root_is_empty() -> None:
    assert ancestors(DIAMOND, "a") == set()


def test_descendants_of_root_is_full_downstream_set() -> None:
    assert descendants(DIAMOND, "a") == {"b", "c", "d"}


def test_descendants_of_leaf_is_empty() -> None:
    assert descendants(DIAMOND, "d") == set()


# --- WorkflowSpec: duplicate ids, dangling deps, self-edges --------------------


def test_workflow_spec_of_builds_diamond() -> None:
    spec = WorkflowSpec.of(
        "wf-1",
        [_step("a"), _step("b", frozenset({"a"})), _step("c", frozenset({"a"})), _step("d", frozenset({"b", "c"}))],
    )
    assert set(spec.steps) == {"a", "b", "c", "d"}
    assert reverse_topological_levels(spec.depends_on) == [["d"], ["b", "c"], ["a"]]


def test_workflow_spec_of_rejects_duplicate_step_id() -> None:
    with pytest.raises(DagError):
        WorkflowSpec.of("wf-1", [_step("a"), _step("a")])


def test_workflow_spec_rejects_dangling_dependency() -> None:
    with pytest.raises(DagError):
        WorkflowSpec.of("wf-1", [_step("a", frozenset({"ghost"}))])


def test_workflow_spec_rejects_cycle() -> None:
    with pytest.raises(DagError):
        WorkflowSpec.of("wf-1", [_step("a", frozenset({"b"})), _step("b", frozenset({"a"}))])


def test_step_rejects_self_dependency_at_construction() -> None:
    with pytest.raises(DagError):
        _step("a", frozenset({"a"}))


def test_step_rejects_empty_id() -> None:
    with pytest.raises(DagError):
        Step(id="", handler=_noop)
