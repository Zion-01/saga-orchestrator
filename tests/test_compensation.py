"""Phase 4, part 1: compensation.

Reverse-order rollback, partial rollback (PENDING steps skipped, not
compensated), the compensator seeing the forward result, and -- as always --
the live snapshot folding byte-identically from the journal it wrote.
"""

from __future__ import annotations

import asyncio

import pytest

from saga.engine import Orchestrator
from saga.errors import PermanentError, WorkflowFailed
from saga.journal import Journal, read_journal
from saga.models import Step, StepContext, WorkflowSpec
from saga.records import RecordType
from saga.replay import fold
from saga.states import StepState, WorkflowState

WF = "wf-1"


def _step(step_id, handler, *, depends_on=frozenset(), **kw) -> Step:
    return Step(id=step_id, handler=handler, depends_on=depends_on, **kw)


async def _run(journal_path, spec, **kw):
    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal, **kw)
        with pytest.raises(WorkflowFailed) as exc_info:
            await orchestrator.run()
    return orchestrator, exc_info.value


def _types(records, step_id):
    return [r.type for r in records if r.step_id == step_id]


# --- reverse-order rollback --------------------------------------------------


async def test_linear_rollback_runs_in_reverse_completion_order(journal_path) -> None:
    ledger: list[str] = []

    def _mk(name, *, fail=False):
        async def handler(ctx: StepContext):
            if fail:
                raise PermanentError("boom", status_code=400)
            return f"{name}-result"

        async def comp(ctx: StepContext, result) -> None:
            ledger.append(name)

        return name, handler, comp

    specs = []
    prev = None
    for name, fail in (("a", False), ("b", False), ("c", True)):
        n, h, c = _mk(name, fail=fail)
        specs.append(_step(n, h, compensate=c, depends_on=frozenset({prev} if prev else set())))
        prev = name
    spec = WorkflowSpec.of(WF, specs)

    orch, _ = await _run(journal_path, spec)

    assert ledger == ["b", "a"]  # exact inverse of completion order a, b
    snap = orch.snapshot
    assert snap.status is WorkflowState.COMPENSATED
    assert snap.steps["a"].state is StepState.COMPENSATED
    assert snap.steps["b"].state is StepState.COMPENSATED
    assert snap.steps["c"].state is StepState.FAILED

    records = read_journal(journal_path).records
    comp_completed = [r.step_id for r in records if r.type is RecordType.COMPENSATION_COMPLETED]
    assert comp_completed == ["b", "a"]
    assert fold(records) == snap


async def test_compensator_receives_the_forward_result(journal_path) -> None:
    seen: dict[str, object] = {}

    async def a(ctx: StepContext):
        return {"booking_id": 42}

    async def a_comp(ctx: StepContext, result) -> None:
        seen["a"] = result

    async def b(ctx: StepContext):
        raise PermanentError("no")

    spec = WorkflowSpec.of(
        WF,
        [
            _step("a", a, compensate=a_comp),
            _step("b", b, depends_on=frozenset({"a"})),
        ],
    )

    await _run(journal_path, spec)
    assert seen["a"] == {"booking_id": 42}


# --- partial rollback: nothing that never happened is undone ----------------


async def test_pending_step_is_skipped_never_compensated(journal_path) -> None:
    comped: list[str] = []

    def _mk(name, *, fail=False):
        async def handler(ctx: StepContext):
            if fail:
                raise PermanentError("boom")
            return name

        async def comp(ctx: StepContext, result) -> None:
            comped.append(name)

        return _step(name, handler, compensate=comp)

    # a -> b -> c(fails) -> d ; d never starts
    a = _mk("a")
    b = Step(id="b", handler=_mk("b").handler, compensate=_mk("b").compensate, depends_on=frozenset({"a"}))
    c = Step(id="c", handler=_mk("c", fail=True).handler, compensate=_mk("c").compensate, depends_on=frozenset({"b"}))
    d = Step(id="d", handler=_mk("d").handler, compensate=_mk("d").compensate, depends_on=frozenset({"c"}))
    spec = WorkflowSpec.of(WF, [a, b, c, d])

    orch, _ = await _run(journal_path, spec)

    assert orch.snapshot.steps["d"].state is StepState.SKIPPED
    assert "d" not in comped
    assert comped == ["b", "a"]

    records = read_journal(journal_path).records
    assert _types(records, "d") == [RecordType.STEP_SKIPPED]
    skip = next(r for r in records if r.step_id == "d")
    assert skip.payload["quarantined_by"] is None
    assert fold(records) == orch.snapshot


async def test_completed_step_without_a_compensator_is_left_completed(journal_path) -> None:
    async def a(ctx: StepContext):
        return "a"

    async def b(ctx: StepContext):
        raise PermanentError("no")

    spec = WorkflowSpec.of(
        WF,
        [_step("a", a), _step("b", b, depends_on=frozenset({"a"}))],
    )

    orch, _ = await _run(journal_path, spec)

    assert orch.snapshot.steps["a"].state is StepState.COMPLETED
    assert orch.snapshot.status is WorkflowState.COMPENSATED
    records = read_journal(journal_path).records
    assert _types(records, "a") == [RecordType.STEP_STARTED, RecordType.STEP_COMPLETED]


async def test_forward_failure_with_nothing_completed_compensates_vacuously(journal_path) -> None:
    async def a(ctx: StepContext):
        raise PermanentError("immediately")

    spec = WorkflowSpec.of(WF, [_step("a", a)])
    orch, _ = await _run(journal_path, spec)

    assert orch.snapshot.status is WorkflowState.COMPENSATED
    assert orch.snapshot.steps["a"].state is StepState.FAILED
    records = read_journal(journal_path).records
    assert records[-1].type is RecordType.WORKFLOW_COMPENSATED
    assert fold(records) == orch.snapshot


# --- diamond: independent branches both unwind -----------------------------


async def test_diamond_rolls_back_every_completed_branch(journal_path) -> None:
    comped: list[str] = []

    def mk(name, *, fail=False, deps=frozenset()):
        async def handler(ctx: StepContext):
            await asyncio.sleep(0)
            if fail:
                raise PermanentError("boom")
            return name

        async def comp(ctx: StepContext, result) -> None:
            comped.append(name)

        return Step(id=name, handler=handler, compensate=comp, depends_on=deps)

    spec = WorkflowSpec.of(
        WF,
        [
            mk("a"),
            mk("b", deps=frozenset({"a"})),
            mk("c", deps=frozenset({"a"})),
            mk("d", deps=frozenset({"b", "c"}), fail=True),
        ],
    )

    orch, _ = await _run(journal_path, spec)

    assert orch.snapshot.status is WorkflowState.COMPENSATED
    assert set(comped) == {"a", "b", "c"}
    # a is compensated last: it completed first, so it unwinds last.
    assert comped[-1] == "a"
    records = read_journal(journal_path).records
    assert fold(records) == orch.snapshot


async def test_parallel_compensation_unwinds_reverse_topological_levels(journal_path) -> None:
    order: list[str] = []

    def mk(name, *, fail=False, deps=frozenset()):
        async def handler(ctx: StepContext):
            await asyncio.sleep(0)
            if fail:
                raise PermanentError("boom")
            return name

        async def comp(ctx: StepContext, result) -> None:
            order.append(f"{name}:start")
            await asyncio.sleep(0.02)
            order.append(f"{name}:end")

        return Step(id=name, handler=handler, compensate=comp, depends_on=deps)

    spec = WorkflowSpec.of(
        WF,
        [
            mk("a"),
            mk("b", deps=frozenset({"a"})),
            mk("c", deps=frozenset({"a"})),
            mk("d", deps=frozenset({"b", "c"}), fail=True),
        ],
    )

    orch, _ = await _run(journal_path, spec, parallel_compensation=True)

    assert orch.snapshot.status is WorkflowState.COMPENSATED
    # b and c are mutually independent: their compensations overlap in time.
    assert order.index("b:start") < order.index("c:end")
    assert order.index("c:start") < order.index("b:end")
    # a depends-on-wise precedes both, so it only starts after both finish.
    assert order.index("a:start") > order.index("b:end")
    assert order.index("a:start") > order.index("c:end")
    records = read_journal(journal_path).records
    assert fold(records) == orch.snapshot
