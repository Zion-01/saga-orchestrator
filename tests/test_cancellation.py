"""Phase 4, part 2: cancellation ceremony.

A sibling failure cancels every in-flight branch through the TaskGroup's
native semantics. Phase 4 adds the ceremony on top: the cancellation is
*journaled* (never swallowed), a cancelled in-flight step is unconditionally
UNCERTAIN (its request may already have landed), handlers get a bounded grace
period to unwind, and ``cancellable=False`` steps are shielded and allowed to
finish before being compensated normally.
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


async def _run(journal_path, spec, **kw):
    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal, **kw)
        with pytest.raises(WorkflowFailed) as exc_info:
            await orchestrator.run()
    return orchestrator, exc_info.value


def _types(records, step_id):
    return [r.type for r in records if r.step_id == step_id]


# --- the phase 4 exit criterion -------------------------------------------------


async def test_sibling_failure_cancels_in_flight_branch_and_rolls_it_back(journal_path) -> None:
    started = asyncio.Event()
    comped: list[str] = []

    async def slow(ctx: StepContext):
        started.set()
        await asyncio.sleep(30)
        return "never"  # pragma: no cover

    async def slow_comp(ctx: StepContext, result) -> None:
        comped.append("slow")

    async def boom(ctx: StepContext):
        await started.wait()
        raise PermanentError("branch B failed")

    spec = WorkflowSpec.of(
        WF,
        [Step(id="slow", handler=slow, compensate=slow_comp), Step(id="boom", handler=boom)],
    )

    orch, err = await _run(journal_path, spec)

    assert err.step_id == "boom"
    assert comped == ["slow"]
    assert orch.snapshot.steps["slow"].state is StepState.COMPENSATED
    assert orch.snapshot.status is WorkflowState.COMPENSATED

    records = read_journal(journal_path).records
    assert _types(records, "slow") == [
        RecordType.STEP_STARTED,
        RecordType.STEP_CANCELLED,
        RecordType.STEP_UNCERTAIN,  # cancelled in flight => unconditionally uncertain
        RecordType.COMPENSATION_STARTED,
        RecordType.COMPENSATION_COMPLETED,
    ]
    uncertain = next(r for r in records if r.step_id == "slow" and r.type is RecordType.STEP_UNCERTAIN)
    assert uncertain.payload["reason"]
    assert fold(records) == orch.snapshot


async def test_step_only_waiting_on_dependencies_is_skipped_with_no_side_records(journal_path) -> None:
    async def a(ctx: StepContext):
        raise PermanentError("boom")

    async def c(ctx: StepContext):
        return "c"  # pragma: no cover - never scheduled

    spec = WorkflowSpec.of(
        WF,
        [Step(id="a", handler=a), Step(id="c", handler=c, depends_on=frozenset({"a"}))],
    )

    orch, _ = await _run(journal_path, spec)

    assert orch.snapshot.steps["c"].state is StepState.SKIPPED
    records = read_journal(journal_path).records
    assert _types(records, "c") == [RecordType.STEP_SKIPPED]
    assert fold(records) == orch.snapshot


# --- grace period -------------------------------------------------------------


async def test_handler_that_ignores_cancellation_past_grace_is_left_uncertain(journal_path) -> None:
    started = asyncio.Event()
    comped: list[str] = []

    async def stubborn(ctx: StepContext):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await asyncio.sleep(0.3)  # ignore the cancellation well past the grace period
            raise
        return "never"  # pragma: no cover

    async def stubborn_comp(ctx: StepContext, result) -> None:
        comped.append("stubborn")

    async def boom(ctx: StepContext):
        await started.wait()
        raise PermanentError("boom")

    spec = WorkflowSpec.of(
        WF,
        [Step(id="stubborn", handler=stubborn, compensate=stubborn_comp), Step(id="boom", handler=boom)],
    )

    orch, _ = await _run(journal_path, spec, cancel_grace_s=0.05)

    records = read_journal(journal_path).records
    slow = _types(records, "stubborn")
    assert slow[:2] == [RecordType.STEP_STARTED, RecordType.STEP_UNCERTAIN]
    reason = next(
        r for r in records if r.step_id == "stubborn" and r.type is RecordType.STEP_UNCERTAIN
    ).payload["reason"]
    assert "grace" in reason
    # it still gets compensated defensively (compensate_on_uncertain defaults True)
    assert comped == ["stubborn"]
    assert orch.snapshot.steps["stubborn"].state is StepState.COMPENSATED
    assert fold(records) == orch.snapshot


# --- cancellable=False ------------------------------------------------------


async def test_non_cancellable_step_is_allowed_to_finish_then_compensated(journal_path) -> None:
    started = asyncio.Event()
    finished = asyncio.Event()
    comped: list[str] = []

    async def critical(ctx: StepContext):
        started.set()
        await asyncio.sleep(0.1)  # would be cancelled if it were cancellable
        finished.set()
        return "committed"

    async def critical_comp(ctx: StepContext, result) -> None:
        assert result == "committed"
        comped.append("critical")

    async def boom(ctx: StepContext):
        await started.wait()
        raise PermanentError("boom")

    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="critical", handler=critical, compensate=critical_comp, cancellable=False),
            Step(id="boom", handler=boom),
        ],
    )

    orch, _ = await _run(journal_path, spec)

    assert finished.is_set()  # it ran to completion despite the sibling failure
    assert comped == ["critical"]
    assert orch.snapshot.steps["critical"].state is StepState.COMPENSATED
    records = read_journal(journal_path).records
    assert _types(records, "critical") == [
        RecordType.STEP_STARTED,
        RecordType.STEP_COMPLETED,
        RecordType.COMPENSATION_STARTED,
        RecordType.COMPENSATION_COMPLETED,
    ]
    assert fold(records) == orch.snapshot


# --- ordering: a cancelled (uncertain) branch unwinds before older completions --


async def test_rollback_order_places_a_cancelled_branch_by_its_start_lsn(journal_path) -> None:
    order: list[str] = []

    def mk(name, *, deps=frozenset(), body=None):
        async def handler(ctx: StepContext):
            if body is not None:
                return await body(ctx)
            return name

        async def comp(ctx: StepContext, result) -> None:
            order.append(name)

        return Step(id=name, handler=handler, compensate=comp, depends_on=deps)

    async def sleeps(ctx):
        await asyncio.sleep(30)
        return "never"  # pragma: no cover

    async def fails(ctx):
        raise PermanentError("boom")

    # a -> b ; b -> c (sleeps, gets cancelled) ; b -> e (fails, triggers rollback)
    spec = WorkflowSpec.of(
        WF,
        [
            mk("a"),
            mk("b", deps=frozenset({"a"})),
            mk("c", deps=frozenset({"b"}), body=sleeps),
            mk("e", deps=frozenset({"b"}), body=fails),
        ],
    )

    orch, _ = await _run(journal_path, spec)

    # c started after b completed, so c (uncertain) unwinds first, then b, then a.
    assert order == ["c", "b", "a"]
    assert orch.snapshot.steps["c"].state is StepState.COMPENSATED
    records = read_journal(journal_path).records
    assert fold(records) == orch.snapshot
