"""Phase 5, part 2: resume-forward and resume-rollback.

``recover(journal_file, spec)`` folds the journal to a snapshot and hands back
an :class:`~saga.engine.Orchestrator` whose ``run()`` continues from exactly
where the last incarnation stopped instead of starting over:

* a journal that stopped mid forward-DAG resumes the un-run steps;
* a journal that stopped mid-rollback resumes the un-compensated steps, a
  compensator caught in flight re-running under its original key;
* a journal already in a terminal workflow state is a clean no-op.

Every case ends by asserting ``fold(all_records) == orchestrator.snapshot`` --
the live snapshot after recovery is byte-identical to a cold fold of what was
written.
"""

from __future__ import annotations

import asyncio

import pytest

from saga.engine import Orchestrator
from saga.errors import PermanentError, SagaError, WorkflowFailed
from saga.journal import Journal, read_journal
from saga.models import Step, StepContext, WorkflowSpec
from saga.records import RecordType
from saga.recovery import recover
from saga.replay import fold
from saga.states import StepState, WorkflowState

WF = "wf-recovery"


def _truncate_records(path, keep: int) -> None:
    """Drop everything after the first ``keep`` complete lines."""
    lines = path.read_bytes().split(b"\n")
    assert lines[-1] == b""  # writer always leaves a trailing newline
    assert keep <= len(lines) - 1
    path.write_bytes(b"\n".join(lines[:keep]) + b"\n")


async def _run_fresh(path, spec, **kw):
    with Journal.open_for_write(path) as journal:
        orch = Orchestrator(spec, journal, **kw)
        try:
            await orch.run()
        except WorkflowFailed:
            pass
    return orch


async def _recover_run(path, spec, **kw):
    orch = recover(path, spec, **kw)
    try:
        snapshot = await orch.run()
    finally:
        orch.close()
    return orch, snapshot


def _lsn_of(records, rtype, step_id=None, which=0):
    hits = [r for r in records if r.type is rtype and (step_id is None or r.step_id == step_id)]
    return hits[which].lsn


# --- resume forward -------------------------------------------------------------


async def test_resume_forward_runs_only_the_unfinished_steps(journal_path) -> None:
    runs: list[str] = []

    def mk(name, deps=frozenset()):
        async def handler(ctx: StepContext):
            runs.append(name)
            return name.upper()

        return Step(id=name, handler=handler, depends_on=deps)

    spec = WorkflowSpec.of(WF, [mk("a"), mk("b", frozenset({"a"})), mk("c", frozenset({"b"}))])

    # A cold run to get a full, valid journal, then rewind to just after a's completion.
    await _run_fresh(journal_path, spec)
    full = read_journal(journal_path).records
    _truncate_records(journal_path, _lsn_of(full, RecordType.STEP_COMPLETED, "a"))

    runs.clear()
    orch, snap = await _recover_run(journal_path, spec)

    assert runs == ["b", "c"]  # a was already done
    assert snap.status is WorkflowState.COMPLETED
    assert [s for s in ("a", "b", "c") if snap.steps[s].state is not StepState.COMPLETED] == []
    assert fold(read_journal(journal_path).records) == snap


async def test_resume_forward_from_mid_step_replays_that_step(journal_path) -> None:
    runs: list[str] = []

    async def handler_a(ctx: StepContext):
        runs.append(ctx.idempotency_key)
        return "A"

    async def handler_b(ctx: StepContext):
        runs.append(ctx.idempotency_key)
        return "B"

    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="a", handler=handler_a),
            Step(id="b", handler=handler_b, depends_on=frozenset({"a"})),
        ],
    )

    await _run_fresh(journal_path, spec)
    full = read_journal(journal_path).records
    # Stop right after b's STEP_STARTED: b is a zombie, no terminal record.
    _truncate_records(journal_path, _lsn_of(full, RecordType.STEP_STARTED, "b"))

    runs.clear()
    orch, snap = await _recover_run(journal_path, spec)

    assert runs == ["wf-recovery:b:1"]  # only b, and under its original key
    assert snap.steps["b"].state is StepState.COMPLETED
    assert snap.steps["b"].attempt == 1
    assert snap.status is WorkflowState.COMPLETED
    assert fold(read_journal(journal_path).records) == snap


async def test_resume_forward_that_hits_a_failure_rolls_back(journal_path) -> None:
    comped: list[str] = []

    async def ok(ctx: StepContext):
        return "ok"

    async def undo(ctx: StepContext, result: object) -> None:
        comped.append(ctx.step_id)

    async def boom(ctx: StepContext):
        raise PermanentError("no")

    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="a", handler=ok, compensate=undo),
            Step(id="b", handler=boom, compensate=undo, depends_on=frozenset({"a"})),
        ],
    )

    await _run_fresh(journal_path, spec)  # ends COMPENSATED
    full = read_journal(journal_path).records
    _truncate_records(journal_path, _lsn_of(full, RecordType.STEP_COMPLETED, "a"))

    comped.clear()
    orch, snap = await _recover_run(journal_path, spec)

    assert comped == ["a"]
    assert snap.steps["a"].state is StepState.COMPENSATED
    assert snap.status is WorkflowState.COMPENSATED
    assert fold(read_journal(journal_path).records) == snap


# --- resume rollback -------------------------------------------------------------


def _failing_spec(comped):
    async def ok(ctx: StepContext):
        return f"{ctx.step_id}-done"

    async def undo(ctx: StepContext, result: object) -> None:
        comped.append(ctx.step_id)

    async def boom(ctx: StepContext):
        raise PermanentError("stop")

    return WorkflowSpec.of(
        WF,
        [
            Step(id="a", handler=ok, compensate=undo),
            Step(id="b", handler=ok, compensate=undo, depends_on=frozenset({"a"})),
            Step(id="c", handler=boom, compensate=undo, depends_on=frozenset({"b"})),
        ],
    )


async def test_resume_rollback_from_just_after_workflow_compensating(journal_path) -> None:
    comped: list[str] = []
    spec = _failing_spec(comped)

    await _run_fresh(journal_path, spec)
    full = read_journal(journal_path).records
    _truncate_records(journal_path, _lsn_of(full, RecordType.WORKFLOW_COMPENSATING))

    comped.clear()
    orch, snap = await _recover_run(journal_path, spec)

    assert comped == ["b", "a"]  # inverse completion order
    assert snap.status is WorkflowState.COMPENSATED
    assert snap.steps["a"].state is StepState.COMPENSATED
    assert snap.steps["b"].state is StepState.COMPENSATED
    assert fold(read_journal(journal_path).records) == snap


async def test_resume_rollback_re_runs_a_compensator_caught_in_flight(journal_path) -> None:
    comped: list[str] = []
    spec = _failing_spec(comped)

    await _run_fresh(journal_path, spec)
    full = read_journal(journal_path).records
    # Stop right after b's COMPENSATION_STARTED: b's compensator is a zombie.
    _truncate_records(journal_path, _lsn_of(full, RecordType.COMPENSATION_STARTED, "b"))

    comped.clear()
    orch, snap = await _recover_run(journal_path, spec)

    assert comped == ["b", "a"]  # b re-run, then a
    assert snap.status is WorkflowState.COMPENSATED
    records = read_journal(journal_path).records
    assert fold(records) == snap
    # b's replayed compensation reuses its original attempt/key.
    b_starts = [
        r for r in records if r.step_id == "b" and r.type is RecordType.COMPENSATION_STARTED
    ]
    assert {r.attempt for r in b_starts} == {1}


async def test_resume_rollback_preserves_inverse_completion_order(journal_path) -> None:
    comped: list[str] = []
    spec = _failing_spec(comped)

    await _run_fresh(journal_path, spec)
    full = read_journal(journal_path).records
    _truncate_records(journal_path, _lsn_of(full, RecordType.WORKFLOW_COMPENSATING))
    orch, snap = await _recover_run(journal_path, spec)

    records = read_journal(journal_path).records
    completion = {
        r.step_id: r.lsn for r in records if r.type is RecordType.STEP_COMPLETED
    }
    first_comp = {}
    for r in records:
        if r.type is RecordType.COMPENSATION_STARTED and r.step_id not in first_comp:
            first_comp[r.step_id] = r.lsn

    by_completion = sorted(first_comp, key=lambda s: completion[s])
    by_compensation = sorted(first_comp, key=lambda s: first_comp[s], reverse=True)
    assert by_completion == by_compensation


# --- terminal journals -------------------------------------------------------------


async def test_recover_on_a_completed_journal_is_a_no_op(journal_path) -> None:
    async def ok(ctx: StepContext):
        return "ok"

    spec = WorkflowSpec.of(WF, [Step(id="a", handler=ok)])
    await _run_fresh(journal_path, spec)
    before = read_journal(journal_path).records

    orch, snap = await _recover_run(journal_path, spec)

    after = read_journal(journal_path).records
    assert len(after) == len(before)  # nothing appended
    assert snap.status is WorkflowState.COMPLETED
    assert fold(after) == snap


async def test_recover_on_a_dead_letter_journal_is_a_no_op(journal_path) -> None:
    async def charge(ctx: StepContext):
        return {"id": 1}

    async def bad_refund(ctx: StepContext, result: object) -> None:
        raise PermanentError("cannot", status_code=400)

    async def ship(ctx: StepContext):
        raise PermanentError("nope")

    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="charge", handler=charge, compensate=bad_refund),
            Step(id="ship", handler=ship, compensate=bad_refund, depends_on=frozenset({"charge"})),
        ],
    )
    await _run_fresh(journal_path, spec)
    before = read_journal(journal_path).records
    assert fold(before).status is WorkflowState.DEAD_LETTER

    orch, snap = await _recover_run(journal_path, spec)

    after = read_journal(journal_path).records
    assert len(after) == len(before)
    assert snap.status is WorkflowState.DEAD_LETTER
    assert fold(after) == snap
