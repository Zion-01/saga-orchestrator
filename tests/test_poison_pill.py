"""Phase 4, part 3: poison-pill compensations.

A compensator that cannot succeed must terminate the workflow in bounded time
-- never an unbounded retry loop -- land it in DEAD_LETTER, quarantine the
ancestors of the failed step (rolling a parent back under a child whose
rollback failed can be genuinely unsafe) while letting independent branches
finish unwinding, and leave an InterventionManifest naming every orphaned
resource.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from saga.engine import Orchestrator
from saga.errors import PermanentError, TransientError, WorkflowFailed
from saga.journal import Journal, read_journal
from saga.manifest import manifest_path
from saga.models import Step, StepContext, WorkflowSpec
from saga.records import RecordType
from saga.replay import fold
from saga.retry import RetryPolicy
from saga.states import StepState, WorkflowState

WF = "wf-1"


async def _run(journal_path, spec, **kw):
    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal, **kw)
        with pytest.raises(WorkflowFailed) as exc_info:
            await orchestrator.run()
    return orchestrator, exc_info.value


def _mk(name, *, result=None, deps=frozenset(), fail_fwd=False, comp=None):
    async def handler(ctx: StepContext):
        await asyncio.sleep(0)
        if fail_fwd:
            raise PermanentError("forward boom")
        return result if result is not None else name

    return Step(id=name, handler=handler, compensate=comp, depends_on=deps)


# --- permanent compensation failure -> DEAD_LETTER + quarantine ---------------


async def test_permanent_compensation_failure_dead_letters_and_quarantines_ancestor(journal_path) -> None:
    async def b_comp(ctx: StepContext, result) -> None:
        raise PermanentError("compensator got HTTP 400", status_code=400)

    spec = WorkflowSpec.of(
        WF,
        [
            _mk("a", result={"charge_id": "ch_1"}, comp=_noop_comp()),
            _mk("b", result={"booking_id": "bk_9"}, deps=frozenset({"a"}), comp=b_comp),
            _mk("c", deps=frozenset({"b"}), fail_fwd=True),
        ],
    )

    orch, _ = await _run(journal_path, spec)

    snap = orch.snapshot
    assert snap.status is WorkflowState.DEAD_LETTER
    assert snap.steps["b"].state is StepState.COMPENSATION_FAILED
    assert snap.steps["a"].state is StepState.SKIPPED  # quarantined: ancestor of b

    records = read_journal(journal_path).records
    a_skip = next(r for r in records if r.step_id == "a" and r.type is RecordType.STEP_SKIPPED)
    assert a_skip.payload["quarantined_by"] == "b"
    assert records[-1].type is RecordType.WORKFLOW_DEAD_LETTER
    assert fold(records) == snap

    assert snap.dead_letter and snap.dead_letter[0].step_id == "b"
    assert snap.dead_letter[0].status_code == 400


def _noop_comp():
    async def comp(ctx: StepContext, result) -> None:
        return None

    return comp


# --- bounded retry on a transient poison pill --------------------------------


async def test_transient_compensation_failure_retries_to_its_budget_then_dead_letters(journal_path) -> None:
    attempts: list[int] = []

    async def b_comp(ctx: StepContext, result) -> None:
        attempts.append(ctx.attempt)
        raise TransientError("still down")

    spec = WorkflowSpec.of(
        WF,
        [
            _mk("a", deps=frozenset()),
            _mk(
                "b",
                deps=frozenset({"a"}),
                comp=b_comp,
            ),
            _mk("c", deps=frozenset({"b"}), fail_fwd=True),
        ],
    )
    # override b's compensation budget to keep the test quick but exercise > 1 attempt
    b = spec.steps["b"]
    b2 = Step(
        id="b",
        handler=b.handler,
        compensate=b_comp,
        depends_on=frozenset({"a"}),
        compensation_retry=RetryPolicy(max_attempts=3, base_delay=0.001),
    )
    spec = WorkflowSpec.of(WF, [spec.steps["a"], b2, spec.steps["c"]])

    orch, _ = await _run(journal_path, spec)

    assert attempts == [1, 2, 3]
    records = read_journal(journal_path).records
    started = [r for r in records if r.step_id == "b" and r.type is RecordType.COMPENSATION_STARTED]
    assert [r.attempt for r in started] == [1, 2, 3]
    assert any(r.type is RecordType.COMPENSATION_FAILED for r in records if r.step_id == "b")
    assert orch.snapshot.status is WorkflowState.DEAD_LETTER
    assert fold(records) == orch.snapshot


# --- independent branch still rolls back during a quarantine ----------------


async def test_independent_branch_still_compensates_while_ancestors_are_quarantined(journal_path) -> None:
    comped: list[str] = []

    def ok_comp(name):
        async def comp(ctx: StepContext, result) -> None:
            comped.append(name)

        return comp

    async def d_comp(ctx: StepContext, result) -> None:
        raise PermanentError("poison")

    # a -> b -> d ; a -> c ; d -> e(fails forward)
    spec = WorkflowSpec.of(
        WF,
        [
            _mk("a", comp=ok_comp("a")),
            _mk("b", deps=frozenset({"a"}), comp=ok_comp("b")),
            _mk("c", deps=frozenset({"a"}), comp=ok_comp("c")),
            _mk("d", deps=frozenset({"b"}), comp=d_comp),
            _mk("e", deps=frozenset({"d"}), fail_fwd=True),
        ],
    )

    orch, _ = await _run(journal_path, spec)

    snap = orch.snapshot
    assert snap.status is WorkflowState.DEAD_LETTER
    assert snap.steps["d"].state is StepState.COMPENSATION_FAILED
    assert snap.steps["c"].state is StepState.COMPENSATED  # independent of d
    assert snap.steps["b"].state is StepState.SKIPPED  # ancestor of d
    assert snap.steps["a"].state is StepState.SKIPPED  # ancestor of d
    assert comped == ["c"]

    records = read_journal(journal_path).records
    quarantined_by = {
        r.step_id: r.payload["quarantined_by"]
        for r in records
        if r.type is RecordType.STEP_SKIPPED
    }
    assert quarantined_by == {"a": "d", "b": "d"}
    assert fold(records) == snap


# --- the manifest ----------------------------------------------------------


async def test_intervention_manifest_is_written_next_to_the_journal(journal_path) -> None:
    async def b_comp(ctx: StepContext, result) -> None:
        raise PermanentError("cannot cancel booking", status_code=409)

    spec = WorkflowSpec.of(
        WF,
        [
            _mk("a", result={"charge_id": "ch_1"}, comp=_noop_comp()),
            _mk("b", result={"booking_id": "bk_9"}, deps=frozenset({"a"}), comp=b_comp),
            _mk("c", deps=frozenset({"b"}), fail_fwd=True),
        ],
    )

    orch, _ = await _run(journal_path, spec)

    path = manifest_path(journal_path)
    assert path.exists()
    doc = json.loads(path.read_text())

    assert doc["workflow_id"] == WF
    assert doc["epoch"] == orch.snapshot.epoch
    assert doc["lsn_range"][0] >= 1 and doc["lsn_range"][1] >= doc["lsn_range"][0]
    assert "saga inspect" in doc["inspect_command"]

    failed = {fc["step_id"]: fc for fc in doc["failed_compensations"]}
    assert failed["b"]["status_code"] == 409
    assert failed["b"]["attempts"] >= 1

    quarantined = {q["step_id"]: q["quarantined_by"] for q in doc["quarantined"]}
    assert quarantined == {"a": "b"}

    orphans = {o["step_id"]: o for o in doc["orphaned_resources"]}
    assert orphans["b"]["result"] == {"booking_id": "bk_9"}
    assert orphans["a"]["result"] == {"charge_id": "ch_1"}

    # the DEAD_LETTER record points at the manifest
    records = read_journal(journal_path).records
    dead = next(r for r in records if r.type is RecordType.WORKFLOW_DEAD_LETTER)
    assert dead.payload["manifest"] == path.name


async def test_uncertain_step_without_a_compensator_dead_letters(journal_path) -> None:
    started = asyncio.Event()

    async def slow(ctx: StepContext):
        started.set()
        await asyncio.sleep(30)
        return "never"  # pragma: no cover

    async def boom(ctx: StepContext):
        await started.wait()
        raise PermanentError("boom")

    spec = WorkflowSpec.of(
        WF,
        [Step(id="slow", handler=slow), Step(id="boom", handler=boom)],  # slow has no compensator
    )

    orch, _ = await _run(journal_path, spec)

    assert orch.snapshot.status is WorkflowState.DEAD_LETTER
    assert orch.snapshot.steps["slow"].state is StepState.COMPENSATION_FAILED
    path = manifest_path(journal_path)
    doc = json.loads(path.read_text())
    assert any(fc["step_id"] == "slow" for fc in doc["failed_compensations"])
    records = read_journal(journal_path).records
    assert fold(records) == orch.snapshot


async def test_dead_letter_is_reached_in_bounded_time(journal_path) -> None:
    async def b_comp(ctx: StepContext, result) -> None:
        raise TransientError("never recovers")

    spec = WorkflowSpec.of(
        WF,
        [
            _mk("a"),
            Step(
                id="b",
                handler=_mk("b").handler,
                compensate=b_comp,
                depends_on=frozenset({"a"}),
                compensation_retry=RetryPolicy(max_attempts=3, base_delay=0.001, budget_s=0.5),
            ),
            _mk("c", deps=frozenset({"b"}), fail_fwd=True),
        ],
    )

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    orch, _ = await _run(journal_path, spec)
    assert loop.time() - t0 < 5.0
    assert orch.snapshot.status is WorkflowState.DEAD_LETTER
