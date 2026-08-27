"""Phase 5, part 1: the zombie-step resolution ladder.

A step whose ``STEP_STARTED`` is on disk with no terminal record after it is a
zombie candidate: its side effect may or may not have landed. Recovery walks
the ladder from CLAUDE.md 4.1 for each one:

* ``probe()`` declared -> call it with the recorded key.
    * ``FOUND(result)``  -> journal ``STEP_PROBE_RESOLVED`` then
      ``STEP_COMPLETED``; the handler is **not** re-run.
    * ``NOT_FOUND``      -> re-invoke the handler with the *identical* key.
    * ``UNKNOWN`` (or the probe itself failing past its own retries) ->
      journal ``STEP_UNCERTAIN`` and route the step to compensation, or
      straight to ``DEAD_LETTER`` if it has no compensator.
* no ``probe()`` -> replay the handler directly with the identical key.

The load-bearing invariant, asserted here explicitly: the forward attempt
counter never advances on recovery, so the replayed call presents a
byte-identical idempotency key.
"""

from __future__ import annotations

import asyncio

import pytest

from saga.errors import SagaError
from saga.journal import Journal, read_journal
from saga.models import ProbeResult, Step, StepContext, WorkflowSpec
from saga.records import RecordType
from saga.recovery import recover
from saga.replay import fold
from saga.retry import RetryPolicy
from saga.states import StepState, WorkflowState

WF = "wf-zombie"
FAST_PROBE = RetryPolicy(max_attempts=3, base_delay=0.001, budget_s=1.0)


def _seed_zombie(path, step_ids=("a",), running="a", attempt=1):
    """Write a journal that stops right after ``running``'s STEP_STARTED."""
    from saga.idempotency import idempotency_key

    with Journal.open_for_write(path) as journal:
        journal.append(
            type=RecordType.WORKFLOW_STARTED,
            workflow_id=WF,
            payload={"step_ids": list(step_ids)},
        )
        journal.append(
            type=RecordType.STEP_STARTED,
            workflow_id=WF,
            step_id=running,
            attempt=attempt,
            idempotency_key=idempotency_key(WF, running, attempt),
        )


async def _noop_comp(ctx: StepContext, result: object) -> None:
    return None


async def _recover_run(path, spec, **kw):
    orch = recover(path, spec, **kw)
    try:
        snapshot = await orch.run()
    finally:
        orch.close()
    return orch, snapshot


def _types(records, step_id):
    return [r.type for r in records if r.step_id == step_id]


# --- probe FOUND -------------------------------------------------------------


async def test_probe_found_completes_without_rerunning_the_handler(journal_path) -> None:
    _seed_zombie(journal_path)
    ran: list[str] = []

    async def handler(ctx: StepContext):
        ran.append(ctx.idempotency_key)  # pragma: no cover - must not run
        return "fresh"

    async def probe(key: str) -> ProbeResult:
        return ProbeResult.found({"adopted": key})

    spec = WorkflowSpec.of(
        WF, [Step(id="a", handler=handler, compensate=_noop_comp, probe=probe)]
    )

    orch, snap = await _recover_run(journal_path, spec, probe_retry=FAST_PROBE)

    assert ran == []
    assert snap.steps["a"].state is StepState.COMPLETED
    assert snap.steps["a"].result == {"adopted": "wf-zombie:a:1"}
    assert snap.status is WorkflowState.COMPLETED

    records = read_journal(journal_path).records
    assert fold(records) == snap
    assert _types(records, "a") == [
        RecordType.STEP_STARTED,
        RecordType.STEP_UNCERTAIN,
        RecordType.STEP_PROBE_RESOLVED,
        RecordType.STEP_COMPLETED,
    ]
    resolved = next(r for r in records if r.type is RecordType.STEP_PROBE_RESOLVED)
    assert resolved.payload["resolution"] == "FOUND"


# --- probe NOT_FOUND -------------------------------------------------------------


async def test_probe_not_found_replays_handler_with_identical_key(journal_path) -> None:
    _seed_zombie(journal_path)
    ran: list[str] = []

    async def handler(ctx: StepContext):
        ran.append(ctx.idempotency_key)
        return "fresh"

    async def probe(key: str) -> ProbeResult:
        return ProbeResult.not_found()

    spec = WorkflowSpec.of(
        WF, [Step(id="a", handler=handler, compensate=_noop_comp, probe=probe)]
    )

    orch, snap = await _recover_run(journal_path, spec, probe_retry=FAST_PROBE)

    assert ran == ["wf-zombie:a:1"]  # same attempt, same key
    assert snap.steps["a"].state is StepState.COMPLETED
    assert snap.steps["a"].result == "fresh"
    assert snap.steps["a"].attempt == 1
    assert snap.steps["a"].idempotency_key == "wf-zombie:a:1"
    assert snap.status is WorkflowState.COMPLETED

    records = read_journal(journal_path).records
    assert fold(records) == snap
    assert _types(records, "a") == [
        RecordType.STEP_STARTED,
        RecordType.STEP_UNCERTAIN,
        RecordType.STEP_PROBE_RESOLVED,
        RecordType.STEP_STARTED,  # replay -- new start, same attempt number
        RecordType.STEP_COMPLETED,
    ]
    starts = [r for r in records if r.step_id == "a" and r.type is RecordType.STEP_STARTED]
    assert {r.attempt for r in starts} == {1}
    assert {r.idempotency_key for r in starts} == {"wf-zombie:a:1"}


# --- probe UNKNOWN -------------------------------------------------------------


async def test_probe_unknown_routes_step_to_compensation(journal_path) -> None:
    _seed_zombie(journal_path)
    comped: list[object] = []

    async def handler(ctx: StepContext):
        return "fresh"  # pragma: no cover

    async def comp(ctx: StepContext, result: object) -> None:
        comped.append(result)

    async def probe(key: str) -> ProbeResult:
        return ProbeResult.unknown()

    spec = WorkflowSpec.of(WF, [Step(id="a", handler=handler, compensate=comp, probe=probe)])

    orch, snap = await _recover_run(journal_path, spec, probe_retry=FAST_PROBE)

    assert comped == [None]  # never completed forward, so no forward result
    assert snap.steps["a"].state is StepState.COMPENSATED
    assert snap.status is WorkflowState.COMPENSATED

    records = read_journal(journal_path).records
    assert fold(records) == snap
    assert RecordType.STEP_UNCERTAIN in _types(records, "a")
    resolved = next(r for r in records if r.type is RecordType.STEP_PROBE_RESOLVED)
    assert resolved.payload["resolution"] == "UNKNOWN"


async def test_probe_raising_past_its_budget_is_treated_as_unknown(journal_path) -> None:
    _seed_zombie(journal_path)

    async def handler(ctx: StepContext):
        return "fresh"  # pragma: no cover

    async def comp(ctx: StepContext, result: object) -> None:
        return None

    calls = 0

    async def probe(key: str) -> ProbeResult:
        nonlocal calls
        calls += 1
        raise RuntimeError("probe endpoint down")

    spec = WorkflowSpec.of(WF, [Step(id="a", handler=handler, compensate=comp, probe=probe)])

    orch, snap = await _recover_run(
        journal_path, spec, probe_retry=RetryPolicy(max_attempts=3, base_delay=0.001)
    )

    assert calls == 3
    assert snap.steps["a"].state is StepState.COMPENSATED
    assert snap.status is WorkflowState.COMPENSATED


async def test_probe_unknown_with_no_compensator_dead_letters(journal_path) -> None:
    _seed_zombie(journal_path)

    async def handler(ctx: StepContext):
        return "fresh"  # pragma: no cover

    async def probe(key: str) -> ProbeResult:
        return ProbeResult.unknown()

    spec = WorkflowSpec.of(WF, [Step(id="a", handler=handler, probe=probe)])

    orch, snap = await _recover_run(journal_path, spec, probe_retry=FAST_PROBE)

    assert snap.steps["a"].state is StepState.COMPENSATION_FAILED
    assert snap.status is WorkflowState.DEAD_LETTER
    assert orch.manifest is not None
    assert "a" in {o.step_id for o in orch.manifest.orphaned_resources}

    records = read_journal(journal_path).records
    assert fold(records) == snap


# --- no probe declared -------------------------------------------------------------


async def test_no_probe_replays_directly_with_identical_key(journal_path) -> None:
    _seed_zombie(journal_path)
    ran: list[str] = []

    async def handler(ctx: StepContext):
        ran.append(ctx.idempotency_key)
        return "fresh"

    spec = WorkflowSpec.of(WF, [Step(id="a", handler=handler, compensate=_noop_comp)])

    orch, snap = await _recover_run(journal_path, spec)

    assert ran == ["wf-zombie:a:1"]
    assert snap.steps["a"].state is StepState.COMPLETED
    assert snap.status is WorkflowState.COMPLETED

    records = read_journal(journal_path).records
    assert fold(records) == snap
    assert _types(records, "a") == [
        RecordType.STEP_STARTED,
        RecordType.STEP_UNCERTAIN,
        RecordType.STEP_STARTED,
        RecordType.STEP_COMPLETED,
    ]


async def test_downstream_step_runs_after_zombie_is_resolved(journal_path) -> None:
    _seed_zombie(journal_path, step_ids=("a", "b"), running="a")
    order: list[str] = []

    async def handler_a(ctx: StepContext):
        order.append("a")
        return "a-result"

    async def handler_b(ctx: StepContext):
        order.append("b")
        return {"saw": ctx.upstream["a"]}

    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="a", handler=handler_a, compensate=_noop_comp),
            Step(id="b", handler=handler_b, compensate=_noop_comp, depends_on=frozenset({"a"})),
        ],
    )

    orch, snap = await _recover_run(journal_path, spec)

    assert order == ["a", "b"]
    assert snap.steps["b"].result == {"saw": "a-result"}
    assert snap.status is WorkflowState.COMPLETED
    assert fold(read_journal(journal_path).records) == snap


# --- guardrails -------------------------------------------------------------


async def test_recover_rejects_a_spec_for_a_different_workflow(journal_path) -> None:
    _seed_zombie(journal_path)

    async def handler(ctx: StepContext):
        return "x"  # pragma: no cover

    spec = WorkflowSpec.of("some-other-wf", [Step(id="a", handler=handler)])

    with pytest.raises(SagaError):
        recover(journal_path, spec)

    # the single-writer lock the failed recover took must have been released
    assert not journal_path.with_name(journal_path.name + ".lock").exists()
