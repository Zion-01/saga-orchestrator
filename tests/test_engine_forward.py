"""Phase 3 exit criterion: a diamond DAG runs its independent branches
concurrently and produces a journal whose fold matches the live in-memory
snapshot exactly.

Forward path only -- no compensation, no cancellation ceremony, no zombie
resolution (phases 4/5). A step that cannot make forward progress (retries
exhausted, times out, or comes back UNCERTAIN) raises ``WorkflowFailed`` out
of ``Orchestrator.run()``; a sibling that was mid-flight when that happens is
left with a dangling ``STEP_STARTED`` and no terminal record, which is
exactly the zombie shape phase 5's recovery ladder is built to resolve --
these tests only assert that shape lands correctly on disk, not that it's
resolved (there is nothing yet to resolve it).
"""

from __future__ import annotations

import asyncio
import time

import pytest

import saga.engine as engine_mod
from saga.errors import PermanentError, TransientError, UncertainOutcome, WorkflowFailed
from saga.engine import Orchestrator
from saga.idempotency import idempotency_key
from saga.journal import Journal, read_journal
from saga.models import Step, StepContext, WorkflowSpec
from saga.records import RecordType
from saga.replay import fold
from saga.retry import RetryPolicy
from saga.states import StepState, WorkflowState

WF = "wf-1"


def _step(step_id, handler, *, depends_on=frozenset(), **kw) -> Step:
    return Step(id=step_id, handler=handler, depends_on=depends_on, **kw)


async def _run(journal_path, spec: WorkflowSpec):
    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        snapshot = await orchestrator.run()
    return orchestrator, snapshot


# --- diamond DAG: concurrency + fold agreement --------------------------------


def _diamond_spec(record_span=None) -> WorkflowSpec:
    async def a(ctx: StepContext):
        return "a-result"

    async def b(ctx: StepContext):
        if record_span is not None:
            start = time.monotonic()
        await asyncio.sleep(0.1)
        if record_span is not None:
            record_span["b"] = (start, time.monotonic())
        return f"b-result:{ctx.upstream['a']}"

    async def c(ctx: StepContext):
        if record_span is not None:
            start = time.monotonic()
        await asyncio.sleep(0.1)
        if record_span is not None:
            record_span["c"] = (start, time.monotonic())
        return f"c-result:{ctx.upstream['a']}"

    async def d(ctx: StepContext):
        return f"d-result:{ctx.upstream['b']}:{ctx.upstream['c']}"

    return WorkflowSpec.of(
        WF,
        [
            _step("a", a),
            _step("b", b, depends_on=frozenset({"a"})),
            _step("c", c, depends_on=frozenset({"a"})),
            _step("d", d, depends_on=frozenset({"b", "c"})),
        ],
    )


async def test_diamond_dag_completes_with_correct_results(journal_path) -> None:
    _, snapshot = await _run(journal_path, _diamond_spec())
    assert snapshot.status is WorkflowState.COMPLETED
    assert snapshot.steps["a"].result == "a-result"
    assert snapshot.steps["b"].result == "b-result:a-result"
    assert snapshot.steps["c"].result == "c-result:a-result"
    assert snapshot.steps["d"].result == "d-result:b-result:a-result:c-result:a-result"
    assert snapshot.completion_order[0] == "a"
    assert snapshot.completion_order[-1] == "d"
    assert set(snapshot.completion_order[1:3]) == {"b", "c"}


async def test_diamond_independent_branches_run_concurrently(journal_path) -> None:
    spans: dict[str, tuple[float, float]] = {}
    started = time.monotonic()
    await _run(journal_path, _diamond_spec(record_span=spans))
    elapsed = time.monotonic() - started

    # b and c each sleep 0.1s; run serially that's >=0.2s, concurrently ~0.1s.
    # The overlap check below is the real proof of concurrency; this bound
    # just needs enough headroom over fsync/scheduler jitter to stay well
    # under the serial time without being tight enough to flake.
    assert elapsed < 0.18
    b_start, b_end = spans["b"]
    c_start, c_end = spans["c"]
    assert b_start < c_end and c_start < b_end  # the two sleeps overlap in time


async def test_live_snapshot_matches_fold_of_the_journal_exactly(journal_path) -> None:
    _, snapshot = await _run(journal_path, _diamond_spec())
    records = read_journal(journal_path).records
    assert fold(records) == snapshot


# --- determinism: spawn order ---------------------------------------------------


async def test_independent_roots_spawn_in_sorted_step_id_order(journal_path) -> None:
    async def noop(ctx: StepContext):
        return None

    spec = WorkflowSpec.of(WF, [_step("z", noop), _step("a", noop), _step("m", noop)])
    await _run(journal_path, spec)

    records = read_journal(journal_path).records
    started = [r for r in records if r.type is RecordType.STEP_STARTED]
    assert [r.step_id for r in started] == ["a", "m", "z"]


# --- idempotency keys ------------------------------------------------------------


async def test_idempotency_keys_follow_workflow_step_attempt(journal_path) -> None:
    async def noop(ctx: StepContext):
        return ctx.idempotency_key

    spec = WorkflowSpec.of(WF, [_step("a", noop)])
    _, snapshot = await _run(journal_path, spec)
    assert snapshot.steps["a"].idempotency_key == idempotency_key(WF, "a", 1)
    assert snapshot.steps["a"].result == idempotency_key(WF, "a", 1)


# --- retries ----------------------------------------------------------------------


async def test_transient_failure_retries_then_succeeds(journal_path, monkeypatch) -> None:
    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def fast_sleep(delay: float) -> None:
        sleeps.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(engine_mod.asyncio, "sleep", fast_sleep)

    attempts: list[int] = []

    async def flaky(ctx: StepContext):
        attempts.append(ctx.attempt)
        if ctx.attempt < 3:
            raise TransientError("not yet")
        return "ok"

    spec = WorkflowSpec.of(WF, [_step("a", flaky, retry=RetryPolicy(max_attempts=5, base_delay=0.01))])
    _, snapshot = await _run(journal_path, spec)

    assert attempts == [1, 2, 3]
    assert snapshot.steps["a"].state is StepState.COMPLETED
    assert snapshot.steps["a"].attempt == 3
    assert snapshot.steps["a"].result == "ok"
    assert sleeps == [
        spec.steps["a"].retry.delay_for(1),
        spec.steps["a"].retry.delay_for(2),
    ]

    records = read_journal(journal_path).records
    failed = [r for r in records if r.type is RecordType.STEP_FAILED]
    assert [r.payload["terminal"] for r in failed] == [False, False]


async def test_transient_failure_exhausting_budget_raises_workflow_failed(journal_path) -> None:
    async def always_fails(ctx: StepContext):
        raise TransientError("nope")

    spec = WorkflowSpec.of(WF, [_step("a", always_fails, retry=RetryPolicy(max_attempts=2, base_delay=0.001))])

    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        with pytest.raises(WorkflowFailed) as exc_info:
            await orchestrator.run()
    assert exc_info.value.step_id == "a"
    assert orchestrator.snapshot.steps["a"].state is StepState.FAILED

    records = read_journal(journal_path).records
    failed = [r for r in records if r.type is RecordType.STEP_FAILED]
    assert [r.payload["terminal"] for r in failed] == [False, True]
    assert fold(records) == orchestrator.snapshot


async def test_permanent_failure_does_not_retry(journal_path) -> None:
    attempts: list[int] = []

    async def bad_request(ctx: StepContext):
        attempts.append(ctx.attempt)
        raise PermanentError("HTTP 400", status_code=400)

    spec = WorkflowSpec.of(WF, [_step("a", bad_request)])

    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        with pytest.raises(WorkflowFailed):
            await orchestrator.run()

    assert attempts == [1]
    assert orchestrator.snapshot.steps["a"].state is StepState.FAILED


# --- uncertain outcomes -------------------------------------------------------------


async def test_uncertain_outcome_journals_step_uncertain(journal_path) -> None:
    async def maybe_charged(ctx: StepContext):
        raise UncertainOutcome("connection dropped mid-request")

    spec = WorkflowSpec.of(WF, [_step("a", maybe_charged)])

    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        with pytest.raises(WorkflowFailed):
            await orchestrator.run()

    records = read_journal(journal_path).records
    a_types = [r.type for r in records if r.step_id == "a"]
    assert a_types[:2] == [RecordType.STEP_STARTED, RecordType.STEP_UNCERTAIN]
    uncertain = next(r for r in records if r.step_id == "a" and r.type is RecordType.STEP_UNCERTAIN)
    assert uncertain.payload["reason"] == "connection dropped mid-request"
    # Phase 4: an UNCERTAIN step with no compensator and no probe cannot be
    # resolved -> DEAD_LETTER rather than being left dangling.
    assert orchestrator.snapshot.steps["a"].state is StepState.COMPENSATION_FAILED
    assert orchestrator.snapshot.status is WorkflowState.DEAD_LETTER
    assert fold(records) == orchestrator.snapshot


async def test_engine_timeout_is_treated_as_uncertain_not_retried(journal_path) -> None:
    async def slow(ctx: StepContext):
        await asyncio.sleep(10)

    spec = WorkflowSpec.of(WF, [_step("a", slow, timeout_s=0.02)])

    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        with pytest.raises(WorkflowFailed):
            await orchestrator.run()

    assert "0.02" in orchestrator.snapshot.steps["a"].uncertain_reason
    records = read_journal(journal_path).records
    a_types = [r.type for r in records if r.step_id == "a"]
    assert a_types[:2] == [RecordType.STEP_STARTED, RecordType.STEP_UNCERTAIN]
    assert RecordType.STEP_FAILED not in a_types  # a timeout is never retried
    assert orchestrator.snapshot.status is WorkflowState.DEAD_LETTER


async def test_handler_raised_timeout_error_is_uncertain_even_without_timeout_s(journal_path) -> None:
    async def raises_timeout(ctx: StepContext):
        raise TimeoutError("downstream was slow")

    spec = WorkflowSpec.of(WF, [_step("a", raises_timeout)])  # no timeout_s configured

    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        with pytest.raises(WorkflowFailed):
            await orchestrator.run()

    assert orchestrator.snapshot.steps["a"].uncertain_reason == "downstream was slow"
    records = read_journal(journal_path).records
    a_types = [r.type for r in records if r.step_id == "a"]
    assert a_types[:2] == [RecordType.STEP_STARTED, RecordType.STEP_UNCERTAIN]


# --- a failed sibling cancels an in-flight one -----------------------------------


async def test_sibling_failure_cancels_in_flight_step_and_journals_the_cancel(journal_path) -> None:
    async def fails_fast(ctx: StepContext):
        raise PermanentError("boom")

    started = asyncio.Event()

    async def slow_and_uninterrupted(ctx: StepContext):
        started.set()
        await asyncio.sleep(10)
        return "never"  # pragma: no cover - cancelled before this runs

    spec = WorkflowSpec.of(WF, [_step("fails", fails_fast), _step("slow", slow_and_uninterrupted)])

    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        with pytest.raises(WorkflowFailed):
            await orchestrator.run()

    assert started.is_set()
    records = read_journal(journal_path).records
    slow_types = [r.type for r in records if r.step_id == "slow"]
    # Phase 4: the cancellation is journaled, and the step -- possibly a zombie
    # -- is promoted to UNCERTAIN. With no compensator it dead-letters.
    assert slow_types[:3] == [
        RecordType.STEP_STARTED,
        RecordType.STEP_CANCELLED,
        RecordType.STEP_UNCERTAIN,
    ]
    assert orchestrator.snapshot.status is WorkflowState.DEAD_LETTER
    assert fold(records) == orchestrator.snapshot
