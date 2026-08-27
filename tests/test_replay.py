"""``replay.fold`` is pure and total: no clocks, no randomness, no I/O, and
either a valid ``WorkflowSnapshot`` or a clean, explicit error for any record
sequence -- including ones no real engine would ever produce, since a
corrupted or hand-edited journal must fail loudly here too.

These tests hand-build ``JournalRecord`` sequences (there is no engine yet to
generate them) against the payload contract documented in ``replay.py``.
"""

from __future__ import annotations

import pytest

from saga.errors import IllegalTransition, JournalCorruption
from saga.models import DeadLetterEntry
from saga.records import JournalRecord, RecordType
from saga.replay import fold
from saga.states import StepState, WorkflowState

WF = "wf-1"


def _rec(
    lsn: int,
    type_: RecordType,
    *,
    epoch: int = 1,
    step_id: str | None = None,
    attempt: int | None = None,
    idempotency_key: str | None = None,
    payload: dict | None = None,
) -> JournalRecord:
    return JournalRecord(
        lsn=lsn,
        epoch=epoch,
        ts=float(lsn),
        type=type_,
        workflow_id=WF,
        step_id=step_id,
        attempt=attempt,
        idempotency_key=idempotency_key,
        payload=payload or {},
    )


def _started(step_ids: list[str], *, lsn: int = 1) -> JournalRecord:
    return _rec(lsn, RecordType.WORKFLOW_STARTED, payload={"step_ids": step_ids})


# --- structural validity ------------------------------------------------------


def test_fold_of_empty_records_raises() -> None:
    with pytest.raises(JournalCorruption):
        fold([])


def test_fold_rejects_a_first_record_that_is_not_workflow_started() -> None:
    records = [_rec(1, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1")]
    with pytest.raises(JournalCorruption):
        fold(records)


def test_fold_rejects_duplicate_workflow_started() -> None:
    records = [_started(["a"]), _rec(2, RecordType.WORKFLOW_STARTED, payload={"step_ids": ["a"]})]
    with pytest.raises(JournalCorruption):
        fold(records)


def test_fold_rejects_an_lsn_gap() -> None:
    records = [_started(["a"]), _rec(3, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1")]
    with pytest.raises(JournalCorruption):
        fold(records)


def test_fold_rejects_epoch_going_backwards() -> None:
    records = [_started(["a"]), _rec(2, RecordType.RECOVERY_STARTED, epoch=0)]
    with pytest.raises(JournalCorruption):
        fold(records)


def test_fold_rejects_a_transition_outside_the_declared_table() -> None:
    # PENDING -> COMPLETED is not legal: there was no STEP_STARTED first.
    records = [_started(["a"]), _rec(2, RecordType.STEP_COMPLETED, step_id="a", payload={"result": 1})]
    with pytest.raises(IllegalTransition):
        fold(records)


# --- seeding from WORKFLOW_STARTED --------------------------------------------


def test_fold_seeds_every_declared_step_as_pending() -> None:
    snapshot = fold([_started(["a", "b", "c"])])
    assert snapshot.workflow_id == WF
    assert snapshot.epoch == 1
    assert snapshot.last_lsn == 1
    assert snapshot.status is WorkflowState.RUNNING
    assert {step_id: rt.state for step_id, rt in snapshot.steps.items()} == {
        "a": StepState.PENDING,
        "b": StepState.PENDING,
        "c": StepState.PENDING,
    }


# --- forward path: diamond DAG ------------------------------------------------


def _diamond_forward_records() -> list[JournalRecord]:
    return [
        _started(["a", "b", "c", "d"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_COMPLETED, step_id="a", payload={"result": 1}),
        _rec(4, RecordType.STEP_STARTED, step_id="b", attempt=1, idempotency_key="wf-1:b:1"),
        _rec(5, RecordType.STEP_STARTED, step_id="c", attempt=1, idempotency_key="wf-1:c:1"),
        _rec(6, RecordType.STEP_COMPLETED, step_id="c", payload={"result": 3}),
        _rec(7, RecordType.STEP_COMPLETED, step_id="b", payload={"result": 2}),
        _rec(8, RecordType.STEP_STARTED, step_id="d", attempt=1, idempotency_key="wf-1:d:1"),
        _rec(9, RecordType.STEP_COMPLETED, step_id="d", payload={"result": 4}),
        _rec(10, RecordType.WORKFLOW_COMPLETED),
    ]


def test_fold_diamond_forward_path_to_completion() -> None:
    snapshot = fold(_diamond_forward_records())
    assert snapshot.status is WorkflowState.COMPLETED
    assert snapshot.completion_order == ["a", "c", "b", "d"]
    assert snapshot.steps["a"].result == 1
    assert snapshot.steps["a"].completion_lsn == 3
    assert snapshot.last_lsn == 10


def test_fold_is_deterministic() -> None:
    records = _diamond_forward_records()
    assert fold(records) == fold(records)


# --- transient failure / retry ------------------------------------------------


def test_fold_transient_failure_then_retry_reaches_completed() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_FAILED, step_id="a", payload={"error": "boom", "terminal": False}),
        _rec(4, RecordType.STEP_STARTED, step_id="a", attempt=2, idempotency_key="wf-1:a:2"),
        _rec(5, RecordType.STEP_COMPLETED, step_id="a", payload={"result": "ok"}),
    ]
    snapshot = fold(records)
    assert snapshot.steps["a"].state is StepState.COMPLETED
    assert snapshot.steps["a"].attempt == 2
    assert snapshot.steps["a"].idempotency_key == "wf-1:a:2"


def test_fold_terminal_failure_records_last_error() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_FAILED, step_id="a", payload={"error": "boom", "terminal": True}),
    ]
    snapshot = fold(records)
    assert snapshot.steps["a"].state is StepState.FAILED
    assert snapshot.steps["a"].last_error == "boom"


# --- zombie resolution ladder --------------------------------------------------


def test_fold_cancel_then_uncertain_then_probe_found() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_CANCELLED, step_id="a"),
        _rec(4, RecordType.STEP_UNCERTAIN, step_id="a", payload={"reason": "cancelled mid-flight"}),
        _rec(5, RecordType.STEP_PROBE_RESOLVED, step_id="a", payload={"resolution": "FOUND"}),
        _rec(6, RecordType.STEP_COMPLETED, step_id="a", payload={"result": "found-it"}),
    ]
    snapshot = fold(records)
    assert snapshot.steps["a"].state is StepState.COMPLETED
    assert snapshot.steps["a"].result == "found-it"
    assert snapshot.steps["a"].uncertain_reason == "FOUND"  # last resolution, kept for audit


def test_fold_uncertain_then_probe_not_found_replays() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_UNCERTAIN, step_id="a", payload={"reason": "crash mid-handler"}),
        _rec(4, RecordType.STEP_PROBE_RESOLVED, step_id="a", payload={"resolution": "NOT_FOUND"}),
        _rec(5, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(6, RecordType.STEP_COMPLETED, step_id="a", payload={"result": "ok"}),
    ]
    snapshot = fold(records)
    assert snapshot.steps["a"].state is StepState.COMPLETED
    assert snapshot.steps["a"].attempt == 1  # same key: replay, not a new attempt


# --- compensation --------------------------------------------------------------


def test_fold_compensation_success() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_COMPLETED, step_id="a", payload={"result": 1}),
        _rec(4, RecordType.WORKFLOW_COMPENSATING),
        _rec(5, RecordType.COMPENSATION_STARTED, step_id="a", attempt=1),
        _rec(6, RecordType.COMPENSATION_COMPLETED, step_id="a"),
        _rec(7, RecordType.WORKFLOW_COMPENSATED),
    ]
    snapshot = fold(records)
    assert snapshot.status is WorkflowState.COMPENSATED
    assert snapshot.steps["a"].state is StepState.COMPENSATED
    assert snapshot.dead_letter == []


def test_fold_compensation_failure_populates_dead_letter() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_COMPLETED, step_id="a", payload={"result": 1}),
        _rec(4, RecordType.WORKFLOW_COMPENSATING),
        _rec(5, RecordType.COMPENSATION_STARTED, step_id="a", attempt=1),
        _rec(6, RecordType.COMPENSATION_FAILED, step_id="a", payload={"error": "HTTP 400", "status_code": 400}),
        _rec(7, RecordType.WORKFLOW_DEAD_LETTER),
    ]
    snapshot = fold(records)
    assert snapshot.status is WorkflowState.DEAD_LETTER
    assert snapshot.steps["a"].state is StepState.COMPENSATION_FAILED
    assert snapshot.dead_letter == [
        DeadLetterEntry(step_id="a", idempotency_key="wf-1:a:1", attempts=1, error="HTTP 400", status_code=400)
    ]


def test_fold_compensation_retries_before_succeeding() -> None:
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_COMPLETED, step_id="a", payload={"result": 1}),
        _rec(4, RecordType.WORKFLOW_COMPENSATING),
        _rec(5, RecordType.COMPENSATION_STARTED, step_id="a", attempt=1),
        _rec(6, RecordType.COMPENSATION_STARTED, step_id="a", attempt=2),  # transient retry, self-loop
        _rec(7, RecordType.COMPENSATION_COMPLETED, step_id="a"),
        _rec(8, RecordType.WORKFLOW_COMPENSATED),
    ]
    snapshot = fold(records)
    assert snapshot.steps["a"].state is StepState.COMPENSATED
    assert snapshot.steps["a"].compensation_attempt == 2


# --- quarantine / skip ----------------------------------------------------------


def test_fold_pending_step_skipped_when_workflow_fails_early() -> None:
    records = [
        _started(["a", "b"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_FAILED, step_id="a", payload={"error": "boom", "terminal": True}),
        _rec(4, RecordType.STEP_SKIPPED, step_id="b", payload={"reason": "workflow failed before this step ran"}),
    ]
    snapshot = fold(records)
    assert snapshot.steps["b"].state is StepState.SKIPPED


def test_fold_completed_step_can_be_skipped_by_quarantine() -> None:
    # COMPLETED -> SKIPPED: an ancestor of a failed compensation that the
    # engine chose not to touch further (section 4.2's dependency-scoped
    # quarantine), distinct from a step that never ran at all.
    records = [
        _started(["a"]),
        _rec(2, RecordType.STEP_STARTED, step_id="a", attempt=1, idempotency_key="wf-1:a:1"),
        _rec(3, RecordType.STEP_COMPLETED, step_id="a", payload={"result": 1}),
        _rec(4, RecordType.STEP_SKIPPED, step_id="a", payload={"reason": "quarantined", "quarantined_by": "b"}),
    ]
    snapshot = fold(records)
    assert snapshot.steps["a"].state is StepState.SKIPPED
