"""``fold(records) -> WorkflowSnapshot``: the pure, total heart of recovery.

No clocks, no randomness, no I/O -- the same byte stream always folds to the
identical snapshot. That is what makes this the single source of truth for
the live engine, the CLI inspector, and the crash-injection sweep alike.

``fold`` is built from two smaller pieces that are public in their own
right, because :mod:`saga.engine` needs them too: :func:`workflow_started`
builds the initial snapshot from a journal's first record, and :func:`apply`
folds one subsequent record into an existing snapshot. The live engine calls
these same two functions -- after every ``journal.append`` and *before*
running any handler, per the write-ahead rule -- to maintain its in-memory
snapshot. Sharing the exact function is what guarantees the engine's live
snapshot and ``fold(all_records)`` can never drift apart; it is not two
implementations kept in sync by hand.

Every record type maps to at most one state-machine mutation, per the
payload contract documented in :mod:`saga.records`. Two kinds of "clean,
explicit error" can come out of a bad record stream, and they are
deliberately different exceptions: a framing problem the journal itself
should never have allowed through (an LSN gap, an epoch that goes backwards,
a stream that doesn't open with ``WORKFLOW_STARTED``) raises
:class:`JournalCorruption`; a structurally well-formed record that demands
an illegal state-machine move (e.g. a ``STEP_COMPLETED`` with no prior
``STEP_STARTED``) raises :class:`IllegalTransition` from the transition
tables in :mod:`saga.states`. Both are :class:`SagaError`, so a caller that
just wants "is this journal trustworthy" can catch broadly.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from .errors import JournalCorruption
from .models import DeadLetterEntry, StepRuntime, WorkflowSnapshot
from .records import JournalRecord, RecordType
from .states import StepState, WorkflowState


def fold(records: Iterable[JournalRecord]) -> WorkflowSnapshot:
    """Fold a record stream into a :class:`WorkflowSnapshot`.

    Accepts any prefix of a real journal, including one truncated mid-step --
    the result is simply a snapshot with those steps still ``PENDING`` or
    ``RUNNING``. What it does not accept is a stream that is internally
    inconsistent; see the module docstring for how that's reported.
    """
    snapshot: WorkflowSnapshot | None = None

    for record in records:
        if snapshot is None:
            if record.type is not RecordType.WORKFLOW_STARTED:
                raise JournalCorruption(
                    f"journal must open with WORKFLOW_STARTED, got {record.type} at lsn={record.lsn}"
                )
            if record.lsn != 1:
                raise JournalCorruption(f"WORKFLOW_STARTED must be lsn=1, got {record.lsn}")
            snapshot = workflow_started(record)
            continue
        apply(snapshot, record)

    if snapshot is None:
        raise JournalCorruption("empty journal: no WORKFLOW_STARTED record")
    return snapshot


def apply(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    """Fold one non-``WORKFLOW_STARTED`` record into an existing snapshot,
    in place. ``record`` must be the immediate successor of whatever
    ``snapshot.last_lsn`` currently is.
    """
    if record.type is RecordType.WORKFLOW_STARTED:
        raise JournalCorruption(f"WORKFLOW_STARTED may only be the first record, got another at lsn={record.lsn}")
    expected_lsn = snapshot.last_lsn + 1
    if record.lsn != expected_lsn:
        raise JournalCorruption(f"lsn gap: expected {expected_lsn}, got {record.lsn}")
    if record.epoch < snapshot.epoch:
        raise JournalCorruption(f"epoch went backwards: {snapshot.epoch} -> {record.epoch} at lsn={record.lsn}")
    snapshot.epoch = record.epoch
    snapshot.last_lsn = record.lsn
    _HANDLERS[record.type](snapshot, record)


def workflow_started(record: JournalRecord) -> WorkflowSnapshot:
    """Build the initial snapshot from a journal's first record."""
    step_ids = record.payload.get("step_ids")
    if not isinstance(step_ids, list) or not all(isinstance(s, str) for s in step_ids):
        raise JournalCorruption(
            f"WORKFLOW_STARTED payload must carry a step_ids list of str, got {record.payload!r}"
        )
    snapshot = WorkflowSnapshot(
        workflow_id=record.workflow_id,
        epoch=record.epoch,
        last_lsn=record.lsn,
        steps={step_id: StepRuntime(step_id) for step_id in step_ids},
    )
    snapshot.transition_workflow(WorkflowState.RUNNING)
    return snapshot


def _step_started(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.RUNNING)
    runtime = snapshot.steps[record.step_id]
    if record.attempt is not None:
        runtime.attempt = record.attempt
    runtime.idempotency_key = record.idempotency_key


def _step_completed(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.COMPLETED)
    runtime = snapshot.steps[record.step_id]
    runtime.result = record.payload.get("result")
    runtime.completion_lsn = record.lsn
    snapshot.completion_order.append(record.step_id)


def _step_failed(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    terminal = bool(record.payload.get("terminal", True))
    snapshot.transition(record.step_id, StepState.FAILED if terminal else StepState.RETRYING)
    snapshot.steps[record.step_id].last_error = record.payload.get("error")


def _step_cancelled(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.CANCELLED)


def _step_uncertain(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.UNCERTAIN)
    snapshot.steps[record.step_id].uncertain_reason = record.payload.get("reason")


def _step_probe_resolved(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    # Informational only: UNCERTAIN has no legal self-loop, and the
    # transition this resolution leads to (RUNNING / COMPLETED /
    # COMPENSATING / ...) is always journaled as its own following record.
    runtime = snapshot.steps[record.step_id]
    runtime.uncertain_reason = record.payload.get("resolution", runtime.uncertain_reason)


def _step_skipped(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.SKIPPED)


def _workflow_compensating(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition_workflow(WorkflowState.COMPENSATING)


def _compensation_started(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.COMPENSATING)
    if record.attempt is not None:
        snapshot.steps[record.step_id].compensation_attempt = record.attempt


def _compensation_completed(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.COMPENSATED)


def _compensation_failed(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition(record.step_id, StepState.COMPENSATION_FAILED)
    runtime = snapshot.steps[record.step_id]
    runtime.last_error = record.payload.get("error")
    snapshot.dead_letter.append(
        DeadLetterEntry(
            step_id=record.step_id,
            idempotency_key=runtime.idempotency_key,
            attempts=runtime.compensation_attempt,
            error=record.payload.get("error", ""),
            status_code=record.payload.get("status_code"),
        )
    )


def _workflow_completed(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition_workflow(WorkflowState.COMPLETED)


def _workflow_compensated(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition_workflow(WorkflowState.COMPENSATED)


def _workflow_dead_letter(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    snapshot.transition_workflow(WorkflowState.DEAD_LETTER)


def _recovery_started(snapshot: WorkflowSnapshot, record: JournalRecord) -> None:
    pass  # the epoch bump itself is already applied uniformly in fold()


_HANDLERS: dict[RecordType, Callable[[WorkflowSnapshot, JournalRecord], None]] = {
    RecordType.RECOVERY_STARTED: _recovery_started,
    RecordType.STEP_STARTED: _step_started,
    RecordType.STEP_COMPLETED: _step_completed,
    RecordType.STEP_FAILED: _step_failed,
    RecordType.STEP_CANCELLED: _step_cancelled,
    RecordType.STEP_UNCERTAIN: _step_uncertain,
    RecordType.STEP_PROBE_RESOLVED: _step_probe_resolved,
    RecordType.STEP_SKIPPED: _step_skipped,
    RecordType.WORKFLOW_COMPENSATING: _workflow_compensating,
    RecordType.COMPENSATION_STARTED: _compensation_started,
    RecordType.COMPENSATION_COMPLETED: _compensation_completed,
    RecordType.COMPENSATION_FAILED: _compensation_failed,
    RecordType.WORKFLOW_COMPLETED: _workflow_completed,
    RecordType.WORKFLOW_COMPENSATED: _workflow_compensated,
    RecordType.WORKFLOW_DEAD_LETTER: _workflow_dead_letter,
}
