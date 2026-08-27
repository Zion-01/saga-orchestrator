"""``recover(journal_file, spec)``: turn a journal left by a dead process back
into a running workflow.

Recovery is deliberately thin, because the hard part is already done elsewhere.
:func:`saga.replay.fold` is pure and total, so folding the journal to a
:class:`~saga.models.WorkflowSnapshot` is the whole of "figure out what
happened". This module only has to:

1. open the journal for writing -- which truncates any torn final line and
   bumps the epoch (a fresh incarnation counter);
2. fold the clean prefix to a snapshot;
3. check the caller's :class:`~saga.models.WorkflowSpec` actually describes
   *this* workflow (same id, same step ids) -- handlers, compensators and
   probes are code and can never come from the journal;
4. hand back an :class:`~saga.engine.Orchestrator` primed with that snapshot.

Calling ``await orchestrator.run()`` then resumes: it appends
``RECOVERY_STARTED``, walks the zombie ladder (CLAUDE.md 4.1) for every step
that was in flight, and drives the workflow to a terminal state -- resuming
the forward DAG or resuming rollback as the snapshot dictates. Unlike a fresh
``run()`` it does not raise :class:`~saga.errors.WorkflowFailed`; the returned
snapshot's ``status`` is the outcome.

The returned orchestrator owns the journal it opened. Close it when done::

    orch = recover(path, spec)
    try:
        snapshot = await orch.run()
    finally:
        orch.close()
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from .engine import Orchestrator
from .errors import JournalCorruption, JournalLocked, SpecMismatch
from .journal import Journal, read_journal
from .lockfile import break_lock as _break_lock
from .models import WorkflowSpec
from .records import RecordType
from .replay import fold
from .retry import DEFAULT_PROBE_RETRY, Disposition, RetryPolicy, classify as _default_classify


def recover(
    journal_file: Path | str,
    spec: WorkflowSpec,
    *,
    cancel_grace_s: float = 5.0,
    parallel_compensation: bool = False,
    classifier: Callable[[BaseException], Disposition] = _default_classify,
    probe_retry: RetryPolicy = DEFAULT_PROBE_RETRY,
    break_stale_lock: bool = True,
) -> Orchestrator:
    """Open ``journal_file``, fold it, and return a resumable orchestrator for
    ``spec``. Raises :class:`~saga.errors.JournalCorruption` if the journal is
    internally inconsistent and :class:`~saga.errors.SpecMismatch` if ``spec``
    is for a different workflow than the one on disk.

    A process that was killed mid-write leaves its single-writer lock behind.
    Recovery is by definition the moment an operator has decided that process
    is gone, so by default ``recover`` clears that stale lock and takes over.
    Pass ``break_stale_lock=False`` to instead surface
    :class:`~saga.errors.JournalLocked` -- do that if there is any chance the
    original writer is still alive.
    """
    path = Path(journal_file)
    if not path.exists():
        raise JournalCorruption(f"no journal to recover at {path}")

    try:
        journal = Journal.open_for_write(path)  # truncates torn tail, bumps epoch, locks
    except JournalLocked:
        if not break_stale_lock:
            raise
        _break_lock(path)
        journal = Journal.open_for_write(path)
    try:
        result = read_journal(path)
        snapshot = fold(result.records)

        if snapshot.workflow_id != spec.workflow_id:
            raise SpecMismatch(
                f"journal is workflow {snapshot.workflow_id!r}, "
                f"spec is {spec.workflow_id!r}"
            )
        journal_ids = set(snapshot.steps)
        spec_ids = set(spec.steps)
        if journal_ids != spec_ids:
            raise SpecMismatch(
                f"step ids differ: journal has {sorted(journal_ids)}, "
                f"spec has {sorted(spec_ids)}"
            )

        incident_lsn = next(
            (r.lsn for r in result.records if r.type is RecordType.WORKFLOW_COMPENSATING),
            None,
        )
        orchestrator = Orchestrator(
            spec,
            journal,
            cancel_grace_s=cancel_grace_s,
            parallel_compensation=parallel_compensation,
            classifier=classifier,
            probe_retry=probe_retry,
            _resume_snapshot=snapshot,
            _resume_incident_lsn=incident_lsn,
        )
        orchestrator._owns_journal = True
        return orchestrator
    except BaseException:
        journal.close()
        raise
