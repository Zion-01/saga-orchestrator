"""The forward engine: concurrent, dependency-ordered, WAL-first step execution.

Phase 3 scope only -- no compensation, no cancellation ceremony, no zombie
resolution. Those are phases 4 and 5. What this module guarantees:

- One ``asyncio`` task per step, spawned inside a single ``asyncio.TaskGroup``
  in ``sorted(step_id)`` order (CLAUDE.md's determinism rule for the ready
  set), each waiting on its dependencies' completion events before running.
- Every state transition is journaled and fsync'd *before* it is applied to
  the in-memory snapshot, and the snapshot is mutated *before* any handler
  runs: ``journal.append() -> write -> fsync -> replay.apply() -> handler``.
- The live snapshot is mutated through the exact same :func:`replay.apply`
  / :func:`replay.workflow_started` functions :func:`replay.fold` uses, so
  the live snapshot and ``fold(all_records)`` can never drift apart -- see
  ``replay.py``'s module docstring.
- Retries follow ``Step.retry`` (attempt count + wall-clock budget, no
  jitter, from :mod:`saga.retry`).

A step's own ``timeout_s``, if set, is enforced with ``asyncio.wait_for``.
Any ``TimeoutError`` reaching a step wrapper -- whether from the engine's own
``wait_for`` giving up or from the handler raising one itself -- is treated
as UNCERTAIN, never as a retryable failure: a timeout means the engine gave
up waiting, not that the side effect didn't happen, and retrying blindly is
exactly the double-execution CLAUDE.md's zombie ladder exists to prevent.
This is a deliberate, narrower rule than ``retry.classify``'s general
TRANSIENT default for ``TimeoutError`` -- it applies only at the point a step
handler is invoked, not to ``classify`` itself.

A step that cannot make forward progress (retries exhausted, timed out, or
explicitly ``UncertainOutcome``) raises ``StepFailure`` to unwind the
``TaskGroup``, which natively cancels every sibling task. A sibling still
waiting on a dependency event is cancelled harmlessly -- it never journaled
``STEP_STARTED``, so nothing happened for it. A sibling actively mid-handler
is cancelled too, uncushioned by any grace period (phase 4 adds that): its
``STEP_STARTED`` is already durably on disk with no terminal record, a
zombie by construction, exactly the shape phase 5's recovery ladder expects.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from . import replay
from .errors import StepFailure, WorkflowFailed
from .idempotency import idempotency_key
from .journal import Journal
from .models import Step, StepContext, WorkflowSnapshot, WorkflowSpec
from .records import RecordType
from .retry import Disposition, classify


class Orchestrator:
    """Runs one :class:`WorkflowSpec` forward against an already-open
    :class:`~saga.journal.Journal`. One-shot: call :meth:`run` once.
    """

    def __init__(self, spec: WorkflowSpec, journal: Journal) -> None:
        self._spec = spec
        self._journal = journal
        self.workflow_id = spec.workflow_id
        self._snapshot: WorkflowSnapshot | None = None
        self._completion_events: dict[str, asyncio.Event] = {}

    @property
    def snapshot(self) -> WorkflowSnapshot:
        """The live snapshot. Available as soon as ``run()`` has journaled
        ``WORKFLOW_STARTED`` -- including after ``run()`` raises, so a
        caller can inspect exactly how far forward progress got.
        """
        if self._snapshot is None:
            raise RuntimeError("Orchestrator.run() has not been awaited yet")
        return self._snapshot

    async def run(self) -> WorkflowSnapshot:
        step_ids = sorted(self._spec.steps)
        record = self._journal.append(
            type=RecordType.WORKFLOW_STARTED,
            workflow_id=self.workflow_id,
            payload={"step_ids": step_ids},
        )
        self._snapshot = replay.workflow_started(record)
        self._completion_events = {step_id: asyncio.Event() for step_id in step_ids}

        try:
            async with asyncio.TaskGroup() as tg:
                for step_id in step_ids:
                    tg.create_task(self._run_step(self._spec.steps[step_id]), name=step_id)
        except* StepFailure as eg:
            failure = eg.exceptions[0]
            raise WorkflowFailed(failure.step_id, failure.cause) from failure

        record = self._journal.append(type=RecordType.WORKFLOW_COMPLETED, workflow_id=self.workflow_id)
        replay.apply(self.snapshot, record)
        return self.snapshot

    async def _run_step(self, step: Step) -> None:
        for dep_id in sorted(step.depends_on):
            await self._completion_events[dep_id].wait()

        attempt = 0
        start = time.monotonic()

        while True:
            attempt += 1
            key = idempotency_key(self.workflow_id, step.id, attempt)
            record = self._journal.append(
                type=RecordType.STEP_STARTED,
                workflow_id=self.workflow_id,
                step_id=step.id,
                attempt=attempt,
                idempotency_key=key,
            )
            replay.apply(self.snapshot, record)

            ctx = StepContext(
                workflow_id=self.workflow_id,
                step_id=step.id,
                attempt=attempt,
                idempotency_key=key,
                upstream=self._upstream_results(step),
            )

            try:
                if step.timeout_s is not None:
                    result = await asyncio.wait_for(step.handler(ctx), timeout=step.timeout_s)
                else:
                    result = await step.handler(ctx)
            except asyncio.CancelledError:
                raise  # never swallow -- would corrupt the event loop's cancellation protocol
            except TimeoutError as exc:
                self._journal_uncertain(step.id, str(exc) or f"timed out after {step.timeout_s}s")
                raise StepFailure(step.id, exc) from exc
            except Exception as exc:
                disposition = classify(exc)
                if disposition is Disposition.UNCERTAIN:
                    self._journal_uncertain(step.id, str(exc))
                    raise StepFailure(step.id, exc) from exc

                elapsed = time.monotonic() - start
                terminal = disposition is not Disposition.TRANSIENT or step.retry.exhausted(attempt, elapsed)
                record = self._journal.append(
                    type=RecordType.STEP_FAILED,
                    workflow_id=self.workflow_id,
                    step_id=step.id,
                    payload={"error": str(exc), "terminal": terminal},
                )
                replay.apply(self.snapshot, record)
                if not terminal:
                    await asyncio.sleep(step.retry.delay_for(attempt))
                    continue
                raise StepFailure(step.id, exc) from exc
            else:
                record = self._journal.append(
                    type=RecordType.STEP_COMPLETED,
                    workflow_id=self.workflow_id,
                    step_id=step.id,
                    payload={"result": result},
                )
                replay.apply(self.snapshot, record)
                self._completion_events[step.id].set()
                return

    def _journal_uncertain(self, step_id: str, reason: str) -> None:
        record = self._journal.append(
            type=RecordType.STEP_UNCERTAIN,
            workflow_id=self.workflow_id,
            step_id=step_id,
            payload={"reason": reason},
        )
        replay.apply(self.snapshot, record)

    def _upstream_results(self, step: Step) -> dict[str, Any]:
        return {dep_id: self.snapshot.steps[dep_id].result for dep_id in step.depends_on}
