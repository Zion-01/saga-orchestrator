"""The engine: concurrent, dependency-ordered, WAL-first forward execution
(phase 3) plus compensation, cancellation ceremony, and poison-pill handling
(phase 4).

Forward path
------------
- One ``asyncio`` task per step, spawned inside a single ``asyncio.TaskGroup``
  in ``sorted(step_id)`` order (CLAUDE.md's determinism rule for the ready
  set), each waiting on its dependencies' completion events before running.
- Every state transition is journaled and fsync'd *before* it is applied to
  the in-memory snapshot, and the snapshot is mutated *before* any handler
  runs: ``journal.append() -> write -> fsync -> replay.apply() -> handler``.
- The live snapshot is mutated through the exact :func:`replay.apply` /
  :func:`replay.workflow_started` functions :func:`replay.fold` uses, so the
  live snapshot and ``fold(all_records)`` can never drift apart.
- Retries follow ``Step.retry``; ``timeout_s`` is enforced with
  ``asyncio.wait_for`` and any ``TimeoutError`` is treated as UNCERTAIN, never
  retried.

Failure, cancellation, compensation
-----------------------------------
When a step cannot make forward progress it raises ``StepFailure`` to unwind
the ``TaskGroup``, whose native semantics cancel every sibling. Phase 4 layers
the ceremony CLAUDE.md 4.3 describes on top:

* **Cancellation is journaled, never swallowed.** A step wrapper that catches
  ``asyncio.CancelledError`` writes ``STEP_CANCELLED`` (or ``STEP_UNCERTAIN``
  if the handler ignored cancellation past ``cancel_grace_s``) and re-raises.
* **A cancelled in-flight step is a zombie by construction.** Its
  ``STEP_STARTED`` is already durably on disk; its request may already have
  landed. So every ``CANCELLED`` step is promoted unconditionally to
  ``UNCERTAIN`` at the start of rollback and enters the same ladder a crashed
  step would (phase 5's probe step aside).
* **Bounded unwinding.** After the TaskGroup requests cancellation the wrapper
  gives the handler ``cancel_grace_s`` to unwind; a handler that runs past
  that is abandoned (kept referenced, drained at the end of ``run``) and left
  ``UNCERTAIN``.
* **``cancellable=False``** steps are ``asyncio.shield``-ed so the sibling
  storm cannot cancel them, allowed to finish, recorded ``STEP_COMPLETED``,
  and then compensated normally.
* **Compensation runs outside the storm.** The ``TaskGroup`` is allowed to
  fully unwind first; rollback then runs in a fresh task, shielded so a caller
  cancelling ``run()`` cannot kill the rollback that a cancellation demanded.

Rollback is serial by default, in descending completion LSN -- the literal
"inverse-order compensation", reconstructible from the journal alone. A step
that never completed (``UNCERTAIN`` from a cancel) is ordered by its
``STEP_STARTED`` LSN instead. ``parallel_compensation=True`` relaxes this to
reverse-topological levels, compensating mutually-independent steps
concurrently; it is off by default because the guarantee is determinism, not
rollback throughput.

A compensator that exhausts its (separate) ``compensation_retry`` budget or
raises a permanent error lands the step in terminal ``COMPENSATION_FAILED``
and the workflow in ``DEAD_LETTER``. Rollback does not simply halt: the
ancestors of the failed step are quarantined -- journaled ``STEP_SKIPPED``
with ``quarantined_by`` -- while independent branches keep unwinding. An
:class:`~saga.manifest.InterventionManifest` naming every orphaned resource is
written next to the journal and referenced from the ``WORKFLOW_DEAD_LETTER``
record.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import replay
from .dag import ancestors, reverse_topological_levels
from .errors import StepFailure, WorkflowFailed
from .idempotency import compensation_key, idempotency_key
from .journal import Journal
from .manifest import FailedCompensation, InterventionManifest, write_manifest
from .models import Step, StepContext, WorkflowSnapshot, WorkflowSpec
from .records import RecordType
from .retry import Disposition, classify as _default_classify
from .states import StepState, WorkflowState

_ROLLBACK_STATES = (StepState.COMPLETED, StepState.UNCERTAIN)


class Orchestrator:
    """Runs one :class:`WorkflowSpec` against an already-open
    :class:`~saga.journal.Journal`. One-shot: call :meth:`run` once.

    ``cancel_grace_s`` bounds how long a cancelled handler is waited on before
    it is abandoned and left ``UNCERTAIN``. ``parallel_compensation`` relaxes
    serial reverse-completion rollback to reverse-topological levels.
    ``classifier`` overrides :func:`saga.retry.classify` for both the forward
    and the compensation retry loops.
    """

    def __init__(
        self,
        spec: WorkflowSpec,
        journal: Journal,
        *,
        cancel_grace_s: float = 5.0,
        parallel_compensation: bool = False,
        classifier: Callable[[BaseException], Disposition] = _default_classify,
    ) -> None:
        self._spec = spec
        self._journal = journal
        self.workflow_id = spec.workflow_id
        self._cancel_grace_s = cancel_grace_s
        self._parallel_compensation = parallel_compensation
        self._classify = classifier
        self._snapshot: WorkflowSnapshot | None = None
        self._completion_events: dict[str, asyncio.Event] = {}
        self._abandoned: list[asyncio.Task[Any]] = []
        self._forward_failure: StepFailure | None = None
        #: set once a DEAD_LETTER manifest has been written, for callers/tests.
        self.manifest: InterventionManifest | None = None

    @property
    def snapshot(self) -> WorkflowSnapshot:
        """The live snapshot. Available as soon as ``run()`` has journaled
        ``WORKFLOW_STARTED`` -- including after ``run()`` raises.
        """
        if self._snapshot is None:
            raise RuntimeError("Orchestrator.run() has not been awaited yet")
        return self._snapshot

    # -- lifecycle ----------------------------------------------------------

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
            # Deterministic pick when two branches fail in the same tick.
            self._forward_failure = min(eg.exceptions, key=lambda e: e.step_id)
        finally:
            await self._drain_abandoned()

        if self._forward_failure is None:
            record = self._journal.append(type=RecordType.WORKFLOW_COMPLETED, workflow_id=self.workflow_id)
            replay.apply(self.snapshot, record)
            return self.snapshot

        # Rollback runs outside the (now fully unwound) TaskGroup, shielded so
        # a caller cancelling run() cannot kill the compensation the failure
        # itself demanded.
        comp_task = asyncio.ensure_future(self._compensate())
        try:
            await asyncio.shield(comp_task)
        except asyncio.CancelledError:
            await comp_task
            raise
        raise WorkflowFailed(self._forward_failure.step_id, self._forward_failure.cause)

    async def _drain_abandoned(self) -> None:
        if not self._abandoned:
            return
        for task in self._abandoned:
            task.cancel()
        await asyncio.gather(*self._abandoned, return_exceptions=True)
        self._abandoned = []

    # -- forward path -----------------------------------------------------

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
            work = asyncio.ensure_future(self._call_handler(step, ctx))

            try:
                # Always shield the wait itself: a sibling-storm cancellation
                # reaches this wrapper without auto-cancelling ``work``, so
                # _on_cancelled keeps full control over how long the handler is
                # given to unwind (the grace period) and over cancellable=False.
                result = await asyncio.shield(work)
            except asyncio.CancelledError:
                await self._on_cancelled(step, work)
                raise  # never swallow -- would corrupt the loop's cancellation protocol
            except TimeoutError as exc:
                self._journal_apply(
                    RecordType.STEP_UNCERTAIN,
                    step_id=step.id,
                    payload={"reason": str(exc) or f"timed out after {step.timeout_s}s"},
                )
                raise StepFailure(step.id, exc) from exc
            except Exception as exc:
                disposition = self._classify(exc)
                if disposition is Disposition.UNCERTAIN:
                    self._journal_apply(
                        RecordType.STEP_UNCERTAIN, step_id=step.id, payload={"reason": str(exc)}
                    )
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

    async def _call_handler(self, step: Step, ctx: StepContext) -> Any:
        if step.timeout_s is not None:
            return await asyncio.wait_for(step.handler(ctx), timeout=step.timeout_s)
        return await step.handler(ctx)

    async def _on_cancelled(self, step: Step, work: asyncio.Task[Any]) -> None:
        """Handle a sibling-storm cancellation reaching one step wrapper.

        ``work`` is the still-running handler task (shielded, so the storm did
        not touch it). This method never re-raises: the caller re-raises the
        original ``CancelledError`` so the ``TaskGroup`` unwinds cleanly.
        """
        if not step.cancellable:
            # Shielded critical section: let it finish (no time bound), then
            # record it as a normal completion so rollback compensates it like
            # any other step.
            await self._wait_out(work, None)
            if work.cancelled():
                self._journal_apply(
                    RecordType.STEP_UNCERTAIN,
                    step_id=step.id,
                    payload={"reason": "shielded non-cancellable handler was cancelled"},
                )
                return
            exc = work.exception()
            if exc is not None:
                self._journal_apply(
                    RecordType.STEP_UNCERTAIN,
                    step_id=step.id,
                    payload={"reason": f"shielded handler failed during cancellation: {exc}"},
                )
                return
            record = self._journal.append(
                type=RecordType.STEP_COMPLETED,
                workflow_id=self.workflow_id,
                step_id=step.id,
                payload={"result": work.result()},
            )
            replay.apply(self.snapshot, record)
            self._completion_events[step.id].set()
            return

        # Cancellable: request the unwind now, then wait at most the grace
        # period. A handler that runs past it is abandoned (drained at the end
        # of run()) and left UNCERTAIN -- its effect may already have landed.
        work.cancel()
        if await self._wait_out(work, self._cancel_grace_s):
            self._journal_apply(RecordType.STEP_CANCELLED, step_id=step.id)
        else:
            self._abandoned.append(work)
            self._journal_apply(
                RecordType.STEP_UNCERTAIN,
                step_id=step.id,
                payload={"reason": "handler ignored cancellation past grace period"},
            )

    async def _wait_out(self, work: asyncio.Task[Any], timeout: float | None) -> bool:
        """Wait for ``work`` to settle, tolerating the re-cancellations that
        keep reaching this wrapper while its ``TaskGroup`` unwinds. Returns
        ``True`` if ``work`` settled, ``False`` if ``timeout`` elapsed first.
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while not work.done():
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return False
            try:
                await asyncio.wait_for(asyncio.shield(work), timeout=remaining)
            except TimeoutError:
                return False
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
            except Exception:  # noqa: BLE001 - work settled by raising; that's "done"
                return True
        return True

    def _upstream_results(self, step: Step) -> dict[str, Any]:
        return {dep_id: self.snapshot.steps[dep_id].result for dep_id in step.depends_on}

    # -- compensation ----------------------------------------------------

    async def _compensate(self) -> None:
        comp_record = self._journal_apply(RecordType.WORKFLOW_COMPENSATING)
        incident_start = comp_record.lsn

        # 1. A cancelled in-flight step is a zombie: promote it to UNCERTAIN.
        for step_id in sorted(self.snapshot.steps):
            runtime = self.snapshot.steps[step_id]
            if runtime.state is StepState.CANCELLED:
                self._journal_apply(
                    RecordType.STEP_UNCERTAIN,
                    step_id=step_id,
                    payload={"reason": runtime.uncertain_reason or "cancelled in flight"},
                )

        # 2. Steps that never got past their intent are nothing to undo.
        for step_id in sorted(self.snapshot.steps):
            state = self.snapshot.steps[step_id].state
            if state in (StepState.PENDING, StepState.RETRYING):
                self._journal_apply(
                    RecordType.STEP_SKIPPED,
                    step_id=step_id,
                    payload={"reason": "not completed when workflow failed", "quarantined_by": None},
                )
            elif state is StepState.RUNNING:  # abandoned handler still detached
                self._journal_apply(
                    RecordType.STEP_UNCERTAIN,
                    step_id=step_id,
                    payload={"reason": "still running when rollback began"},
                )

        # 3. Roll back, newest effect first.
        quarantined: dict[str, str] = {}
        failed: list[FailedCompensation] = []
        dep_map = self._spec.depends_on

        for group in self._rollback_groups():
            targets = [self._process_pre_checks(sid, quarantined, failed) for sid in group]
            runnable = [(sid, step) for sid, step in targets if step is not None]
            just_failed: list[str] = [
                sid for sid, step in targets if step is None and sid in {f.step_id for f in failed}
            ]

            if runnable:
                results = await asyncio.gather(
                    *(self._run_compensation(step, self.snapshot.steps[sid]) for sid, step in runnable)
                )
                for (sid, _step), (ok, failure) in zip(runnable, results):
                    if not ok and failure is not None:
                        failed.append(failure)
                        just_failed.append(sid)

            for culprit in just_failed:
                for anc in ancestors(dep_map, culprit):
                    if anc not in quarantined and self.snapshot.steps[anc].state in _ROLLBACK_STATES:
                        quarantined[anc] = culprit

        # 4. Terminal outcome.
        if failed:
            manifest = InterventionManifest.for_incident(
                snapshot=self.snapshot,
                journal_path=self._journal.path,
                incident_start_lsn=incident_start,
                end_lsn=self._journal.next_lsn,
                failed_compensations=failed,
                quarantined=quarantined,
            )
            path = write_manifest(manifest, self._journal.path)
            self.manifest = manifest
            self._journal_apply(RecordType.WORKFLOW_DEAD_LETTER, payload={"manifest": path.name})
        else:
            self._journal_apply(RecordType.WORKFLOW_COMPENSATED)

    def _rollback_groups(self) -> list[list[str]]:
        """The order rollback walks steps in.

        Serial (default): one step per group, descending completion LSN (a
        never-completed UNCERTAIN step ordered by its STEP_STARTED LSN).
        Parallel: reverse-topological levels, so mutually-independent steps in
        a level are compensated concurrently while a level is still strictly
        after every level that depends on it.
        """
        if self._parallel_compensation:
            return [sorted(level) for level in reverse_topological_levels(self._spec.depends_on)]

        def order_key(step_id: str) -> int:
            runtime = self.snapshot.steps[step_id]
            if runtime.completion_lsn is not None:
                return runtime.completion_lsn
            return runtime.started_lsn or 0

        candidates = [
            sid for sid, rt in self.snapshot.steps.items() if rt.state in _ROLLBACK_STATES
        ]
        return [[sid] for sid in sorted(candidates, key=order_key, reverse=True)]

    def _process_pre_checks(
        self, step_id: str, quarantined: dict[str, str], failed: list[FailedCompensation]
    ) -> tuple[str, Step | None]:
        """Resolve everything about a rollback target that does *not* need to
        await a compensator. Returns ``(step_id, step)`` if the compensator
        should run, ``(step_id, None)`` otherwise (already handled here).
        """
        runtime = self.snapshot.steps[step_id]
        if runtime.state not in _ROLLBACK_STATES:
            return step_id, None  # swept, or already compensated in an earlier group

        step = self._spec.steps[step_id]

        if step_id in quarantined:
            self._journal_apply(
                RecordType.STEP_SKIPPED,
                step_id=step_id,
                payload={"reason": "quarantined", "quarantined_by": quarantined[step_id]},
            )
            return step_id, None

        if runtime.state is StepState.UNCERTAIN and not step.compensate_on_uncertain:
            self._journal_apply(
                RecordType.STEP_SKIPPED,
                step_id=step_id,
                payload={"reason": "uncertain, assumed not run", "quarantined_by": None},
            )
            return step_id, None

        if step.compensate is None:
            if runtime.state is StepState.UNCERTAIN:
                # A possible orphan with no way to retract it: straight to DEAD_LETTER.
                error = "uncertain outcome and no compensator declared"
                self._journal_apply(
                    RecordType.COMPENSATION_FAILED,
                    step_id=step_id,
                    payload={"error": error, "status_code": None},
                )
                failed.append(
                    FailedCompensation(step_id, runtime.idempotency_key, 0, error, None, None)
                )
            # A plain COMPLETED step with no compensator has nothing to undo.
            return step_id, None

        return step_id, step

    async def _run_compensation(
        self, step: Step, runtime: Any
    ) -> tuple[bool, FailedCompensation | None]:
        """Bounded, classified compensation loop for one step. Never raises for
        an ordinary compensator error -- returns ``(False, FailedCompensation)``.
        """
        policy = step.compensation_retry
        start = time.monotonic()
        attempt = 0
        forward_result = runtime.result

        while True:
            attempt += 1
            key = compensation_key(self.workflow_id, step.id, attempt)
            record = self._journal.append(
                type=RecordType.COMPENSATION_STARTED,
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
                await step.compensate(ctx, forward_result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - classified below
                disposition = self._classify(exc)
                elapsed = time.monotonic() - start
                if disposition is Disposition.TRANSIENT and not policy.exhausted(attempt, elapsed):
                    await asyncio.sleep(policy.delay_for(attempt))
                    continue

                status = getattr(exc, "status_code", None)
                if not isinstance(status, int):
                    status = getattr(exc, "status", None)
                    status = status if isinstance(status, int) else None
                body = getattr(exc, "response_body", None)
                if body is None:
                    body = getattr(exc, "body", None)

                record = self._journal.append(
                    type=RecordType.COMPENSATION_FAILED,
                    workflow_id=self.workflow_id,
                    step_id=step.id,
                    payload={"error": str(exc), "status_code": status},
                )
                replay.apply(self.snapshot, record)
                return False, FailedCompensation(step.id, key, attempt, str(exc), status, body)
            else:
                record = self._journal.append(
                    type=RecordType.COMPENSATION_COMPLETED,
                    workflow_id=self.workflow_id,
                    step_id=step.id,
                )
                replay.apply(self.snapshot, record)
                return True, None

    # -- helpers -------------------------------------------------------

    def _journal_apply(
        self,
        type: RecordType,
        *,
        step_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ):
        record = self._journal.append(
            type=type, workflow_id=self.workflow_id, step_id=step_id, payload=payload or {}
        )
        replay.apply(self.snapshot, record)
        return record
