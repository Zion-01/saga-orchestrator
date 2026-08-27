"""Booking saga: reserve a flight, then a date-matched hotel, then a car.

The car rental always fails with a permanent error, so the flight and hotel
reservations that already succeeded are rolled back in inverse completion
order. Run it directly to watch a full rollback and read the journal by eye::

    python examples/booking_saga.py

The steps form a linear chain (flight -> hotel -> car) on purpose: this module
is also the subject of ``tests/test_crash_sweep.py``, which needs a
byte-deterministic journal so it can kill the writer at every LSN in turn and
recover from each. Concurrency is exercised in ``tests/test_engine_forward.py``.

Environment knobs (all optional):

``SAGA_JOURNAL``
    Journal file path. Default: ``./booking.journal``.
``SAGA_LEDGER``
    Side-ledger path. Every *real* handler / compensator invocation appends one
    ``phase<TAB>step<TAB>idempotency_key`` line. The sweep reads this to prove
    no step's effect is ever executed under two different idempotency keys.
``SAGA_CRASH_AT_LSN``
    See :data:`saga.journal.CRASH_AT_LSN_ENV`.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from saga.engine import Orchestrator
from saga.errors import PermanentError, WorkflowFailed
from saga.journal import Journal
from saga.models import Step, StepContext, WorkflowSpec

WORKFLOW_ID = "booking-7a1"


def _ledger_append(phase: str, ctx: StepContext) -> None:
    ledger = os.environ.get("SAGA_LEDGER")
    if not ledger:
        return
    with open(ledger, "a", encoding="utf-8") as fh:
        fh.write(f"{phase}\t{ctx.step_id}\t{ctx.idempotency_key}\n")


async def _reserve(ctx: StepContext) -> dict[str, str]:
    await asyncio.sleep(0)
    _ledger_append("forward", ctx)
    return {"confirmation": f"{ctx.step_id.upper()}-{ctx.workflow_id}"}


async def _release(ctx: StepContext, result: object) -> None:
    await asyncio.sleep(0)
    _ledger_append("compensate", ctx)


async def _rent_car(ctx: StepContext) -> dict[str, str]:
    await asyncio.sleep(0)
    _ledger_append("forward", ctx)
    raise PermanentError("no cars available for those dates", status_code=409)


def build_spec(workflow_id: str = WORKFLOW_ID) -> WorkflowSpec:
    """The workflow spec, exported so the recovery sweep can hand it to
    :func:`saga.recovery.recover` (handlers are code, never in the journal).
    """
    flight = Step(id="flight", handler=_reserve, compensate=_release)
    hotel = Step(
        id="hotel", handler=_reserve, compensate=_release, depends_on=frozenset({"flight"})
    )
    car = Step(
        id="car", handler=_rent_car, compensate=_release, depends_on=frozenset({"hotel"})
    )
    return WorkflowSpec.of(workflow_id, [flight, hotel, car])


async def main() -> int:
    journal_path = Path(os.environ.get("SAGA_JOURNAL", "booking.journal"))
    spec = build_spec()
    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        try:
            snapshot = await orchestrator.run()
        except WorkflowFailed:
            snapshot = orchestrator.snapshot

    print(f"workflow {snapshot.workflow_id}: {snapshot.status}")
    for step_id in sorted(snapshot.steps):
        runtime = snapshot.steps[step_id]
        print(f"  {step_id:8} {runtime.state}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
