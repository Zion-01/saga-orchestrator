"""Poison-pill compensation: a rollback that can never succeed.

``charge_card`` succeeds, ``ship_order`` then fails forward, so rollback tries
to refund the card -- and the refund endpoint returns HTTP 400 every time. That
is not retryable, so compensation stops in bounded time, the workflow lands in
``DEAD_LETTER``, and an :class:`~saga.manifest.InterventionManifest` naming the
un-refunded charge is written next to the journal::

    python examples/poison_pill.py

Environment knobs: ``SAGA_JOURNAL`` (default ``./poison_pill.journal``) and
``SAGA_CRASH_AT_LSN`` (see :data:`saga.journal.CRASH_AT_LSN_ENV`).
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from saga.engine import Orchestrator
from saga.errors import PermanentError, WorkflowFailed
from saga.journal import Journal
from saga.manifest import manifest_path
from saga.models import Step, StepContext, WorkflowSpec

WORKFLOW_ID = "order-3b9"


async def _charge(ctx: StepContext) -> dict[str, str]:
    await asyncio.sleep(0)
    return {"charge_id": f"ch_{ctx.workflow_id}"}


async def _refund(ctx: StepContext, result: object) -> None:
    await asyncio.sleep(0)
    raise PermanentError("refund window closed", status_code=400)


async def _ship(ctx: StepContext) -> dict[str, str]:
    await asyncio.sleep(0)
    raise PermanentError("warehouse rejected the order", status_code=422)


def build_spec(workflow_id: str = WORKFLOW_ID) -> WorkflowSpec:
    charge = Step(id="charge_card", handler=_charge, compensate=_refund)
    ship = Step(
        id="ship_order", handler=_ship, compensate=_refund, depends_on=frozenset({"charge_card"})
    )
    return WorkflowSpec.of(workflow_id, [charge, ship])


async def main() -> int:
    journal_path = Path(os.environ.get("SAGA_JOURNAL", "poison_pill.journal"))
    spec = build_spec()
    with Journal.open_for_write(journal_path) as journal:
        orchestrator = Orchestrator(spec, journal)
        try:
            snapshot = await orchestrator.run()
        except WorkflowFailed:
            snapshot = orchestrator.snapshot

    print(f"workflow {snapshot.workflow_id}: {snapshot.status}")
    for step_id in sorted(snapshot.steps):
        print(f"  {step_id:12} {snapshot.steps[step_id].state}")
    if snapshot.dead_letter:
        print(f"manifest: {manifest_path(journal_path)}")
        for entry in snapshot.dead_letter:
            print(f"  orphaned {entry.step_id}: {entry.error} (status {entry.status_code})")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
