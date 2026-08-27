"""Phase 5, part 3: the crash-injection sweep -- the load-bearing test.

``SAGA_CRASH_AT_LSN=n`` makes the journal writer ``os._exit(1)`` the instant it
has fsync'd LSN ``n``. This test runs ``examples/booking_saga.py`` in a
subprocess once per ``n`` in ``1 .. max_lsn``, then calls
:func:`saga.recovery.recover` in this process and asserts, for *every* ``n``:

1. **No double execution.** Handlers append to a side-ledger outside the
   journal. A given idempotency key may appear more than once (replay is
   legal) but no step's forward effect is ever executed under two different
   attempt numbers -- the attempt counter never advances on recovery.
2. **No orphans.** Every step that ever reached ``COMPLETED`` ends
   ``COMPENSATED`` (this saga always rolls back fully).
3. **Terminality.** Recovery reaches a terminal workflow state; it never
   hangs (pytest-timeout would catch a hang) and never loops.
4. **Fold agreement.** The post-recovery in-memory snapshot equals a cold
   ``fold`` of the whole journal.
5. **Inverse order.** Compensation start-LSN order is the exact reverse of
   completion-LSN order.

Because the whole engine is a fold over an append-only log, sweeping every
LSN is exhaustive over crash points, not a sample.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = REPO_ROOT / "examples" / "booking_saga.py"

sys.path.insert(0, str(REPO_ROOT / "examples"))
import booking_saga  # noqa: E402

from saga.journal import read_journal  # noqa: E402
from saga.recovery import recover  # noqa: E402
from saga.replay import fold  # noqa: E402
from saga.states import TERMINAL_WORKFLOW_STATES, StepState, WorkflowState  # noqa: E402


def _run_example(journal: Path, ledger: Path, crash_at: int | None) -> int:
    env = dict(os.environ)
    env["SAGA_JOURNAL"] = str(journal)
    env["SAGA_LEDGER"] = str(ledger)
    if crash_at is not None:
        env["SAGA_CRASH_AT_LSN"] = str(crash_at)
    else:
        env.pop("SAGA_CRASH_AT_LSN", None)
    proc = subprocess.run(
        [sys.executable, str(EXAMPLE)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return proc.returncode


def _ledger_rows(ledger: Path) -> list[tuple[str, str, str]]:
    if not ledger.exists():
        return []
    rows = []
    for line in ledger.read_text(encoding="utf-8").splitlines():
        phase, step, key = line.split("\t")
        rows.append((phase, step, key))
    return rows


@pytest.fixture(scope="module")
def max_lsn(tmp_path_factory) -> int:
    """One clean, crash-free run establishes how many LSNs the saga produces."""
    d = tmp_path_factory.mktemp("baseline")
    journal = d / "wf.journal"
    rc = _run_example(journal, d / "ledger.txt", crash_at=None)
    assert rc == 0, "baseline booking saga run should exit 0"
    records = read_journal(journal).records
    assert records[-1].type.value == "WORKFLOW_COMPENSATED"
    return len(records)


def test_baseline_saga_shape(max_lsn) -> None:
    # flight + hotel forward, car fails, both compensated, workflow compensated.
    assert max_lsn >= 10


@pytest.mark.timeout(180)
def test_crash_at_every_lsn_recovers(max_lsn, tmp_path) -> None:
    for n in range(1, max_lsn + 1):
        journal = tmp_path / f"wf-{n}.journal"
        ledger = tmp_path / f"ledger-{n}.txt"

        rc = _run_example(journal, ledger, crash_at=n)
        assert rc == 1, f"n={n}: crash-injected run should exit 1"

        spec = booking_saga.build_spec()
        orch = recover(journal, spec)
        try:
            snap = asyncio.run(orch.run())  # resume run() drives to a terminal state
        finally:
            orch.close()

        records = read_journal(journal).records

        # 3. terminality
        assert snap.status in TERMINAL_WORKFLOW_STATES, f"n={n}: {snap.status} not terminal"
        assert snap.status is WorkflowState.COMPENSATED, f"n={n}: expected full rollback"

        # 4. fold agreement
        assert fold(records) == snap, f"n={n}: live snapshot != cold fold"

        # 2. no orphans -- every step that ever completed is compensated
        completed_ever = {r.step_id for r in records if r.type.value == "STEP_COMPLETED"}
        for step_id in completed_ever:
            assert snap.steps[step_id].state is StepState.COMPENSATED, (
                f"n={n}: {step_id} reached COMPLETED but ended {snap.steps[step_id].state}"
            )

        # 1. no double execution -- attempt counter never advances on recovery
        rows = _ledger_rows(ledger)
        by_step_phase: dict[tuple[str, str], set[int]] = {}
        for phase, step, key in rows:
            attempt = int(key.rsplit(":", 1)[1])
            by_step_phase.setdefault((phase, step), set()).add(attempt)
            # the key must be the canonical one for its attempt
            if phase == "forward":
                assert key == f"booking-7a1:{step}:{attempt}", f"n={n}: forged key {key!r}"
        for (phase, step), attempts in by_step_phase.items():
            assert attempts == {1}, (
                f"n={n}: {phase}/{step} ran under attempts {sorted(attempts)}, expected just 1"
            )

        # 5. inverse order
        completion = {r.step_id: r.lsn for r in records if r.type.value == "STEP_COMPLETED"}
        first_comp: dict[str, int] = {}
        for r in records:
            if r.type.value == "COMPENSATION_STARTED" and r.step_id not in first_comp:
                first_comp[r.step_id] = r.lsn
        by_completion = sorted(first_comp, key=lambda s: completion[s])
        by_compensation = sorted(first_comp, key=lambda s: first_comp[s], reverse=True)
        assert by_completion == by_compensation, f"n={n}: rollback not in inverse completion order"
