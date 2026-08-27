"""Phase 5, part 4: the ``saga`` CLI inspector.

``saga inspect|verify|replay|manifest|graph`` are read-only forensics over a
journal file. They share the same pure fold the engine and recovery use, so
what the CLI prints is exactly what recovery would act on.
"""

from __future__ import annotations

import json

import pytest

from saga.cli import main
from saga.engine import Orchestrator
from saga.errors import PermanentError, WorkflowFailed
from saga.journal import Journal, read_journal
from saga.models import Step, StepContext, WorkflowSpec

WF = "wf-cli"


async def _ok(ctx: StepContext):
    return {"id": ctx.step_id}


async def _boom(ctx: StepContext):
    raise PermanentError("no")


async def _noop_comp(ctx: StepContext, result: object) -> None:
    return None


async def _cold_run(path, spec, **kw):
    with Journal.open_for_write(path) as journal:
        orch = Orchestrator(spec, journal, **kw)
        try:
            await orch.run()
        except WorkflowFailed:
            pass
    return orch


def _rewind(path, keep_lsn: int) -> None:
    lines = path.read_bytes().split(b"\n")
    path.write_bytes(b"\n".join(lines[:keep_lsn]) + b"\n")


def _bit_flip_a_line(path, line_index: int) -> None:
    """Bump one digit of a record's ``ts`` field: the line still parses as
    JSON, but the CRC computed over the body no longer matches."""
    lines = path.read_bytes().split(b"\n")
    row = lines[line_index]
    i = row.index(b'"ts":') + 5
    row = row[:i] + bytes([48 + (row[i] - 48 + 1) % 10]) + row[i + 1 :]
    lines[line_index] = row
    path.write_bytes(b"\n".join(lines))


# --- inspect -------------------------------------------------------------


async def test_inspect_prints_status_and_step_states(journal_path, capsys) -> None:
    spec = WorkflowSpec.of(WF, [Step(id="alpha", handler=_ok), Step(id="beta", handler=_ok)])
    await _cold_run(journal_path, spec)

    rc = main(["inspect", str(journal_path)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "COMPLETED" in out
    assert "alpha" in out and "beta" in out
    assert WF in out


async def test_inspect_flags_a_zombie_step_and_the_key_that_needs_probing(journal_path, capsys) -> None:
    spec = WorkflowSpec.of(
        WF, [Step(id="alpha", handler=_ok), Step(id="beta", handler=_ok, depends_on=frozenset({"alpha"}))]
    )
    await _cold_run(journal_path, spec)

    # Rewind to just after beta's STEP_STARTED: the shape a mid-run Ctrl-C leaves.
    recs = read_journal(journal_path).records
    keep = next(r.lsn for r in recs if r.step_id == "beta" and r.type.value == "STEP_STARTED")
    _rewind(journal_path, keep)

    rc = main(["inspect", str(journal_path)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "beta" in out
    assert "RUNNING" in out
    assert "wf-cli:beta:1" in out  # the idempotency key an operator would probe


# --- verify -------------------------------------------------------------


async def test_verify_accepts_a_clean_journal(journal_path, capsys) -> None:
    await _cold_run(journal_path, WorkflowSpec.of(WF, [Step(id="a", handler=_ok)]))

    rc = main(["verify", str(journal_path)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "OK" in out


async def test_verify_names_the_offending_lsn_on_a_bit_flip(journal_path, capsys) -> None:
    spec = WorkflowSpec.of(
        WF, [Step(id="a", handler=_ok), Step(id="b", handler=_ok, depends_on=frozenset({"a"}))]
    )
    await _cold_run(journal_path, spec)

    _bit_flip_a_line(journal_path, 2)  # third line == lsn 3

    rc = main(["verify", str(journal_path)])
    err = capsys.readouterr().err
    assert rc == 1
    assert "lsn=3" in err


# --- replay -------------------------------------------------------------


async def test_replay_emits_the_folded_snapshot_as_json(journal_path, capsys) -> None:
    spec = WorkflowSpec.of(
        WF, [Step(id="a", handler=_ok), Step(id="b", handler=_ok, depends_on=frozenset({"a"}))]
    )
    await _cold_run(journal_path, spec)

    rc = main(["replay", str(journal_path)])
    out = capsys.readouterr().out

    assert rc == 0
    payload = json.loads(out)
    assert payload["workflow_id"] == WF
    assert payload["status"] == "COMPLETED"
    assert payload["steps"]["a"]["state"] == "COMPLETED"
    assert payload["completion_order"] == ["a", "b"]


# --- manifest -------------------------------------------------------------


async def test_manifest_prints_the_intervention_document(journal_path, capsys) -> None:
    async def charge(ctx: StepContext):
        return {"charge_id": "ch_1"}

    async def bad_refund(ctx: StepContext, result: object) -> None:
        raise PermanentError("refund window closed", status_code=400)

    async def ship(ctx: StepContext):
        raise PermanentError("warehouse down")

    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="charge", handler=charge, compensate=bad_refund),
            Step(id="ship", handler=ship, compensate=bad_refund, depends_on=frozenset({"charge"})),
        ],
    )
    await _cold_run(journal_path, spec)

    rc = main(["manifest", str(journal_path)])
    out = capsys.readouterr().out

    assert rc == 0
    doc = json.loads(out)
    assert doc["workflow_id"] == WF
    assert any(fc["step_id"] == "charge" for fc in doc["failed_compensations"])


async def test_manifest_reports_absence_when_not_dead_lettered(journal_path, capsys) -> None:
    await _cold_run(journal_path, WorkflowSpec.of(WF, [Step(id="a", handler=_ok)]))

    rc = main(["manifest", str(journal_path)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "no manifest" in out.lower()


# --- graph -------------------------------------------------------------


async def test_graph_emits_mermaid_with_every_step_and_its_final_state(journal_path, capsys) -> None:
    spec = WorkflowSpec.of(
        WF,
        [
            Step(id="a", handler=_ok, compensate=_noop_comp),
            Step(id="b", handler=_ok, compensate=_noop_comp, depends_on=frozenset({"a"})),
            Step(id="c", handler=_boom, compensate=_noop_comp, depends_on=frozenset({"b"})),
        ],
    )
    await _cold_run(journal_path, spec)

    rc = main(["graph", str(journal_path)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "flowchart" in out or "stateDiagram" in out
    for step_id in ("a", "b", "c"):
        assert step_id in out
    assert "COMPENSATED" in out
    assert "FAILED" in out
