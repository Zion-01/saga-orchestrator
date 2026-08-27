"""``saga`` -- a read-only command-line inspector for journal files.

Five subcommands, all built on the same pure :func:`saga.replay.fold` the
engine and recovery use, so what the CLI shows is exactly what recovery would
act on:

``saga inspect <journal>``
    Human-readable snapshot: workflow status, every step's state, and -- for a
    zombie (a step still ``RUNNING`` / ``UNCERTAIN`` in the fold) -- the
    idempotency key an operator would hand to a probe.
``saga verify <journal>``
    Fold the journal and report OK, or fail naming the offending LSN. This is
    the CRC / framing / LSN-continuity check.
``saga replay <journal>``
    Emit the folded :class:`~saga.models.WorkflowSnapshot` as JSON.
``saga manifest <journal>``
    Print the :class:`~saga.manifest.InterventionManifest` written beside a
    dead-lettered journal, or report its absence.
``saga graph <journal>``
    Emit a mermaid ``flowchart`` with every step annotated by its final state.
    Declared dependencies are not in the journal, so edges are the observed
    start/complete ordering -- flagged as such in a comment.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

from .errors import SagaError
from .journal import read_journal
from .manifest import manifest_path
from .records import JournalRecord, RecordType
from .replay import fold
from .states import StepState

_PROBE_ME = (StepState.RUNNING, StepState.UNCERTAIN)


def _load(journal: str):
    result = read_journal(Path(journal))
    return result, fold(result.records)


def cmd_inspect(args: argparse.Namespace) -> int:
    result, snap = _load(args.journal)
    print(f"journal     : {args.journal}")
    print(f"workflow_id : {snap.workflow_id}")
    print(f"epoch       : {snap.epoch}")
    print(f"status      : {snap.status}")
    print(f"records     : {len(result.records)} (last lsn {snap.last_lsn})")
    if result.torn_tail_bytes:
        print(f"torn tail   : {result.torn_tail_bytes} trailing bytes ignored")
    print("steps:")
    for step_id in sorted(snap.steps):
        rt = snap.steps[step_id]
        line = f"  {step_id:18} {rt.state}"
        if rt.state in _PROBE_ME:
            reason = f" -- {rt.uncertain_reason}" if rt.uncertain_reason else ""
            line += f"   needs probe: key={rt.idempotency_key}{reason}"
        elif rt.last_error:
            line += f"   last_error={rt.last_error!r}"
        print(line)
    if snap.dead_letter:
        print("dead letter:")
        for entry in snap.dead_letter:
            print(f"  {entry.step_id:18} {entry.error!r} (status {entry.status_code})")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    try:
        result = read_journal(Path(args.journal))
        snap = fold(result.records)
    except SagaError as exc:
        print(f"CORRUPT: {exc}", file=sys.stderr)
        return 1
    tail = (
        f", {result.torn_tail_bytes} torn-tail bytes ignored"
        if result.torn_tail_bytes
        else ""
    )
    print(
        f"OK: {len(result.records)} records, lsn 1..{snap.last_lsn}, "
        f"status {snap.status}{tail}"
    )
    return 0


def _snapshot_dict(snap) -> dict:
    return {
        "workflow_id": snap.workflow_id,
        "epoch": snap.epoch,
        "status": snap.status.value,
        "last_lsn": snap.last_lsn,
        "completion_order": list(snap.completion_order),
        "steps": {
            step_id: {
                "state": rt.state.value,
                "attempt": rt.attempt,
                "compensation_attempt": rt.compensation_attempt,
                "idempotency_key": rt.idempotency_key,
                "result": rt.result,
                "last_error": rt.last_error,
                "completion_lsn": rt.completion_lsn,
                "uncertain_reason": rt.uncertain_reason,
            }
            for step_id, rt in sorted(snap.steps.items())
        },
        "dead_letter": [dataclasses.asdict(entry) for entry in snap.dead_letter],
    }


def cmd_replay(args: argparse.Namespace) -> int:
    _result, snap = _load(args.journal)
    print(json.dumps(_snapshot_dict(snap), indent=2, default=repr))
    return 0


def cmd_manifest(args: argparse.Namespace) -> int:
    path = manifest_path(args.journal)
    if not path.exists():
        print(f"no manifest at {path} (the workflow did not dead-letter)")
        return 1
    print(path.read_text(encoding="utf-8"))
    return 0


_STATE_CLASS = {
    StepState.COMPLETED: "done",
    StepState.COMPENSATED: "undone",
    StepState.SKIPPED: "skipped",
    StepState.FAILED: "failed",
    StepState.COMPENSATION_FAILED: "failed",
    StepState.UNCERTAIN: "uncertain",
    StepState.RUNNING: "uncertain",
}


def _inferred_edges(records: list[JournalRecord]) -> list[tuple[str, str]]:
    """Edges from observed ordering: link each step to whichever step's
    STEP_COMPLETED most recently preceded this step's first STEP_STARTED.
    Not the declared DAG (which the journal does not carry) but a faithful
    picture of what ran before what.
    """
    first_start: dict[str, int] = {}
    completions: list[tuple[int, str]] = []
    for r in records:
        if r.step_id is None:
            continue
        if r.type is RecordType.STEP_STARTED and r.step_id not in first_start:
            first_start[r.step_id] = r.lsn
        elif r.type is RecordType.STEP_COMPLETED:
            completions.append((r.lsn, r.step_id))
    edges: list[tuple[str, str]] = []
    for step_id, started in sorted(first_start.items(), key=lambda kv: kv[1]):
        prior = [sid for lsn, sid in completions if lsn < started and sid != step_id]
        if prior:
            edges.append((prior[-1], step_id))
    return edges


def cmd_graph(args: argparse.Namespace) -> int:
    result, snap = _load(args.journal)
    lines = [
        "flowchart TD",
        f"    %% workflow {snap.workflow_id} -- status {snap.status}",
        "    %% edges are observed run ordering, not declared dependencies",
    ]
    for step_id in sorted(snap.steps):
        rt = snap.steps[step_id]
        node = step_id.replace("-", "_")
        lines.append(f'    {node}["{step_id}\\n{rt.state}"]')
        css = _STATE_CLASS.get(rt.state)
        if css:
            lines.append(f"    class {node} {css};")
    for parent, child in _inferred_edges(result.records):
        lines.append(f"    {parent.replace('-', '_')} --> {child.replace('-', '_')}")
    lines += [
        "    classDef done fill:#d4f7d4,stroke:#2e7d32;",
        "    classDef undone fill:#fff3cd,stroke:#b8860b;",
        "    classDef skipped fill:#eeeeee,stroke:#999999;",
        "    classDef failed fill:#f8d7da,stroke:#c62828;",
        "    classDef uncertain fill:#e1e8f7,stroke:#3949ab;",
    ]
    print("\n".join(lines))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="saga", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, func, help_text in (
        ("inspect", cmd_inspect, "human-readable snapshot of a journal"),
        ("verify", cmd_verify, "CRC + framing + LSN-continuity check; names any bad LSN"),
        ("replay", cmd_replay, "fold the journal and print the snapshot as JSON"),
        ("manifest", cmd_manifest, "print the InterventionManifest beside a dead-lettered journal"),
        ("graph", cmd_graph, "emit a mermaid graph annotated with each step's final state"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("journal", help="path to the journal file")
        p.set_defaults(func=func)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except SagaError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
