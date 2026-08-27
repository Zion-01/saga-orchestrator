"""The :class:`InterventionManifest`: the operator hand-off document written
next to a journal the moment a workflow reaches ``DEAD_LETTER``.

A poison-pill compensation is, by definition, something the engine cannot fix
on its own -- a human has to. This manifest is what that human reads. It is a
plain JSON file (``<journal>.manifest.json``) sitting beside a plain JSONL
journal precisely so the whole incident can be understood by eye:

* which compensations failed, with how many attempts, the final error, and
  any HTTP status / response body the compensator surfaced;
* the *surviving result payloads* of the failed and quarantined steps -- the
  orphaned-resource inventory an operator actually needs (booking ids, charge
  ids, ...);
* the quarantine list: which steps the engine deliberately did **not** roll
  back, and which failed step caused each to be held back;
* the exact ``saga inspect`` command to reproduce the analysis.

Building the manifest is pure: it reads a :class:`~saga.models.WorkflowSnapshot`
and some already-collected incident facts and returns a value. Writing it is
the only side effect, in :func:`write_manifest`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import WorkflowSnapshot

MANIFEST_SUFFIX = ".manifest.json"


def manifest_path(journal_path: Path | str) -> Path:
    """The manifest file that belongs to ``journal_path``."""
    return Path(str(journal_path) + MANIFEST_SUFFIX)


@dataclass(frozen=True, slots=True)
class FailedCompensation:
    """One compensator that exhausted its budget or hit a permanent error."""

    step_id: str
    idempotency_key: str | None
    attempts: int
    error: str
    status_code: int | None = None
    response_body: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "idempotency_key": self.idempotency_key,
            "attempts": self.attempts,
            "error": self.error,
            "status_code": self.status_code,
            "response_body": self.response_body,
        }


@dataclass(frozen=True, slots=True)
class OrphanedResource:
    """A step whose forward side effect is still out there, unretracted."""

    step_id: str
    state: str
    idempotency_key: str | None
    result: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "state": self.state,
            "idempotency_key": self.idempotency_key,
            "result": self.result,
        }


@dataclass(frozen=True, slots=True)
class QuarantinedStep:
    """A step the engine chose not to compensate, and the failure that caused it."""

    step_id: str
    quarantined_by: str

    def to_dict(self) -> dict[str, Any]:
        return {"step_id": self.step_id, "quarantined_by": self.quarantined_by}


@dataclass(frozen=True, slots=True)
class InterventionManifest:
    workflow_id: str
    epoch: int
    lsn_range: tuple[int, int]
    failed_compensations: list[FailedCompensation]
    orphaned_resources: list[OrphanedResource]
    quarantined: list[QuarantinedStep]
    inspect_command: str

    @classmethod
    def for_incident(
        cls,
        *,
        snapshot: WorkflowSnapshot,
        journal_path: Path | str,
        incident_start_lsn: int,
        end_lsn: int,
        failed_compensations: list[FailedCompensation],
        quarantined: dict[str, str],
    ) -> InterventionManifest:
        orphan_ids = sorted(
            {fc.step_id for fc in failed_compensations} | set(quarantined)
        )
        orphaned = [
            OrphanedResource(
                step_id=sid,
                state=str(snapshot.steps[sid].state),
                idempotency_key=snapshot.steps[sid].idempotency_key,
                result=snapshot.steps[sid].result,
            )
            for sid in orphan_ids
        ]
        return cls(
            workflow_id=snapshot.workflow_id,
            epoch=snapshot.epoch,
            lsn_range=(incident_start_lsn, end_lsn),
            failed_compensations=list(failed_compensations),
            orphaned_resources=orphaned,
            quarantined=[QuarantinedStep(sid, by) for sid, by in sorted(quarantined.items())],
            inspect_command=f"saga inspect {journal_path}",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "epoch": self.epoch,
            "lsn_range": list(self.lsn_range),
            "failed_compensations": [fc.to_dict() for fc in self.failed_compensations],
            "orphaned_resources": [o.to_dict() for o in self.orphaned_resources],
            "quarantined": [q.to_dict() for q in self.quarantined],
            "inspect_command": self.inspect_command,
        }

    def to_json(self) -> str:
        """Pretty, stable JSON. ``default=repr`` keeps a non-serializable step
        result from sinking the whole hand-off document."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True, default=repr)


def write_manifest(manifest: InterventionManifest, journal_path: Path | str) -> Path:
    """Write ``manifest`` to ``<journal_path>.manifest.json`` and return that path."""
    path = manifest_path(journal_path)
    path.write_text(manifest.to_json(), encoding="utf-8")
    return path
