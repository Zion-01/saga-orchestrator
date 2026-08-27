"""Journal record schema: canonical JSON encode/decode with a CRC32 guard.

Every record carries a fixed set of top-level fields (the framing) plus a
``payload`` dict whose shape depends on ``type``. The payload contract below
is what :mod:`saga.replay` reads and what a future engine writes -- it is
documented here, next to the schema, rather than split across both:

=========================  =========================================
RecordType                 payload
=========================  =========================================
WORKFLOW_STARTED           {"step_ids": [str, ...]}
RECOVERY_STARTED           {}
STEP_STARTED               {}   (attempt / idempotency_key are top-level)
STEP_COMPLETED             {"result": Any}
STEP_FAILED                {"error": str, "terminal": bool}
STEP_CANCELLED             {}
STEP_UNCERTAIN             {"reason": str}
STEP_PROBE_RESOLVED        {"resolution": "FOUND" | "NOT_FOUND" | "UNKNOWN"}
STEP_SKIPPED               {"reason": str, "quarantined_by": str | None}
WORKFLOW_COMPENSATING      {}
COMPENSATION_STARTED       {}   (top-level attempt is the *compensation* attempt)
COMPENSATION_COMPLETED     {}
COMPENSATION_FAILED        {"error": str, "status_code": int | None}
WORKFLOW_COMPLETED         {}
WORKFLOW_COMPENSATED       {}
WORKFLOW_DEAD_LETTER       {"manifest": str}   (basename of the InterventionManifest)
=========================  =========================================

The top-level ``attempt`` field is contextual: for forward-path record types
it is the forward attempt number; for ``COMPENSATION_*`` types it is the
compensation attempt number. The two never collide because a step is never
in both a forward and a compensation phase at once.
"""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .errors import JournalCorruption


class RecordType(str, Enum):
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    RECOVERY_STARTED = "RECOVERY_STARTED"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMPLETED = "STEP_COMPLETED"
    STEP_FAILED = "STEP_FAILED"
    STEP_CANCELLED = "STEP_CANCELLED"
    STEP_UNCERTAIN = "STEP_UNCERTAIN"
    STEP_PROBE_RESOLVED = "STEP_PROBE_RESOLVED"
    STEP_SKIPPED = "STEP_SKIPPED"
    WORKFLOW_COMPENSATING = "WORKFLOW_COMPENSATING"
    COMPENSATION_STARTED = "COMPENSATION_STARTED"
    COMPENSATION_COMPLETED = "COMPENSATION_COMPLETED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"
    WORKFLOW_COMPLETED = "WORKFLOW_COMPLETED"
    WORKFLOW_COMPENSATED = "WORKFLOW_COMPENSATED"
    WORKFLOW_DEAD_LETTER = "WORKFLOW_DEAD_LETTER"

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return self.value


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True, slots=True)
class JournalRecord:
    """One journaled state transition. ``crc`` is derived, never authored."""

    lsn: int
    epoch: int
    ts: float
    type: RecordType
    workflow_id: str
    step_id: str | None
    attempt: int | None
    idempotency_key: str | None
    payload: dict[str, Any] = field(default_factory=dict)

    def _body(self) -> dict[str, Any]:
        """Every field except the CRC itself -- what the CRC is computed over."""
        return {
            "lsn": self.lsn,
            "epoch": self.epoch,
            "ts": self.ts,
            "type": str(self.type),
            "workflow_id": self.workflow_id,
            "step_id": self.step_id,
            "attempt": self.attempt,
            "idempotency_key": self.idempotency_key,
            "payload": self.payload,
        }

    def crc(self) -> str:
        """CRC32, as 8 lowercase hex digits, over the canonical JSON body.

        Canonicalizing (sorted keys, tight separators) before hashing is what
        makes the CRC stable across writers/platforms -- see CLAUDE.md's
        determinism rules.
        """
        return format(zlib.crc32(_canonical_json(self._body()).encode("utf-8")), "08x")

    def encode(self) -> str:
        """One canonical JSON line, without a trailing newline.

        Raises ``TypeError``/``ValueError`` immediately -- before any I/O --
        if ``payload`` is not JSON-serializable or contains NaN/Infinity.
        """
        line = self._body()
        line["crc"] = self.crc()
        return _canonical_json(line)

    @classmethod
    def decode(cls, line: str) -> JournalRecord:
        """Parse and CRC-verify one line. Raises ``JournalCorruption`` on any
        framing problem: malformed JSON, a missing/misshapen field, or a CRC
        that does not match the recomputed value.
        """
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise JournalCorruption(f"malformed JSON line: {exc}") from exc

        try:
            crc = obj.pop("crc")
            record = cls(
                lsn=obj["lsn"],
                epoch=obj["epoch"],
                ts=obj["ts"],
                type=RecordType(obj["type"]),
                workflow_id=obj["workflow_id"],
                step_id=obj.get("step_id"),
                attempt=obj.get("attempt"),
                idempotency_key=obj.get("idempotency_key"),
                payload=obj.get("payload", {}),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise JournalCorruption(f"malformed record fields: {exc}") from exc

        expected = record.crc()
        if crc != expected:
            raise JournalCorruption(f"crc mismatch at lsn={record.lsn}: expected {expected}, got {crc!r}")
        return record
