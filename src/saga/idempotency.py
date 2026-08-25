"""Deterministic idempotency-key derivation.

A key is a pure function of ``(workflow_id, step_id, attempt)``.  No clock, no
UUID, no hostname, no counter that lives only in memory.  That is what lets a
crashed-and-replayed invocation present the *byte-identical* key the remote side
already deduplicated against.
"""

from __future__ import annotations

from .errors import SagaError

SEPARATOR = ":"
_COMPENSATION_INFIX = "compensate"


def _validate(part: str, label: str) -> str:
    if not part:
        raise SagaError(f"{label} must be non-empty")
    if SEPARATOR in part:
        raise SagaError(f"{label} must not contain {SEPARATOR!r}: {part!r}")
    return part


def idempotency_key(workflow_id: str, step_id: str, attempt: int) -> str:
    """``workflow_id:step_id:attempt`` -- the forward-invocation key."""
    if attempt < 1:
        raise SagaError(f"attempt must be >= 1, got {attempt}")
    return SEPARATOR.join(
        (_validate(workflow_id, "workflow_id"), _validate(step_id, "step_id"), str(attempt))
    )


def compensation_key(workflow_id: str, step_id: str, attempt: int) -> str:
    """``workflow_id:step_id:compensate:attempt`` -- the rollback-invocation key.

    Compensations get their own key namespace so a compensator can never collide
    with the forward call it is undoing.
    """
    if attempt < 1:
        raise SagaError(f"attempt must be >= 1, got {attempt}")
    return SEPARATOR.join(
        (
            _validate(workflow_id, "workflow_id"),
            _validate(step_id, "step_id"),
            _COMPENSATION_INFIX,
            str(attempt),
        )
    )
