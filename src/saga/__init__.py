"""Deterministic saga orchestrator: an in-process, crash-tolerant workflow
engine that executes a DAG of steps under asyncio, journals every state
transition to disk before it happens, and can be killed at any instant and
resume without re-executing completed work or leaking half-applied effects.

Public surface::

    from saga import Orchestrator, Step, WorkflowSpec, Journal, recover

* :class:`Step` / :class:`WorkflowSpec` -- the static, user-authored DAG.
* :class:`Journal` -- the append+fsync write-ahead log; open one per workflow.
* :class:`Orchestrator` -- runs a spec against a journal (forward, then
  compensation on failure).
* :func:`recover` -- fold a journal left by a dead process and resume it.
* :class:`ProbeResult` -- a step's optional zombie resolver returns one of
  ``ProbeResult.found(...)`` / ``.not_found()`` / ``.unknown()``.

Error taxonomy for step authors: raise :class:`TransientError` to be retried,
:class:`PermanentError` to fail without retry, :class:`UncertainOutcome` when
the handler cannot tell whether its effect landed.
"""

from __future__ import annotations

from .engine import Orchestrator
from .errors import (
    PermanentError,
    SagaError,
    SpecMismatch,
    TransientError,
    UncertainOutcome,
    WorkflowFailed,
)
from .journal import Journal
from .models import ProbeResult, ProbeStatus, Step, StepContext, WorkflowSpec
from .recovery import recover
from .states import StepState, WorkflowState

__all__ = [
    "Orchestrator",
    "Journal",
    "Step",
    "StepContext",
    "WorkflowSpec",
    "ProbeResult",
    "ProbeStatus",
    "recover",
    "SagaError",
    "SpecMismatch",
    "TransientError",
    "PermanentError",
    "UncertainOutcome",
    "WorkflowFailed",
    "StepState",
    "WorkflowState",
]
