"""State machine definitions and the transition tables that enforce them.

Every mutation of a step's or a workflow's state routes through :func:`check`,
including mutations performed by the pure journal fold in :mod:`saga.replay`.
That means a corrupt or hand-edited journal fails loudly at recovery instead of
silently producing a state machine that never existed.
"""

from __future__ import annotations

from enum import Enum

from .errors import IllegalTransition


class StepState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNCERTAIN = "UNCERTAIN"
    SKIPPED = "SKIPPED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return self.value


class WorkflowState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    COMPENSATING = "COMPENSATING"
    COMPLETED = "COMPLETED"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return self.value


S = StepState
W = WorkflowState

#: ``RETRYING`` is the explicit form of the "RUNNING -> RUNNING on transient
#: failure" self-loop.  Making it a distinct state means the zombie predicate is
#: exactly ``state is RUNNING`` -- a step sitting in RETRYING had its failure
#: *observed and journaled*, so it is not a zombie candidate.
LEGAL_STEP_TRANSITIONS: dict[StepState, frozenset[StepState]] = {
    S.PENDING: frozenset({S.RUNNING, S.SKIPPED, S.CANCELLED}),
    S.RUNNING: frozenset({S.COMPLETED, S.FAILED, S.RETRYING, S.CANCELLED, S.UNCERTAIN}),
    S.RETRYING: frozenset({S.RUNNING, S.FAILED, S.CANCELLED, S.SKIPPED}),
    S.CANCELLED: frozenset({S.UNCERTAIN, S.SKIPPED}),
    S.UNCERTAIN: frozenset(
        {S.RUNNING, S.COMPLETED, S.COMPENSATING, S.SKIPPED, S.COMPENSATION_FAILED}
    ),
    S.COMPLETED: frozenset({S.COMPENSATING, S.SKIPPED}),
    S.COMPENSATING: frozenset({S.COMPENSATING, S.COMPENSATED, S.COMPENSATION_FAILED}),
    # terminal
    S.FAILED: frozenset(),
    S.SKIPPED: frozenset(),
    S.COMPENSATED: frozenset(),
    S.COMPENSATION_FAILED: frozenset(),
}

LEGAL_WORKFLOW_TRANSITIONS: dict[WorkflowState, frozenset[WorkflowState]] = {
    W.PENDING: frozenset({W.RUNNING, W.FAILED}),
    W.RUNNING: frozenset({W.COMPLETED, W.CANCELLING, W.COMPENSATING, W.FAILED}),
    W.CANCELLING: frozenset({W.COMPENSATING, W.FAILED}),
    W.COMPENSATING: frozenset({W.COMPENSATED, W.DEAD_LETTER, W.FAILED}),
    # terminal
    W.COMPLETED: frozenset(),
    W.COMPENSATED: frozenset(),
    W.FAILED: frozenset(),
    W.DEAD_LETTER: frozenset(),
}

#: Step states from which no further work will ever be scheduled.
TERMINAL_STEP_STATES = frozenset(
    {S.COMPLETED, S.FAILED, S.SKIPPED, S.COMPENSATED, S.COMPENSATION_FAILED}
)

#: Workflow states that mean "this journal is closed".
TERMINAL_WORKFLOW_STATES = frozenset({W.COMPLETED, W.COMPENSATED, W.FAILED, W.DEAD_LETTER})

#: Step states that mean the forward phase can never complete this step.
FORWARD_BLOCKED_STEP_STATES = frozenset(
    {S.FAILED, S.SKIPPED, S.COMPENSATING, S.COMPENSATED, S.COMPENSATION_FAILED}
)


def check(subject: str, frm: StepState | WorkflowState, to: StepState | WorkflowState) -> None:
    """Raise :class:`IllegalTransition` unless ``frm -> to`` is declared legal."""
    table = LEGAL_STEP_TRANSITIONS if isinstance(frm, StepState) else LEGAL_WORKFLOW_TRANSITIONS
    if to not in table.get(frm, frozenset()):
        raise IllegalTransition(subject, frm, to)
