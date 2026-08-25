"""Exception taxonomy.

The two classes that matter to step authors are :class:`TransientError` (retry me)
and :class:`PermanentError` (do not retry me, ever).  Everything else is either an
engine-internal signal or an operator-facing failure.
"""

from __future__ import annotations


class SagaError(Exception):
    """Base class for everything this package raises."""


# --- classification signals raised by user handlers -------------------------


class TransientError(SagaError):
    """The operation failed but may succeed if retried."""


class PermanentError(SagaError):
    """The operation failed in a way that retrying cannot fix (e.g. HTTP 400)."""

    def __init__(self, message: str = "", *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class UncertainOutcome(SagaError):
    """The handler cannot tell whether its side effect landed.

    Raising this is stronger than failing: the step goes to ``UNCERTAIN`` and is
    resolved through the probe/replay ladder rather than simply retried.
    """


# --- structural / durability errors ----------------------------------------


class DagError(SagaError):
    """The workflow specification is not a valid DAG."""


class IllegalTransition(SagaError):
    """A state transition outside the declared transition table was attempted."""

    def __init__(self, subject: str, frm: object, to: object) -> None:
        super().__init__(f"illegal transition for {subject}: {frm} -> {to}")
        self.subject = subject
        self.frm = frm
        self.to = to


class JournalCorruption(SagaError):
    """The journal failed a CRC, framing, or LSN-continuity check."""


class JournalLocked(SagaError):
    """Another process holds the single-writer lock for this journal."""


class SpecMismatch(SagaError):
    """The supplied workflow spec does not match the one recorded in the journal."""


# --- control-flow signals ---------------------------------------------------


class StepFailure(SagaError):
    """Internal: raised inside a step task to unwind the forward TaskGroup."""

    def __init__(self, step_id: str, cause: BaseException | None = None) -> None:
        super().__init__(f"step {step_id!r} failed: {cause!r}")
        self.step_id = step_id
        self.cause = cause
