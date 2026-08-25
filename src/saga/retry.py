"""Retry budgets and error classification.

Backoff is deterministic (no jitter) because reproducibility is this engine's
headline property.  If you want jitter, supply your own ``delay_for``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

from .errors import PermanentError, TransientError, UncertainOutcome


class Disposition(str, Enum):
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"
    UNCERTAIN = "UNCERTAIN"


#: 4xx statuses that are genuinely worth retrying despite being client errors.
RETRYABLE_STATUS = frozenset({408, 425, 429})


def classify(exc: BaseException) -> Disposition:
    """Decide whether ``exc`` may be retried.

    Recognises, in order: the explicit marker exceptions; anything carrying a
    ``status_code`` attribute (the shape most HTTP clients expose); timeouts.
    Unknown exceptions default to TRANSIENT on the forward path -- the retry
    budget, not the classifier, is what guarantees termination.
    """
    if isinstance(exc, UncertainOutcome):
        return Disposition.UNCERTAIN
    if isinstance(exc, PermanentError):
        return Disposition.PERMANENT
    if isinstance(exc, TransientError):
        return Disposition.TRANSIENT
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if isinstance(status, int) and 400 <= status < 500 and status not in RETRYABLE_STATUS:
        return Disposition.PERMANENT
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
        return Disposition.TRANSIENT
    return Disposition.TRANSIENT


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """A bounded retry budget.

    Termination is guaranteed by two independent caps: ``max_attempts`` and
    ``budget_s``.  There is no configuration in which the loop is unbounded.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    multiplier: float = 2.0
    max_delay: float = 30.0
    budget_s: float | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay_for(self, attempt: int) -> float:
        """Delay before attempt ``attempt + 1``, given ``attempt`` just failed."""
        return min(self.base_delay * (self.multiplier ** max(0, attempt - 1)), self.max_delay)

    def exhausted(self, attempt: int, elapsed: float) -> bool:
        if attempt >= self.max_attempts:
            return True
        return self.budget_s is not None and elapsed >= self.budget_s


#: Compensation defaults are deliberately tighter than forward defaults: a
#: rollback that will not converge should reach DEAD_LETTER quickly so a human
#: sees the InterventionManifest while the incident is still warm.
DEFAULT_FORWARD_RETRY = RetryPolicy(max_attempts=3)
DEFAULT_COMPENSATION_RETRY = RetryPolicy(max_attempts=3, base_delay=0.1, budget_s=60.0)
DEFAULT_PROBE_RETRY = RetryPolicy(max_attempts=3, base_delay=0.05, budget_s=10.0)
