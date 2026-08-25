"""Data models: the static spec authored by users and the runtime snapshot
recovery folds into.

Split into two families on purpose. ``Step`` and ``WorkflowSpec`` are the
user-authored, immutable description of *what should happen*. ``StepRuntime``
and ``WorkflowSnapshot`` are the mutable reconstruction of *what has happened
so far*, produced only by folding the journal (:mod:`saga.replay`) or by the
live engine applying the same transitions in real time. Nothing here touches
I/O; these are plain in-memory records.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any

from .dag import validate as _validate_dag
from .errors import DagError
from .retry import DEFAULT_COMPENSATION_RETRY, DEFAULT_FORWARD_RETRY, RetryPolicy
from .states import StepState, WorkflowState, check


@dataclass(frozen=True, slots=True)
class StepContext:
    """What a handler, compensator, or probe sees when it is invoked."""

    workflow_id: str
    step_id: str
    attempt: int
    idempotency_key: str
    upstream: Mapping[str, Any]


class ProbeStatus(str, Enum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """Sum type: exactly one of ``FOUND(result)`` / ``NOT_FOUND`` / ``UNKNOWN``.

    Constructed via the ``found`` / ``not_found`` / ``unknown`` factories rather
    than the constructor directly, so a caller cannot accidentally attach a
    result to a non-FOUND outcome.
    """

    status: ProbeStatus
    result: Any = None

    def __post_init__(self) -> None:
        if self.status is not ProbeStatus.FOUND and self.result is not None:
            raise ValueError(f"{self.status} must not carry a result")

    @classmethod
    def found(cls, result: Any) -> ProbeResult:
        return cls(ProbeStatus.FOUND, result)

    @classmethod
    def not_found(cls) -> ProbeResult:
        return cls(ProbeStatus.NOT_FOUND)

    @classmethod
    def unknown(cls) -> ProbeResult:
        return cls(ProbeStatus.UNKNOWN)


Handler = Callable[[StepContext], Awaitable[Any]]
Compensator = Callable[[StepContext, Any], Awaitable[None]]
Prober = Callable[[str], Awaitable["ProbeResult | None"]]


@dataclass(frozen=True, slots=True)
class Step:
    """The static, user-authored unit of work."""

    id: str
    handler: Handler
    compensate: Compensator | None = None
    probe: Prober | None = None
    depends_on: frozenset[str] = frozenset()
    retry: RetryPolicy = DEFAULT_FORWARD_RETRY
    compensation_retry: RetryPolicy = DEFAULT_COMPENSATION_RETRY
    timeout_s: float | None = None
    cancellable: bool = True
    compensate_on_uncertain: bool = True

    def __post_init__(self) -> None:
        if not self.id:
            raise DagError("step id must be non-empty")
        if self.id in self.depends_on:
            raise DagError(f"step {self.id!r} depends on itself")


@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    """A validated DAG of steps: the immutable blueprint for one workflow."""

    workflow_id: str
    steps: Mapping[str, Step]

    def __post_init__(self) -> None:
        for step_id, step in self.steps.items():
            if step.id != step_id:
                raise DagError(f"step keyed {step_id!r} has id {step.id!r}")
        _validate_dag({step_id: step.depends_on for step_id, step in self.steps.items()})
        object.__setattr__(self, "steps", MappingProxyType(dict(self.steps)))

    @classmethod
    def of(cls, workflow_id: str, steps: Iterable[Step]) -> WorkflowSpec:
        """Build from a flat iterable, catching duplicate step ids up front.

        ``dag.validate`` operates on a ``{step_id: depends_on}`` mapping, which
        cannot represent a duplicate key at all -- so the duplicate check has to
        happen here, before that mapping is built.
        """
        by_id: dict[str, Step] = {}
        for step in steps:
            if step.id in by_id:
                raise DagError(f"duplicate step id {step.id!r}")
            by_id[step.id] = step
        return cls(workflow_id, by_id)

    @property
    def depends_on(self) -> dict[str, frozenset[str]]:
        return {step_id: step.depends_on for step_id, step in self.steps.items()}


@dataclass(slots=True)
class StepRuntime:
    """The mutable, reconstructed state of one step."""

    step_id: str
    state: StepState = StepState.PENDING
    attempt: int = 0
    compensation_attempt: int = 0
    idempotency_key: str | None = None
    result: Any = None
    last_error: str | None = None
    completion_lsn: int | None = None
    uncertain_reason: str | None = None


@dataclass(frozen=True, slots=True)
class DeadLetterEntry:
    """One orphaned step named in the ``InterventionManifest``."""

    step_id: str
    idempotency_key: str | None
    attempts: int
    error: str
    status_code: int | None = None


@dataclass(slots=True)
class WorkflowSnapshot:
    """The reconstructed machine: pure data, mutated only through ``transition``
    / ``transition_workflow`` so that every state change is checked against the
    tables in :mod:`saga.states`.
    """

    workflow_id: str
    epoch: int
    status: WorkflowState = WorkflowState.PENDING
    last_lsn: int = 0
    steps: dict[str, StepRuntime] = field(default_factory=dict)
    completion_order: list[str] = field(default_factory=list)
    dead_letter: list[DeadLetterEntry] = field(default_factory=list)

    @classmethod
    def for_spec(cls, spec: WorkflowSpec, epoch: int = 1) -> WorkflowSnapshot:
        return cls(
            workflow_id=spec.workflow_id,
            epoch=epoch,
            steps={step_id: StepRuntime(step_id) for step_id in spec.steps},
        )

    def transition(self, step_id: str, to: StepState) -> None:
        runtime = self.steps[step_id]
        check(f"{self.workflow_id}:{step_id}", runtime.state, to)
        runtime.state = to

    def transition_workflow(self, to: WorkflowState) -> None:
        check(self.workflow_id, self.status, to)
        self.status = to
