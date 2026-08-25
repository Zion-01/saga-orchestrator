"""Every legal transition succeeds; every illegal one raises. Exhaustive over
the full state x state matrix, not a sample -- that is the point of having a
declared table at all.
"""

from __future__ import annotations

import pytest

from saga.errors import IllegalTransition
from saga.models import StepRuntime, WorkflowSnapshot
from saga.states import (
    LEGAL_STEP_TRANSITIONS,
    LEGAL_WORKFLOW_TRANSITIONS,
    TERMINAL_STEP_STATES,
    TERMINAL_WORKFLOW_STATES,
    StepState,
    WorkflowState,
    check,
)

STEP_PAIRS = [(frm, to) for frm in StepState for to in StepState]
WORKFLOW_PAIRS = [(frm, to) for frm in WorkflowState for to in WorkflowState]


@pytest.mark.parametrize("frm,to", STEP_PAIRS, ids=[f"{f}->{t}" for f, t in STEP_PAIRS])
def test_step_transition_matrix(frm: StepState, to: StepState) -> None:
    legal = to in LEGAL_STEP_TRANSITIONS.get(frm, frozenset())
    if legal:
        check("step", frm, to)  # must not raise
    else:
        with pytest.raises(IllegalTransition):
            check("step", frm, to)


@pytest.mark.parametrize("frm,to", WORKFLOW_PAIRS, ids=[f"{f}->{t}" for f, t in WORKFLOW_PAIRS])
def test_workflow_transition_matrix(frm: WorkflowState, to: WorkflowState) -> None:
    legal = to in LEGAL_WORKFLOW_TRANSITIONS.get(frm, frozenset())
    if legal:
        check("workflow", frm, to)
    else:
        with pytest.raises(IllegalTransition):
            check("workflow", frm, to)


def test_terminal_step_states_permit_no_forward_progress() -> None:
    # TERMINAL_STEP_STATES means "no further forward work", not "no outgoing
    # transition at all": COMPLETED can still move on to COMPENSATING/SKIPPED
    # when a later sibling failure triggers rollback.
    for state in TERMINAL_STEP_STATES:
        assert StepState.RUNNING not in LEGAL_STEP_TRANSITIONS[state]
        assert StepState.RETRYING not in LEGAL_STEP_TRANSITIONS[state]


def test_terminal_workflow_states_have_no_legal_outgoing_transitions() -> None:
    for state in TERMINAL_WORKFLOW_STATES:
        assert LEGAL_WORKFLOW_TRANSITIONS[state] == frozenset()


def test_illegal_transition_carries_subject_and_states() -> None:
    with pytest.raises(IllegalTransition) as exc_info:
        check("wf-1:charge_card", StepState.COMPLETED, StepState.RUNNING)
    err = exc_info.value
    assert err.subject == "wf-1:charge_card"
    assert err.frm is StepState.COMPLETED
    assert err.to is StepState.RUNNING


# --- WorkflowSnapshot routes every mutation through the same tables ---------


def test_snapshot_transition_allows_legal_step_move() -> None:
    snapshot = WorkflowSnapshot(workflow_id="wf-1", epoch=1, steps={"a": StepRuntime("a")})
    snapshot.transition("a", StepState.RUNNING)
    assert snapshot.steps["a"].state is StepState.RUNNING
    snapshot.transition("a", StepState.COMPLETED)
    assert snapshot.steps["a"].state is StepState.COMPLETED


def test_snapshot_transition_rejects_illegal_step_move() -> None:
    snapshot = WorkflowSnapshot(workflow_id="wf-1", epoch=1, steps={"a": StepRuntime("a")})
    with pytest.raises(IllegalTransition):
        snapshot.transition("a", StepState.COMPLETED)  # PENDING -> COMPLETED is not legal
    assert snapshot.steps["a"].state is StepState.PENDING  # rejected mutation leaves state intact


def test_snapshot_transition_workflow_allows_legal_move() -> None:
    snapshot = WorkflowSnapshot(workflow_id="wf-1", epoch=1)
    snapshot.transition_workflow(WorkflowState.RUNNING)
    assert snapshot.status is WorkflowState.RUNNING


def test_snapshot_transition_workflow_rejects_illegal_move() -> None:
    snapshot = WorkflowSnapshot(workflow_id="wf-1", epoch=1)
    with pytest.raises(IllegalTransition):
        snapshot.transition_workflow(WorkflowState.COMPLETED)  # PENDING -> COMPLETED skips RUNNING
    assert snapshot.status is WorkflowState.PENDING
