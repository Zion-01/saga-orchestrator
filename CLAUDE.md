Deterministic Saga Orchestrator — Architectural Plan
Context
There is no existing implementation anywhere under C:\Users\M'soft — this is a greenfield build. The target is an in-process, crash-tolerant workflow engine: a Python library that executes a DAG of steps concurrently under asyncio, journals every state transition to disk before it happens, and can be killed at any instant and resume without re-executing completed work or leaking half-applied side effects.

The hard problem is not the DAG scheduler — it is the gap between "a side effect happened in the outside world" and "we durably know it happened." Everything in this design exists to make that gap narrow, detectable, and recoverable. The engine is stdlib-only at its core (asyncio, json, os, zlib, dataclasses, enum) so durability behavior is explicit rather than delegated to a database.

Confirmed decisions from the design discussion:

Decision	Choice
Zombie-step resolution	Intent record + optional probe() with replay fallback
Journal format	JSONL, one record per line, CRC32 + monotonic LSN, O_APPEND + os.fsync
Deliverable scope	Library + pytest suite (with crash injection) + CLI inspector
Location	C:\Users\M'soft\saga-orchestrator
Runtime	Python 3.12.5 (asyncio.TaskGroup, except*, Self all available)
1. Project Structure
saga-orchestrator/
├── pyproject.toml                  # hatchling, py>=3.11, pytest + pytest-asyncio (dev only)
├── README.md
├── src/
│   └── saga/
│       ├── __init__.py             # public surface: Orchestrator, Step, recover, SagaError
│       ├── states.py               # StepState / WorkflowState enums + LEGAL_TRANSITIONS tables
│       ├── models.py               # Step, StepRuntime, WorkflowSpec, WorkflowSnapshot, ProbeResult
│       ├── records.py              # RecordType enum + JournalRecord (encode/decode/CRC)
│       ├── errors.py               # TransientError, PermanentError, UncertainOutcome, PoisonPill
│       ├── idempotency.py          # deterministic key derivation
│       ├── dag.py                  # cycle detection, dependency closure, reverse-topo ordering
│       ├── journal.py              # Journal: append+fsync writer, replaying reader, torn-tail repair
│       ├── replay.py               # fold(records) -> WorkflowSnapshot  (pure, no I/O)
│       ├── retry.py                # RetryPolicy, backoff, error classification hook
│       ├── engine.py               # Orchestrator: forward run, cancellation, compensation
│       ├── recovery.py             # recover(journal_file) -> resumable Orchestrator
│       ├── manifest.py             # InterventionManifest builder + writer
│       ├── lockfile.py             # single-writer guard per journal
│       └── cli.py                  # `saga inspect|verify|replay|manifest|graph`
├── examples/
│   ├── booking_saga.py             # flight + hotel + car, car fails -> full rollback
│   └── poison_pill.py              # compensation returns HTTP 400 -> DEAD_LETTER
└── tests/
    ├── conftest.py                 # tmp journal fixture, RecordingHandler side-ledger
    ├── test_dag.py
    ├── test_states.py              # every illegal transition raises
    ├── test_journal.py             # CRC corruption, torn tail, fsync call ordering
    ├── test_replay.py              # fold determinism
    ├── test_engine_forward.py
    ├── test_compensation.py        # inverse order, partial rollback
    ├── test_cancellation.py        # branch A cancelled when branch B fails
    ├── test_zombie.py              # probe FOUND / NOT_FOUND / UNKNOWN paths
    ├── test_poison_pill.py         # DEAD_LETTER + manifest contents
    └── test_crash_sweep.py         # ★ exhaustive kill-at-every-LSN recovery sweep
2. Data Models
Conceptual definitions — field names are the contract, exact typing is an implementation detail.

Step — the static, user-authored unit of work
Step:
    id                     str                  # unique within workflow; used in the idempotency key
    handler                async (ctx) -> Any   # the forward side effect
    compensate             async (ctx, result) -> None | None
    probe                  async (key) -> ProbeResult | None   # OPTIONAL zombie resolver
    depends_on             frozenset[str]       # parent step ids
    retry                  RetryPolicy          # forward attempts
    compensation_retry     RetryPolicy          # rollback attempts (separate budget)
    timeout_s              float | None
    cancellable            bool = True          # False => shielded from sibling-failure cancellation
    compensate_on_uncertain bool = True         # treat UNCERTAIN as "probably ran" and roll it back
ProbeResult is a small sum type: FOUND(result) | NOT_FOUND | UNKNOWN. ctx carries workflow_id, step_id, attempt, idempotency_key, and a read-only view of upstream step results.

JournalRecord — the durable unit
JournalRecord:
    lsn              int        # monotonic, gap-free, assigned by the writer
    epoch            int        # incarnation counter; bumped once per process that opens the journal
    ts               float      # wall clock, for humans ONLY — never an input to logic
    type             RecordType
    workflow_id      str
    step_id          str | None
    attempt          int | None
    idempotency_key  str | None
    payload          dict       # result snapshot, error detail, manifest ref — type-specific
    crc              str        # CRC32 over the canonical JSON of every other field
RecordType: WORKFLOW_STARTED, RECOVERY_STARTED, STEP_STARTED, STEP_COMPLETED, STEP_FAILED, STEP_CANCELLED, STEP_UNCERTAIN, STEP_PROBE_RESOLVED, STEP_SKIPPED, WORKFLOW_COMPENSATING, COMPENSATION_STARTED, COMPENSATION_COMPLETED, COMPENSATION_FAILED, WORKFLOW_COMPLETED, WORKFLOW_COMPENSATED, WORKFLOW_DEAD_LETTER.

On-disk line (canonical JSON: sort_keys=True, separators=(",",":"), no NaN):

{"attempt":1,"epoch":2,"idempotency_key":"wf-7a1:charge_card:1","lsn":41,"payload":{},"step_id":"charge_card","ts":1756...,"type":"STEP_STARTED","workflow_id":"wf-7a1","crc":"a3f1c208"}
WorkflowState — the reconstructed machine
Split into two pieces so recovery is a pure fold:

StepRuntime:
    step_id, state (StepState), attempt, compensation_attempt,
    idempotency_key, result, last_error, completion_lsn, uncertain_reason

WorkflowSnapshot:
    workflow_id, epoch, status (WorkflowState), last_lsn,
    steps: dict[str, StepRuntime],
    completion_order: list[str],       # by ascending completion_lsn — defines inverse rollback order
    dead_letter: list[DeadLetterEntry] # populated on poison-pill compensations
replay.fold(records) -> WorkflowSnapshot is pure and total: no clocks, no randomness, no I/O. Same byte stream in ⇒ identical snapshot out. This is the single source of truth for recovery, for the CLI inspector, and for tests.

3. State Machine
Step-level
stateDiagram-v2
    [*] --> PENDING
    PENDING --> RUNNING: deps COMPLETED, STEP_STARTED fsync'd
    PENDING --> SKIPPED: workflow failed before this step ran
    RUNNING --> COMPLETED: handler returned
    RUNNING --> FAILED: handler raised, retries exhausted
    RUNNING --> RUNNING: TransientError, attempt++
    RUNNING --> CANCELLED: sibling branch failed
    RUNNING --> UNCERTAIN: crash / timeout / cancel with in-flight effect
    CANCELLED --> UNCERTAIN: always (effect may have landed)
    UNCERTAIN --> COMPLETED: probe FOUND
    UNCERTAIN --> RUNNING: probe NOT_FOUND, replay with SAME key
    UNCERTAIN --> COMPENSATING: probe UNKNOWN, compensate defensively
    COMPLETED --> COMPENSATING: rollback reached this step
    COMPENSATING --> COMPENSATED: compensator returned
    COMPENSATING --> COMPENSATING: TransientError, comp_attempt++
    COMPENSATING --> COMPENSATION_FAILED: PermanentError or budget exhausted
    COMPLETED --> [*]
    COMPENSATED --> [*]
    FAILED --> [*]
    SKIPPED --> [*]
    COMPENSATION_FAILED --> [*]
Workflow-level
stateDiagram-v2
    [*] --> PENDING
    PENDING --> RUNNING
    RUNNING --> COMPLETED: all steps COMPLETED
    RUNNING --> CANCELLING: a step FAILED, siblings in flight
    CANCELLING --> COMPENSATING: all in-flight tasks settled
    RUNNING --> COMPENSATING: a step FAILED, nothing else in flight
    COMPENSATING --> COMPENSATED: every rollback succeeded
    COMPENSATING --> DEAD_LETTER: >=1 COMPENSATION_FAILED
    COMPLETED --> [*]
    COMPENSATED --> [*]
    DEAD_LETTER --> [*]
Enforcement
states.py holds LEGAL_STEP_TRANSITIONS: dict[StepState, frozenset[StepState]] and the workflow equivalent. Every mutation routes through snapshot.transition(step_id, new_state), which raises IllegalTransition on a violation. Two consequences worth stating: illegal transitions are caught in replay.fold too, so a corrupt or hand-edited journal fails loudly at recovery instead of silently producing a wrong machine; and test_states.py can assert the full negative space (every pair not in the table raises).

The write-ahead ordering rule, stated once and applied everywhere:

journal.append(record) → os.write → os.fsync(fd) → only then mutate the in-memory snapshot → only then await the handler.

There is exactly one code path that invokes a user handler, and it is unreachable except through a completed fsync.

4. Edge Case Strategy
4.1 Zombie Steps — partial ACK
Mechanism. Before any handler runs, STEP_STARTED — carrying the idempotency key — is fsync'd. So a crash leaves one of exactly two shapes on disk: an intent with no terminal record (zombie candidate), or an intent plus a terminal record (settled). There is no third shape, and no shape where a side effect can have occurred with nothing on disk about it.

Resolution ladder at recovery, per zombie candidate:

probe() declared → call it with the recorded key.
FOUND(result) → journal STEP_PROBE_RESOLVED then STEP_COMPLETED; the step is not re-run.
NOT_FOUND → re-invoke the handler with the identical idempotency_key.
UNKNOWN (probe itself failing/ambiguous, after its own bounded retries) → journal STEP_UNCERTAIN; the step is treated as possibly completed and routed to COMPENSATING if compensate_on_uncertain is set, or straight to DEAD_LETTER if the step has no compensator.
No probe() → replay directly with the identical key. Semantics are at-least-once delivery + a stable dedup key, which is exactly what the remote side needs to make it effectively-once.
The invariant that makes replay safe — and the single most important rule in this design:

attempt increments only in response to a journaled STEP_FAILED. It never increments because of a crash, a cancellation, or a recovery.

Therefore wf:step:attempt is a pure function of the journal prefix. A crashed-and-replayed invocation reuses byte-identical key wf-7a1:charge_card:1; the remote's dedup table hits and returns the original result. Had the attempt counter advanced on recovery, the retry would present a fresh key and the double-charge this whole design exists to prevent would happen on the very first crash.

4.2 Poison Pill Compensations
Bounded, classified, and terminal. Compensation retries draw on a compensation_retry budget entirely separate from the forward one. Every failure passes through a classifier (retry.classify, user-overridable): PermanentError — the raised type, or an HTTP 4xx surfaced by the handler — retries zero times. TransientError retries with exponential backoff, capped by both an attempt count (default 3) and a wall-clock budget. There is no unbounded path; the loop condition is attempt < max AND elapsed < budget AND classification is TRANSIENT.

On exhaustion: journal COMPENSATION_FAILED → the step enters terminal COMPENSATION_FAILED → the workflow is marked DEAD_LETTER.

Rollback does not simply halt. Halting on the first poison pill strands every not-yet-compensated step as an additional silent leak. Instead the engine applies a dependency-scoped quarantine: it stops compensating the ancestors of the failed step (rolling back a parent whose child's rollback failed can be genuinely unsafe — the child may still hold a reference to the parent's resource), while independent branches continue to roll back normally. Every step skipped by the quarantine is journaled STEP_SKIPPED with reason quarantined_by=<step_id> so the manifest can distinguish "we chose not to" from "we forgot."

InterventionManifest is written to <journal>.manifest.json alongside a WORKFLOW_DEAD_LETTER record pointing at it. It contains: workflow id and epoch; the LSN range of the incident; for each failed compensation the step id, idempotency key, attempts made, and the final error with status code and response body; the surviving result payloads of failed/quarantined steps (this is the orphaned resource inventory an operator actually needs — booking ids, charge ids); the quarantine list with causes; and the exact saga inspect command to reproduce the analysis. The manifest is a hand-off document, which is precisely why the journal is human-readable JSONL.

4.3 Partial DAG Rollbacks
Forward execution spawns one task per node inside a single asyncio.TaskGroup. Each task first awaits its parents' completion events, then runs. When a step exhausts its retries and raises, the TaskGroup's native semantics cancel every sibling — this is the required behavior, obtained from the language rather than hand-rolled.

Four details make that safe:

Cancellation is journaled, not swallowed. Each step wrapper catches asyncio.CancelledError, writes STEP_CANCELLED, and re-raises. Swallowing it would corrupt the event loop's cancellation protocol.
A cancelled in-flight step is a zombie by construction. Its STEP_STARTED is already on disk and its request may already have landed remotely. So CANCELLED transitions unconditionally to UNCERTAIN and enters §4.1's exact ladder — one mechanism serves both the crash case and the cancel case.
Bounded unwinding. Cancellation is requested, then the engine waits cancel_grace_s for the task to unwind. A handler that ignores cancellation past the grace period is abandoned and left UNCERTAIN. Steps declared cancellable=False are asyncio.shield-ed and allowed to finish, then compensated normally — the escape hatch for a non-interruptible critical section.
Compensation cannot be cancelled by the storm that triggered it. The TaskGroup is allowed to fully unwind first (the except* handler collects the exception group); rollback then runs in a fresh, shielded task outside that group. Otherwise the in-flight cancellation would kill the rollback that the cancellation itself demanded.
Ordering. Rollback default is strictly serial, in reverse completion_order (descending completion LSN) — deterministic, reconstructible from the journal alone, and the literal reading of "inverse-order compensation." Steps PENDING when the failure hit are journaled STEP_SKIPPED and never compensated (nothing happened, so there is nothing to undo). An opt-in parallel_compensation=True relaxes this to reverse-topological levels, compensating mutually-independent steps concurrently; it is off by default because the project's guarantee is determinism, not rollback throughput.

5. Determinism Rules (cross-cutting)
These are asserted by tests, not just documented:

Idempotency keys derive only from (workflow_id, step_id, attempt). No clock, no UUID, no hostname.
workflow_id is caller-supplied or derived from a caller-supplied seed — never uuid4() — so a workflow is re-runnable byte-identically.
The ready-set is drained in sorted(step_id) order, so task spawn order is reproducible even though completion order is not.
ts is written for humans and is never read by any logic. replay.fold never touches it.
JSON is canonicalized (sort_keys, tight separators) before CRC, so the CRC is stable across writers.
LSNs are gap-free; a gap at recovery is treated as corruption, not as a skippable hole.
6. Phased Implementation
Phase 1 — Foundations (no I/O). states.py, models.py, errors.py, idempotency.py, dag.py. Transition tables, illegal-transition guard, cycle detection, dependency closure, reverse-topological ordering. Tests: test_dag.py, test_states.py. Exit criterion: the state machine is enforceable and the DAG validator rejects cycles, dangling depends_on, and duplicate step ids.

Phase 2 — The durable journal. records.py, journal.py, replay.py, lockfile.py. Append-with-fsync writer, CRC-verifying reader, torn-final-line detection and truncation, epoch assignment, single-writer lock. replay.fold as a pure function. Tests: test_journal.py (bit-flip corruption, truncated last line, byte-level fsync-before-return assertion via a monkeypatched os.fsync recorder), test_replay.py. Exit criterion: any prefix of a journal — including a byte-level partial one — folds to a valid snapshot or a clean, explicit error.

Phase 3 — Forward engine. retry.py, engine.py forward path only. TaskGroup scheduling, dependency events, retry/backoff, timeouts, WAL ordering at every transition. Tests: test_engine_forward.py. Exit criterion: a diamond DAG runs its independent branches concurrently and produces a journal whose fold matches the live in-memory snapshot exactly.

Phase 4 — Compensation, cancellation, poison pills. Reverse-order rollback, sibling cancellation with grace period and shielding, cancellable=False, dependency-scoped quarantine, manifest.py, DEAD_LETTER. Tests: test_compensation.py, test_cancellation.py, test_poison_pill.py. Exit criterion: branch B failing while branch A is mid-flight cancels A, marks it UNCERTAIN, and rolls back in exact inverse completion order; a permanently-failing compensator terminates in bounded time with a manifest naming every orphaned resource.

Phase 5 — Recovery, zombies, CLI. recovery.py (recover(journal_file)), the probe/replay ladder, resume-forward and resume-rollback paths, cli.py. Tests: test_zombie.py, test_crash_sweep.py. Exit criterion: the crash sweep (below) passes for every LSN.

7. Verification
Crash-injection sweep — the load-bearing test. SAGA_CRASH_AT_LSN=<n> makes the journal writer call os._exit(1) immediately after the fsync of LSN n. The test runs the example saga in a subprocess for n = 1..max_lsn, then calls recover(journal) in the parent and asserts, for every single n:

No double execution. Handlers append to a side-ledger file outside the journal. Every (step_id, idempotency_key) pair may appear more than once (replay is legal) but every distinct key appears for at most one logical attempt, and no step ever observes two different keys for the same attempt number.
No orphans. On any failure path, every step that reached COMPLETED reaches COMPENSATED, COMPENSATION_FAILED, or an explicit quarantined STEP_SKIPPED — never nothing.
Terminality. Recovery reaches COMPLETED, COMPENSATED, or DEAD_LETTER. It never hangs and never loops.
Fold agreement. The post-recovery in-memory snapshot equals replay.fold(all_records).
Inverse order. Compensation LSN order is the exact reverse of completion LSN order.
Because the whole engine is a fold over an append-only log, this sweep is exhaustive over crash points rather than a sample — that is the payoff for keeping replay.fold pure.

Supporting verification:

python -m pytest -q — full suite; pytest --timeout=30 to catch any infinite compensation loop as a failure rather than a hang.
python examples/booking_saga.py — hotel booked, flight booked, car rental fails ⇒ flight then hotel compensated in that order; journal readable by eye.
python examples/poison_pill.py — compensator returns HTTP 400 ⇒ DEAD_LETTER in bounded time, manifest written.
saga verify <journal> on a deliberately bit-flipped file ⇒ names the offending LSN.
saga inspect <journal> after a mid-run Ctrl-C ⇒ shows the UNCERTAIN step and the key that needs probing.
saga graph <journal> ⇒ mermaid DAG annotated with each node's final state.
8. Open Items (defaults chosen, easy to revisit)
max_compensation_attempts=3, cancel_grace_s=5.0, forward backoff 0.1 * 2**attempt capped at 30s.
Journal is single-workflow-per-file. Multi-workflow journals would need a workflow_id index at recovery; not in scope.
payload result snapshots are assumed JSON-serializable. A non-serializable return raises at journal time — loudly, before the fsync — rather than being silently dropped.