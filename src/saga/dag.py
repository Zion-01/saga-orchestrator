"""DAG validation and ordering helpers.

Ordering is deterministic everywhere: ties are broken by ``sorted(step_id)`` so
that two runs of the same spec schedule tasks in the same order.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from .errors import DagError


def validate(depends_on: Mapping[str, Iterable[str]]) -> None:
    """Reject dangling dependencies, self-edges, and cycles."""
    ids = set(depends_on)
    for step_id, deps in depends_on.items():
        for dep in deps:
            if dep == step_id:
                raise DagError(f"step {step_id!r} depends on itself")
            if dep not in ids:
                raise DagError(f"step {step_id!r} depends on unknown step {dep!r}")
    _detect_cycle(depends_on)


def _detect_cycle(depends_on: Mapping[str, Iterable[str]]) -> None:
    WHITE, GREY, BLACK = 0, 1, 2
    colour = dict.fromkeys(depends_on, WHITE)

    def visit(node: str, path: list[str]) -> None:
        colour[node] = GREY
        for dep in sorted(depends_on[node]):
            if colour[dep] is GREY:
                cycle = path[path.index(dep) :] if dep in path else [dep]
                raise DagError("cycle detected: " + " -> ".join([*cycle, dep]))
            if colour[dep] is WHITE:
                visit(dep, [*path, dep])
        colour[node] = BLACK

    for node in sorted(depends_on):
        if colour[node] is WHITE:
            visit(node, [node])


def topological_levels(depends_on: Mapping[str, Iterable[str]]) -> list[list[str]]:
    """Group step ids into dependency levels; level *n* depends only on < *n*."""
    remaining = {k: set(v) for k, v in depends_on.items()}
    levels: list[list[str]] = []
    done: set[str] = set()
    while remaining:
        ready = sorted(k for k, deps in remaining.items() if deps <= done)
        if not ready:  # pragma: no cover - validate() rules this out
            raise DagError("cycle detected while levelling")
        levels.append(ready)
        done.update(ready)
        for k in ready:
            del remaining[k]
    return levels


def reverse_topological_levels(depends_on: Mapping[str, Iterable[str]]) -> list[list[str]]:
    """Levels for rollback: dependents are torn down before their dependencies."""
    return list(reversed(topological_levels(depends_on)))


def ancestors(depends_on: Mapping[str, Iterable[str]], step_id: str) -> set[str]:
    """Transitive dependencies of ``step_id`` (the steps it was built on top of)."""
    seen: set[str] = set()
    stack = list(depends_on.get(step_id, ()))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(depends_on.get(node, ()))
    return seen


def descendants(depends_on: Mapping[str, Iterable[str]], step_id: str) -> set[str]:
    """Transitive dependents of ``step_id``."""
    children: dict[str, set[str]] = {k: set() for k in depends_on}
    for node, deps in depends_on.items():
        for dep in deps:
            children[dep].add(node)
    seen: set[str] = set()
    stack = list(children.get(step_id, ()))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(children.get(node, ()))
    return seen
