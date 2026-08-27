"""Single-writer guard: at most one live :class:`~saga.journal.Journal` writer
per journal file.

The lock is a sibling ``<journal>.lock`` file created with the
``O_CREAT | O_EXCL`` exclusivity flag, which is atomic on every platform
Python targets, so no platform-specific file-locking API is needed. It
records the holding process id for diagnostics.

There is deliberately no automatic staleness detection. Probing whether a
pid is still alive is not portable enough to trust with "who owns the
journal" -- on Windows in particular, ``os.kill(pid, 0)`` does not mean
"check liveness" the way it does on POSIX, it sends a real signal. A lock
left behind by a process that died without releasing it must be cleared
explicitly, by an operator who has confirmed the process is gone, via
:func:`break_lock`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .errors import JournalLocked


def _lock_path(journal_path: Path) -> Path:
    return journal_path.with_name(journal_path.name + ".lock")


def _read_holder(lock_path: Path) -> int | None:
    try:
        text = lock_path.read_text(encoding="ascii").strip()
    except OSError:
        return None
    try:
        return int(text) if text else None
    except ValueError:
        return None


@dataclass(slots=True)
class FileLock:
    """A held lock. Call :meth:`release` (or use ``Journal`` as a context
    manager, which does this for you) when done with the journal.
    """

    path: Path
    _released: bool = False

    @classmethod
    def acquire(cls, journal_path: Path) -> FileLock:
        lock_path = _lock_path(journal_path)
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            holder = _read_holder(lock_path)
            detail = f" (held by pid {holder})" if holder is not None else ""
            raise JournalLocked(f"{journal_path} is locked{detail}") from None
        try:
            os.write(fd, str(os.getpid()).encode("ascii"))
        finally:
            os.close(fd)
        return cls(lock_path)

    def release(self) -> None:
        if self._released:
            return
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass
        self._released = True


def break_lock(journal_path: Path) -> None:
    """Manually clear a lock left by a process confirmed to be dead.

    Never called automatically -- see the module docstring.
    """
    try:
        os.remove(_lock_path(journal_path))
    except FileNotFoundError:
        pass
