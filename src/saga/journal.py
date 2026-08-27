"""The durable journal: an append+fsync writer and a torn-tail-tolerant reader.

Write-ahead rule, enforced here and nowhere else: :meth:`Journal.append`
completes -- ``os.write`` then ``os.fsync`` -- before it returns a record to
the caller. Nothing about a step's handler is reachable except through a
completed fsync; that ordering is this module's entire reason to exist.

Reads are pure and tolerant of exactly one thing: an incomplete final line,
left by a crash mid-write. Anything else that fails CRC or JSON parsing is
*not* tolerated -- :func:`read_journal` raises :class:`JournalCorruption`,
because that is real corruption, not a crash artifact.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .lockfile import FileLock
from .records import JournalRecord, RecordType

#: O_BINARY only exists on Windows; without it os.open() would open the file
#: in text mode there and silently translate "\n" -> "\r\n" on write, which
#: would corrupt the byte-for-byte framing the reader relies on.
_O_BINARY = getattr(os, "O_BINARY", 0)
_WRITE_FLAGS = os.O_APPEND | os.O_CREAT | os.O_WRONLY | _O_BINARY


@dataclass(slots=True)
class ReadResult:
    records: list[JournalRecord]
    torn_tail_bytes: int  # 0 if the file ended on a complete line (or is empty)
    valid_byte_length: int  # length of the file's clean, complete-lines prefix


def read_journal(path: Path) -> ReadResult:
    """Parse ``path`` into complete, CRC-verified records.

    Never mutates the file. If the file does not end with a newline, the
    trailing partial line is treated as a torn write and excluded from
    ``records`` rather than raising -- but every complete line (every line
    that *does* end with ``\\n``) must decode and CRC-verify cleanly, torn
    tail or not.
    """
    if not path.exists():
        return ReadResult(records=[], torn_tail_bytes=0, valid_byte_length=0)

    raw = path.read_bytes()
    if not raw:
        return ReadResult(records=[], torn_tail_bytes=0, valid_byte_length=0)

    body = raw
    torn_tail_bytes = 0
    if not raw.endswith(b"\n"):
        body = raw[: raw.rfind(b"\n") + 1]  # -1 -> rfind gives 0 -> body == b""
        torn_tail_bytes = len(raw) - len(body)

    records = [JournalRecord.decode(line.decode("utf-8")) for line in body.split(b"\n")[:-1]]
    return ReadResult(records=records, torn_tail_bytes=torn_tail_bytes, valid_byte_length=len(body))


@dataclass(slots=True)
class Journal:
    """A single-writer append handle for one journal file.

    Construct via :meth:`open_for_write`, not directly. Use as a context
    manager so the lock is always released:

        with Journal.open_for_write(path) as journal:
            journal.append(type=RecordType.WORKFLOW_STARTED, ...)
    """

    path: Path
    epoch: int
    _fd: int
    _lock: FileLock
    _next_lsn: int
    _closed: bool = field(default=False, init=False)

    @classmethod
    def open_for_write(cls, path: Path) -> Journal:
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = FileLock.acquire(path)
        try:
            result = read_journal(path)
            if result.torn_tail_bytes:
                _truncate(path, result.valid_byte_length)
            last = result.records[-1] if result.records else None
            fd = os.open(str(path), _WRITE_FLAGS, 0o644)
        except BaseException:
            lock.release()
            raise
        return cls(
            path=path,
            epoch=(last.epoch if last else 0) + 1,
            _fd=fd,
            _lock=lock,
            _next_lsn=(last.lsn if last else 0) + 1,
        )

    @property
    def next_lsn(self) -> int:
        return self._next_lsn

    def append(
        self,
        *,
        type: RecordType,
        workflow_id: str,
        step_id: str | None = None,
        attempt: int | None = None,
        idempotency_key: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> JournalRecord:
        record = JournalRecord(
            lsn=self._next_lsn,
            epoch=self.epoch,
            ts=time.time(),
            type=type,
            workflow_id=workflow_id,
            step_id=step_id,
            attempt=attempt,
            idempotency_key=idempotency_key,
            payload=payload or {},
        )
        line = (record.encode() + "\n").encode("utf-8")  # encode() raises before any I/O below
        os.write(self._fd, line)
        os.fsync(self._fd)
        self._next_lsn += 1
        return record

    def close(self) -> None:
        if self._closed:
            return
        os.close(self._fd)
        self._lock.release()
        self._closed = True

    def __enter__(self) -> Journal:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _truncate(path: Path, length: int) -> None:
    with open(path, "r+b") as f:
        f.truncate(length)
        f.flush()
        os.fsync(f.fileno())
