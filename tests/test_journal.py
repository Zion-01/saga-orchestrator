"""Phase 2 exit criterion: any prefix of a journal -- including a byte-level
partial one -- folds to a valid snapshot or a clean, explicit error.

This file covers the on-disk layer: canonical encode/decode and CRC framing
(``records.py``), the append+fsync writer with torn-tail repair and the
single-writer lock (``journal.py`` / ``lockfile.py``). ``test_replay.py``
covers the pure state-machine fold on top of records these tests produce.
"""

from __future__ import annotations

import json
import os

import pytest

import saga.journal as journal_mod
from saga.errors import JournalCorruption, JournalLocked
from saga.journal import Journal, read_journal
from saga.lockfile import FileLock, break_lock
from saga.records import JournalRecord, RecordType

WF = "wf-1"


def _record(**overrides) -> JournalRecord:
    fields = dict(
        lsn=1,
        epoch=1,
        ts=1756000000.0,
        type=RecordType.STEP_STARTED,
        workflow_id=WF,
        step_id="charge_card",
        attempt=1,
        idempotency_key="wf-1:charge_card:1",
        payload={},
    )
    fields.update(overrides)
    return JournalRecord(**fields)


# --- JournalRecord: canonical encode / decode / CRC -------------------------


def test_encode_is_canonical_json_with_sorted_keys_and_tight_separators() -> None:
    line = _record().encode()
    assert ", " not in line
    assert ": " not in line
    obj = json.loads(line)
    assert list(obj.keys()) == sorted(obj.keys())


def test_decode_round_trips_an_encoded_record() -> None:
    record = _record(lsn=41, epoch=2, type=RecordType.STEP_COMPLETED, payload={"result": 7})
    assert JournalRecord.decode(record.encode()) == record


def test_decode_rejects_malformed_json() -> None:
    with pytest.raises(JournalCorruption):
        JournalRecord.decode("not json at all")


def test_decode_rejects_missing_required_field() -> None:
    with pytest.raises(JournalCorruption):
        JournalRecord.decode('{"lsn":1,"epoch":1,"ts":1.0,"type":"WORKFLOW_STARTED","workflow_id":"wf-1"}')


def test_decode_rejects_crc_tampering() -> None:
    obj = json.loads(_record().encode())
    obj["payload"] = {"tampered": True}  # mutate a field, leave the crc as-is
    tampered = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    with pytest.raises(JournalCorruption):
        JournalRecord.decode(tampered)


def test_encode_raises_before_any_write_on_unserializable_payload() -> None:
    with pytest.raises(TypeError):
        _record(payload={"bad": object()}).encode()


# --- Journal: append + fsync ordering ----------------------------------------


def test_append_assigns_gap_free_ascending_lsns(journal_path) -> None:
    with Journal.open_for_write(journal_path) as journal:
        r1 = journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})
        r2 = journal.append(
            type=RecordType.STEP_STARTED, workflow_id=WF, step_id="a", attempt=1, idempotency_key="wf-1:a:1"
        )
    assert (r1.lsn, r2.lsn) == (1, 2)


def test_append_fsyncs_after_write_before_returning(journal_path, monkeypatch) -> None:
    calls: list[str] = []
    real_write = os.write
    real_fsync = os.fsync

    def recording_write(fd, data):
        calls.append("write")
        return real_write(fd, data)

    def recording_fsync(fd):
        calls.append("fsync")
        return real_fsync(fd)

    monkeypatch.setattr(journal_mod.os, "write", recording_write)
    monkeypatch.setattr(journal_mod.os, "fsync", recording_fsync)

    with Journal.open_for_write(journal_path) as journal:
        calls.clear()
        journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})

    assert calls == ["write", "fsync"]


def test_append_raises_before_writing_on_unserializable_payload(journal_path) -> None:
    with Journal.open_for_write(journal_path) as journal:
        with pytest.raises(TypeError):
            journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"bad": object()})
    assert read_journal(journal_path).records == []


def test_journal_persists_lsn_and_bumps_epoch_across_reopen(journal_path) -> None:
    with Journal.open_for_write(journal_path) as journal:
        assert journal.epoch == 1
        journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})
        journal.append(
            type=RecordType.STEP_STARTED, workflow_id=WF, step_id="a", attempt=1, idempotency_key="wf-1:a:1"
        )

    with Journal.open_for_write(journal_path) as journal:
        assert journal.epoch == 2
        record = journal.append(type=RecordType.RECOVERY_STARTED, workflow_id=WF)
        assert (record.lsn, record.epoch) == (3, 2)


# --- read_journal: CRC corruption vs. torn-tail leniency ---------------------


def test_read_journal_on_missing_file_is_empty(journal_path) -> None:
    result = read_journal(journal_path)
    assert result.records == []
    assert result.torn_tail_bytes == 0


def test_read_journal_raises_on_mid_file_bit_flip(journal_path) -> None:
    with Journal.open_for_write(journal_path) as journal:
        journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})
        journal.append(
            type=RecordType.STEP_STARTED, workflow_id=WF, step_id="a", attempt=1, idempotency_key="wf-1:a:1"
        )
        journal.append(type=RecordType.STEP_COMPLETED, workflow_id=WF, step_id="a", payload={"result": 42})

    raw = bytearray(journal_path.read_bytes())
    raw[10] ^= 0x01  # flip one bit well inside the first line, far from EOF
    journal_path.write_bytes(bytes(raw))

    with pytest.raises(JournalCorruption):
        read_journal(journal_path)


def test_read_journal_tolerates_torn_final_line(journal_path) -> None:
    with Journal.open_for_write(journal_path) as journal:
        journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})
        journal.append(
            type=RecordType.STEP_STARTED, workflow_id=WF, step_id="a", attempt=1, idempotency_key="wf-1:a:1"
        )

    # Simulate a crash mid-write: a third line with no trailing newline.
    with open(journal_path, "ab") as f:
        f.write(b'{"attempt":1,"epoch":1,"idempotency_key":"wf-1:a:1","lsn":3,"payload":{},"step_i')

    result = read_journal(journal_path)
    assert [r.lsn for r in result.records] == [1, 2]
    assert result.torn_tail_bytes > 0


def test_open_for_write_repairs_a_torn_tail_before_resuming(journal_path) -> None:
    with Journal.open_for_write(journal_path) as journal:
        journal.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})

    with open(journal_path, "ab") as f:
        f.write(b'{"broken": tr')  # torn: no trailing newline

    with Journal.open_for_write(journal_path) as journal:
        record = journal.append(
            type=RecordType.STEP_STARTED, workflow_id=WF, step_id="a", attempt=1, idempotency_key="wf-1:a:1"
        )
        assert record.lsn == 2  # the torn line was discarded, not counted

    result = read_journal(journal_path)
    assert [r.lsn for r in result.records] == [1, 2]


# --- single-writer lock -------------------------------------------------------


def test_second_writer_is_locked_out(journal_path) -> None:
    with Journal.open_for_write(journal_path):
        with pytest.raises(JournalLocked):
            Journal.open_for_write(journal_path)


def test_lock_is_released_on_close(journal_path) -> None:
    journal = Journal.open_for_write(journal_path)
    journal.close()
    with Journal.open_for_write(journal_path) as journal2:
        journal2.append(type=RecordType.WORKFLOW_STARTED, workflow_id=WF, payload={"step_ids": ["a"]})


def test_break_lock_clears_an_orphaned_lock(journal_path) -> None:
    FileLock.acquire(journal_path)  # simulates a process that opened and then died
    with pytest.raises(JournalLocked):
        Journal.open_for_write(journal_path)

    break_lock(journal_path)

    with Journal.open_for_write(journal_path):
        pass  # succeeds now that the stale lock is gone
