"""Tests for the SHA-256 keyed, reusable prediction parse index.

Covers the guarantees requested for the cached-parse workflow:

* identical bytes are parsed once and then reused, different bytes never
  collide;
* a structurally invalid file raises and leaves no index behind, so it can
  never participate in a backtest and the next attempt rebuilds;
* a parse that contains row-level errors is still cached as a *ready* index
  (the parse itself succeeded) and surfaces the same errors through the cache;
* indices survive a fresh process (persistence in the SQLite file);
* concurrent first uploads of the same bytes from threads **and** processes
  produce exactly one ready index;
* the indexed evaluation path is equivalent to the one-shot ``run_backtest``.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import os
import tempfile

import pytest

_TMP = tempfile.mkdtemp(prefix="prediction-index-tests-")
os.environ["BACKTEST_DB_PATH"] = os.path.join(_TMP, "backtest.db")
os.environ["BACKTEST_INDEX_DB_PATH"] = os.path.join(_TMP, "prediction-index.db")
# Keep the cross-process test fast while still exercising lock waiting.
os.environ.setdefault("BACKTEST_INDEX_BUSY_TIMEOUT", "30")

from app import prediction_index  # noqa: E402
from app.backtest import (  # noqa: E402
    ParsedPredictions,
    ValidationError,
    evaluate_backtest,
    parse_actuals,
    parse_cutoff,
    parse_predictions,
    run_backtest,
)

PRED_HEADER = "series_id,issue_time,target_time,lower,upper\n"
ACT_HEADER = "series_id,target_time,actual\n"
CUTOFF = "2026-11-20T12:00:00Z"


def pred(*rows: str) -> bytes:
    return (PRED_HEADER + "\n".join(rows) + "\n").encode()


def act(*rows: str) -> bytes:
    return (ACT_HEADER + "\n".join(rows) + "\n").encode()


VALID_PRED = pred(
    "a,2026-11-20T10:00:00Z,2026-11-20T13:00:00Z,1,2",
    "a,2026-11-20T11:30:00Z,2026-11-20T13:00:00Z,1.5,2.5",
    "a,2026-11-20T12:30:00Z,2026-11-20T13:00:00Z,9,10",
)
OTHER_PRED = pred(
    "b,2026-11-20T10:00:00Z,2026-11-20T13:00:00Z,3,4",
)
ACTUALS = act("a,2026-11-20T13:00:00Z,2.0")


@pytest.fixture(autouse=True)
def _clean_index_databases():
    """Each test starts with an empty on-disk index database."""
    prediction_index.reset_for_tests()
    for suffix in ("", "-wal", "-shm"):
        path = prediction_index.INDEX_DB_PATH + suffix
        if os.path.exists(path):
            os.remove(path)
    yield
    prediction_index.reset_for_tests()


# ----------------------------------------------------------------- basics


def test_sha256_keys_complete_bytes():
    digest = prediction_index.hash_content(VALID_PRED)
    assert digest == hashlib.sha256(VALID_PRED).hexdigest()
    assert len(digest) == 64
    # A one-byte change must produce a different key.
    changed = VALID_PRED.replace(b"1,2", b"1,3", 1)
    assert prediction_index.hash_content(changed) != digest


def test_first_build_then_reuse_returns_identical_parse():
    first = prediction_index.get_or_build(VALID_PRED)
    assert first.reused is False
    assert first.content_sha256 == hashlib.sha256(VALID_PRED).hexdigest()

    direct = parse_predictions(VALID_PRED)
    assert first.predictions.rows == direct.rows
    assert first.predictions.errors == direct.errors
    assert first.predictions.row_count == direct.row_count

    second = prediction_index.get_or_build(VALID_PRED)
    assert second.reused is True
    assert second.predictions == first.predictions

    status = prediction_index.get_status(first.content_sha256)
    assert status is not None
    assert status.status == "ready"
    assert status.parsed_rows == len(direct.rows)
    assert status.error_count == 0
    assert status.byte_size == len(VALID_PRED)


def test_different_content_never_misses_into_another_index():
    one = prediction_index.get_or_build(VALID_PRED)
    two = prediction_index.get_or_build(OTHER_PRED)
    assert one.content_sha256 != two.content_sha256
    assert one.reused is False and two.reused is False

    listing = {item.content_sha256: item for item in prediction_index.list_status()}
    assert set(listing) == {one.content_sha256, two.content_sha256}
    assert all(item.status == "ready" for item in listing.values())

    # Re-uploading either file hits its own index, never the other one.
    assert prediction_index.get_or_build(VALID_PRED).reused is True
    assert prediction_index.get_or_build(OTHER_PRED).reused is True


def test_row_level_errors_are_cached_and_reproduced():
    duplicate = "a,2026-11-20T10:00:00Z,2026-11-20T13:00:00Z,1,2"
    content = pred(duplicate, duplicate)
    outcome = prediction_index.get_or_build(content)
    assert outcome.reused is False
    assert outcome.predictions.errors and "duplicate prediction" in (
        outcome.predictions.errors[0]
    )

    cached = prediction_index.get_or_build(content)
    assert cached.reused is True
    assert cached.predictions.errors == outcome.predictions.errors

    # The error still rejects the batch through the evaluation phase.
    with pytest.raises(ValidationError) as excinfo:
        evaluate_backtest(
            cutoff=parse_cutoff(CUTOFF),
            predictions=cached.predictions,
            actuals=parse_actuals(ACTUALS),
        )
    assert "duplicate prediction" in str(excinfo.value)


def test_structural_failure_leaves_no_index_and_retries_cleanly():
    missing_column = b"series_id,issue_time,target_time,lower\n" + VALID_PRED
    with pytest.raises(ValidationError):
        prediction_index.get_or_build(missing_column)

    digest = prediction_index.hash_content(missing_column)
    assert prediction_index.get_status(digest) is None
    assert prediction_index.list_status() == []

    # A later valid upload must not be affected by the rolled-back failure.
    outcome = prediction_index.get_or_build(VALID_PRED)
    assert outcome.reused is False
    assert prediction_index.get_status(outcome.content_sha256).status == "ready"


def test_invalid_encoding_failure_leaves_no_index():
    content = PRED_HEADER.encode() + b"a,2026-11-20T10:00:00Z,\xff\xfe,1,2\n"
    with pytest.raises(ValidationError):
        prediction_index.get_or_build(content)
    assert prediction_index.list_status() == []


# ------------------------------------------------------------ persistence


def test_index_survives_a_new_process_connection():
    outcome = prediction_index.get_or_build(VALID_PRED)
    # Simulate a process restart by dropping every in-memory connection and
    # reopening the same database file from scratch.
    fresh_outcome = _build_in_subprocess(VALID_PRED)
    assert fresh_outcome["reused"] is True
    assert fresh_outcome["digest"] == outcome.content_sha256
    assert fresh_outcome["parsed_rows"] == len(outcome.predictions.rows)


# ------------------------------------------------------------ concurrency


def test_concurrent_threads_build_exactly_one_index():
    results: list[prediction_index.IndexOutcome] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            results.append(prediction_index.get_or_build(VALID_PRED))
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [
        __import__("threading").Thread(target=worker) for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(results) == 8
    assert sum(1 for item in results if item.reused is False) == 1
    assert sum(1 for item in results if item.reused is True) == 7
    digests = {item.content_sha256 for item in results}
    assert digests == {hashlib.sha256(VALID_PRED).hexdigest()}
    assert len(prediction_index.list_status()) == 1


def test_concurrent_processes_build_exactly_one_index():
    context = multiprocessing.get_context("fork")
    with context.Pool(processes=4) as pool:
        outcomes = pool.map(_pool_worker, [VALID_PRED] * 8)

    builds = [item for item in outcomes if not item["reused"]]
    reuses = [item for item in outcomes if item["reused"]]
    assert len(builds) == 1
    assert len(reuses) == 7
    assert {item["digest"] for item in outcomes} == {
        hashlib.sha256(VALID_PRED).hexdigest()
    }
    assert {item["parsed_rows"] for item in outcomes} == {
        len(parse_predictions(VALID_PRED).rows)
    }
    assert len(prediction_index.list_status()) == 1


def test_concurrent_processes_with_different_files_keep_separate_indices():
    context = multiprocessing.get_context("fork")
    payloads = [VALID_PRED, OTHER_PRED] * 4
    with context.Pool(processes=4) as pool:
        outcomes = pool.map(_pool_worker, payloads)

    digests = {item["digest"] for item in outcomes}
    assert digests == {
        hashlib.sha256(VALID_PRED).hexdigest(),
        hashlib.sha256(OTHER_PRED).hexdigest(),
    }
    statuses = {item.content_sha256: item for item in prediction_index.list_status()}
    assert len(statuses) == 2
    assert all(item.status == "ready" for item in statuses.values())


# ------------------------------------------------------------ equivalence


def test_indexed_path_matches_one_shot_run_backtest():
    outcome = prediction_index.get_or_build(VALID_PRED)
    indexed = evaluate_backtest(
        cutoff=parse_cutoff(CUTOFF),
        predictions=outcome.predictions,
        actuals=parse_actuals(ACTUALS),
    )
    one_shot = run_backtest(
        prediction_csv=VALID_PRED,
        actual_csv=ACTUALS,
        cutoff_raw=CUTOFF,
    )
    assert indexed == one_shot

    # And again, this time served entirely from the cached index.
    reused = prediction_index.get_or_build(VALID_PRED)
    assert reused.reused is True
    second = evaluate_backtest(
        cutoff=parse_cutoff(CUTOFF),
        predictions=reused.predictions,
        actuals=parse_actuals(ACTUALS),
    )
    assert second == one_shot


def test_indexed_path_rejects_batch_identically_to_one_shot():
    # Every vintage is issued after the cutoff, so the atomic completeness
    # check rejects the whole batch in both code paths with identical errors.
    late_pred = pred(
        "a,2026-11-20T12:30:00Z,2026-11-20T13:00:00Z,1,2",
    )
    outcome = prediction_index.get_or_build(late_pred)
    with pytest.raises(ValidationError) as cached_exc:
        evaluate_backtest(
            cutoff=parse_cutoff(CUTOFF),
            predictions=outcome.predictions,
            actuals=parse_actuals(ACTUALS),
        )
    with pytest.raises(ValidationError) as direct_exc:
        run_backtest(
            prediction_csv=late_pred,
            actual_csv=ACTUALS,
            cutoff_raw=CUTOFF,
        )
    assert cached_exc.value.errors == direct_exc.value.errors


def test_empty_index_database_reports_gracefully():
    assert prediction_index.list_status() == []
    assert prediction_index.get_status("deadbeef") is None


def test_deleted_database_file_is_rebuilt_and_reused():
    first = prediction_index.get_or_build(VALID_PRED)
    assert first.reused is False

    # Simulate the cache being wiped while the process keeps running.
    for suffix in ("", "-wal", "-shm"):
        path = prediction_index.INDEX_DB_PATH + suffix
        if os.path.exists(path):
            os.remove(path)

    # The next upload must transparently bootstrap a fresh database and
    # rebuild the index instead of failing on the stale in-process state.
    rebuilt = prediction_index.get_or_build(VALID_PRED)
    assert rebuilt.reused is False
    assert rebuilt.content_sha256 == first.content_sha256
    assert rebuilt.predictions == first.predictions

    # And the following upload is served from the new index.
    assert prediction_index.get_or_build(VALID_PRED).reused is True


# ------------------------------------------------------------ subprocess


def _pool_worker(content: bytes) -> dict:
    """Top-level callable for process-pool workers (forked)."""
    outcome = prediction_index.get_or_build(content)
    return {
        "digest": outcome.content_sha256,
        "reused": outcome.reused,
        "parsed_rows": len(outcome.predictions.rows),
    }


def _build_in_subprocess(content: bytes) -> dict:
    """Run ``get_or_build`` in a forked process sharing only the index DB."""
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    process = context.Process(
        target=_subprocess_worker,
        args=(prediction_index.INDEX_DB_PATH, content, queue),
    )
    process.start()
    process.join()
    if process.exitcode != 0:  # pragma: no cover - surfaces worker failures
        raise AssertionError(f"subprocess failed with exit code {process.exitcode}")
    return queue.get()


def _subprocess_worker(db_path: str, content: bytes, queue) -> None:
    # Forked process: inherited in-memory state is irrelevant because the
    # index is read from the shared SQLite file, exercising real persistence.
    from app import prediction_index as module

    module.INDEX_DB_PATH = db_path
    outcome = module.get_or_build(content)
    queue.put(
        {
            "digest": outcome.content_sha256,
            "reused": outcome.reused,
            "parsed_rows": len(outcome.predictions.rows),
        }
    )
