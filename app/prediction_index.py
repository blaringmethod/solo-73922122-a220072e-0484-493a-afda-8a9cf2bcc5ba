"""Reusable SHA-256-keyed SQLite parse index for prediction CSVs.

Why a separate database
-----------------------
The backtest database (:mod:`app.storage`) is the durable audit record of
finished runs and must keep its schema stable. The prediction index is a
*disposable build cache*: it is safe to delete the whole file because every
index can be rebuilt from the original bytes, which are still stored per run.
A separate file also lets the two concerns use independent write locks.

Correctness rules
-----------------
1. The key is the SHA-256 of the prediction file's **complete byte content**,
   so different byte sequences can never hit the same index and identical
   bytes always hit the same one.
2. An index is published only when every row has been parsed and inserted in
   one transaction. If anything fails the transaction is rolled back: no
   partial index ever becomes visible ("building" rows are never reused).
3. Two processes uploading the same bytes for the first time serialise on a
   ``BEGIN IMMEDIATE`` transaction. The loser waits on the SQLite file lock,
   then finds the winner's ``ready`` row and reuses it -- exactly one usable
   index is produced.
4. A ``building`` row can only exist while its writer transaction is open.
   After a crash or ``kill -9`` the lock is released and SQLite rolls the
   transaction back, so on restart no stale ``building`` row is ever observed.
   As defence in depth, any orphaned ``building`` row left by an exotic
   journaling failure is treated as absent and rebuilt, never reused.
5. Indices built by a previous process/container are plain rows in the shared
   database file and keep working after restarts.

The module is process- and thread-safe: every operation opens its own
short-lived SQLite connection and relies on SQLite's file locking rather than
in-memory state.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, NamedTuple, TypeVar

from .backtest import ParsedPrediction, ParsedPredictions, parse_predictions

INDEX_DB_PATH = os.path.abspath(
    os.environ.get("BACKTEST_INDEX_DB_PATH", "data/prediction_index.db")
)

# Wait long enough for a concurrent first-upload build of a max-size file.
_BUSY_TIMEOUT_SECONDS = float(
    os.environ.get("BACKTEST_INDEX_BUSY_TIMEOUT", "60")
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS prediction_index (
    content_sha256  TEXT PRIMARY KEY,
    byte_size       INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('building', 'ready')),
    row_count       INTEGER NOT NULL,
    parsed_rows     INTEGER NOT NULL,
    error_count     INTEGER NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prediction_rows (
    content_sha256  TEXT NOT NULL,
    row_no          INTEGER NOT NULL,
    series_id       TEXT NOT NULL,
    issue_time      TEXT NOT NULL,
    target_time     TEXT NOT NULL,
    lower           REAL NOT NULL,
    upper           REAL NOT NULL,
    PRIMARY KEY (content_sha256, row_no)
);

CREATE TABLE IF NOT EXISTS prediction_parse_errors (
    content_sha256  TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    message         TEXT NOT NULL,
    PRIMARY KEY (content_sha256, seq)
);
"""


class IndexOutcome(NamedTuple):
    """Result of :func:`get_or_build` -- the parse plus cache bookkeeping."""

    predictions: ParsedPredictions
    content_sha256: str
    reused: bool
    byte_size: int


@dataclass(frozen=True)
class IndexStatus:
    content_sha256: str
    byte_size: int
    status: str
    row_count: int
    parsed_rows: int
    error_count: int
    created_at: str
    updated_at: str


def hash_content(content: bytes) -> str:
    """SHA-256 hex digest over the prediction file's complete bytes."""
    return hashlib.sha256(content).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# In-process serialisation keyed by digest. SQLite's BEGIN IMMEDIATE already
# serialises separate *processes*; these locks additionally make threads of the
# same process queue on a digest before opening a writer, so concurrent
# first-time uploads of one file never duplicate the parse work. Distinct
# digests use distinct locks and stay concurrent.
_digest_locks_guard = threading.Lock()
_digest_locks: dict[str, threading.Lock] = {}


def _lock_for(digest: str) -> threading.Lock:
    with _digest_locks_guard:
        lock = _digest_locks.get(digest)
        if lock is None:
            lock = threading.Lock()
            _digest_locks[digest] = lock
        return lock


_T = TypeVar("_T")


def _connect() -> sqlite3.Connection:
    directory = os.path.dirname(os.path.abspath(INDEX_DB_PATH))
    os.makedirs(directory, exist_ok=True)
    connection = sqlite3.connect(INDEX_DB_PATH, timeout=_BUSY_TIMEOUT_SECONDS)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = %d" % int(_BUSY_TIMEOUT_SECONDS * 1000))
    return connection


# Schema bootstrap lock. WAL mode is a persistent property of the database
# file, so it must be set only when the tables are genuinely missing, and in
# autocommit mode (the journal_mode pragma cannot run inside a transaction).
# Reissuing it on every connection would fail immediately with
# "database is locked" while another process holds the write lock, breaking
# concurrent first uploads. The schema is verified against the actual file
# rather than remembered in a process flag, so a deleted/recreated database
# (and a forked child that outlived it) bootstraps itself again.
_bootstrap_lock = threading.Lock()


def reset_for_tests() -> None:
    """Drop in-process digest locks (tests delete the DB file between cases)."""
    _digest_locks.clear()


def _ensure_bootstrapped() -> None:
    probe = _connect()
    try:
        if _has_schema(probe):
            return
    finally:
        probe.close()

    with _bootstrap_lock:
        deadline = time.monotonic() + _BUSY_TIMEOUT_SECONDS
        attempt = 0
        while True:
            connection = _connect()
            try:
                # Another thread/process may have bootstrapped while we waited.
                if _has_schema(connection):
                    return
                # Fresh database: switch to WAL first (persisted in the file),
                # then create every table in one script.
                connection.execute("PRAGMA journal_mode=WAL")
                connection.executescript(_SCHEMA)
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(min(0.2 * (2**attempt), 1.0))
                attempt += 1
            finally:
                connection.close()


def _with_write_lock(action: Callable[[sqlite3.Connection], _T]) -> _T:
    """Run ``action`` inside BEGIN IMMEDIATE, retrying on lock contention.

    With WAL + busy_timeout SQLite normally waits for the writer lock, but
    ``SQLITE_BUSY`` can still be returned immediately in some timing windows.
    Retrying with backoff keeps concurrent first uploads correct instead of
    surfacing a spurious 500 or building duplicate indices.
    """
    _ensure_bootstrapped()
    deadline = time.monotonic() + _BUSY_TIMEOUT_SECONDS
    attempt = 0
    while True:
        connection = _connect()
        try:
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                connection.close()
                time.sleep(min(0.2 * (2**attempt), 1.0))
                attempt += 1
                continue
            try:
                result = action(connection)
            except BaseException:
                connection.rollback()
                raise
            connection.commit()
            return result
        finally:
            connection.close()


def _has_schema(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'prediction_index'"
        ).fetchone()
        is not None
    )


def _load_ready(connection: sqlite3.Connection, digest: str) -> IndexOutcome | None:
    """Reconstruct a *ready* index. Anything else is treated as absent."""
    # On a brand-new database the schema is only created by the writer path; a
    # missing table is simply a cache miss, never an error.
    if not _has_schema(connection):
        return None
    meta = connection.execute(
        "SELECT * FROM prediction_index WHERE content_sha256 = ?",
        (digest,),
    ).fetchone()
    if meta is None or meta["status"] != "ready":
        # Unknown digest, or an orphaned in-flight marker: never reuse it.
        return None

    row_records = connection.execute(
        "SELECT row_no, series_id, issue_time, target_time, lower, upper "
        "FROM prediction_rows WHERE content_sha256 = ? ORDER BY row_no",
        (digest,),
    ).fetchall()
    error_records = connection.execute(
        "SELECT message FROM prediction_parse_errors "
        "WHERE content_sha256 = ? ORDER BY seq",
        (digest,),
    ).fetchall()

    rows = tuple(
        ParsedPrediction(
            row_no=int(record["row_no"]),
            series_id=record["series_id"],
            issue_time=datetime.fromisoformat(record["issue_time"]),
            target_time=datetime.fromisoformat(record["target_time"]),
            lower=float(record["lower"]),
            upper=float(record["upper"]),
        )
        for record in row_records
    )
    errors = tuple(record["message"] for record in error_records)

    # Integrity guard: a truncated/corrupted cache row must not feed a run.
    if len(rows) != meta["parsed_rows"] or len(errors) != meta["error_count"]:
        return None

    predictions = ParsedPredictions(
        rows=rows,
        errors=errors,
        row_count=int(meta["row_count"]),
    )
    return IndexOutcome(
        predictions=predictions,
        content_sha256=digest,
        reused=True,
        byte_size=int(meta["byte_size"]),
    )


def get_or_build(content: bytes) -> IndexOutcome:
    """Return the cached parse for ``content``, building it once if needed.

    Returns a ready :class:`IndexOutcome` or raises; a failed build leaves no
    trace in the database (the transaction is rolled back), so it can never
    participate in a backtest and the next upload retries cleanly.
    """
    digest = hash_content(content)
    byte_size = len(content)

    # Fast read-only path: common case once the index has been built. WAL mode
    # is a persistent database property, so no journal_mode pragma is needed.
    reader = _connect()
    try:
        cached = _load_ready(reader, digest)
    finally:
        reader.close()
    if cached is not None:
        return cached

    # Cache miss. Same-process threads queue on the digest lock; separate
    # processes additionally serialise on the SQLite write lock below.
    with _lock_for(digest):
        # Another thread may have built the index while we waited.
        reader = _connect()
        try:
            cached = _load_ready(reader, digest)
        finally:
            reader.close()
        if cached is not None:
            return cached

        # Serialise first-time builds across processes AND threads on the
        # SQLite write lock. BEGIN IMMEDIATE reserves the lock up front, so two
        # concurrent uploads of identical bytes can never both run the
        # parse/insert work.
        def _build(writer: sqlite3.Connection) -> IndexOutcome | None:
            cached = _load_ready(writer, digest)
            if cached is not None:
                return cached  # another process won the build; reuse it.

            now = _now_iso()
            # Clear any orphaned 'building' marker/rows before rebuilding.
            writer.execute(
                "DELETE FROM prediction_parse_errors WHERE content_sha256 = ?",
                (digest,),
            )
            writer.execute(
                "DELETE FROM prediction_rows WHERE content_sha256 = ?", (digest,)
            )
            writer.execute(
                "DELETE FROM prediction_index WHERE content_sha256 = ?", (digest,)
            )
            writer.execute(
                "INSERT INTO prediction_index (content_sha256, byte_size, status, "
                "row_count, parsed_rows, error_count, created_at, updated_at) "
                "VALUES (?, ?, 'building', 0, 0, 0, ?, ?)",
                (digest, byte_size, now, now),
            )

            # The expensive, deterministic parse. Structural ValidationErrors
            # (bad encoding/header/columns) propagate to the caller and roll
            # the whole transaction back -- no index is published.
            parsed = parse_predictions(content)

            writer.executemany(
                "INSERT INTO prediction_rows (content_sha256, row_no, series_id, "
                "issue_time, target_time, lower, upper) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        digest,
                        item.row_no,
                        item.series_id,
                        item.issue_time.isoformat(),
                        item.target_time.isoformat(),
                        item.lower,
                        item.upper,
                    )
                    for item in parsed.rows
                ],
            )
            writer.executemany(
                "INSERT INTO prediction_parse_errors (content_sha256, seq, message) "
                "VALUES (?, ?, ?)",
                [
                    (digest, seq, message)
                    for seq, message in enumerate(parsed.errors)
                ],
            )
            writer.execute(
                "UPDATE prediction_index SET status = 'ready', row_count = ?, "
                "parsed_rows = ?, error_count = ?, updated_at = ? "
                "WHERE content_sha256 = ?",
                (
                    parsed.row_count,
                    len(parsed.rows),
                    len(parsed.errors),
                    _now_iso(),
                    digest,
                ),
            )
            return None

        result = _with_write_lock(_build)
        if result is not None:
            return result

    # Read the freshly committed index back through the same loader so a
    # cached parse and a rebuilt parse take one identical code path.
    verify = _connect()
    try:
        outcome = _load_ready(verify, digest)
    finally:
        verify.close()
    if outcome is None:  # pragma: no cover - defensive; commit succeeded above
        raise RuntimeError("prediction index was not visible after commit")
    return IndexOutcome(
        predictions=outcome.predictions,
        content_sha256=digest,
        reused=False,
        byte_size=byte_size,
    )


def get_status(digest: str) -> IndexStatus | None:
    """Inspect one index (admin/diagnostics); never returns building data."""
    connection = _connect()
    try:
        row = connection.execute(
            "SELECT * FROM prediction_index WHERE content_sha256 = ?", (digest,)
        ).fetchone() if _has_schema(connection) else None
    finally:
        connection.close()
    if row is None:
        return None
    return IndexStatus(
        content_sha256=row["content_sha256"],
        byte_size=int(row["byte_size"]),
        status=row["status"],
        row_count=int(row["row_count"]),
        parsed_rows=int(row["parsed_rows"]),
        error_count=int(row["error_count"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def list_status(limit: int = 50) -> list[IndexStatus]:
    limit = max(1, min(limit, 200))
    connection = _connect()
    try:
        if not _has_schema(connection):
            return []
        rows = connection.execute(
            "SELECT * FROM prediction_index ORDER BY updated_at DESC, rowid DESC "
            "LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        connection.close()
    return [
        IndexStatus(
            content_sha256=row["content_sha256"],
            byte_size=int(row["byte_size"]),
            status=row["status"],
            row_count=int(row["row_count"]),
            parsed_rows=int(row["parsed_rows"]),
            error_count=int(row["error_count"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        for row in rows
    ]
