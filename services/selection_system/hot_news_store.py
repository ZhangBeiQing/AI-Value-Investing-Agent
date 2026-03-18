"""SQLite-backed persistence for incremental hot-news theme state."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from core.logging import get_logger


LOGGER = get_logger("SelectionHotNewsStore")

_THEME_JSON_FIELDS = {
    "bull_case_json": [],
    "bear_case_json": [],
    "linked_boards_json": [],
    "linked_symbols_json": [],
    "key_evidence_news_ids_json": [],
    "aliases_json": [],
    "metadata_json": {},
}


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_loads(text: str, *, default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def _now_iso() -> str:
    return datetime.now().isoformat()


def _dedupe_texts(values: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _normalize_theme_record(theme: Mapping[str, Any]) -> Dict[str, Any]:
    now = _now_iso()
    aliases = _dedupe_texts(
        list(theme.get("aliases") or [])
        + [theme.get("theme_name") or theme.get("canonical_name") or ""]
    )
    metadata = dict(theme.get("metadata") or {})
    normalized = {
        "theme_id": str(theme.get("theme_id") or "").strip(),
        "theme_name": str(theme.get("theme_name") or theme.get("canonical_name") or "").strip(),
        "status": str(theme.get("status") or "active").strip() or "active",
        "first_seen_at": str(theme.get("first_seen_at") or now),
        "last_seen_at": str(theme.get("last_seen_at") or now),
        "created_at": str(theme.get("created_at") or now),
        "updated_at": str(theme.get("updated_at") or now),
        "summary": str(theme.get("summary") or "").strip(),
        "today_delta": str(theme.get("today_delta") or "").strip(),
        "strength": str(theme.get("strength") or "emergence").strip() or "emergence",
        "persistence_view": str(theme.get("persistence_view") or "").strip(),
        "bull_case": _dedupe_texts(theme.get("bull_case") or []),
        "bear_case": _dedupe_texts(theme.get("bear_case") or []),
        "linked_boards": _dedupe_texts(theme.get("linked_boards") or []),
        "linked_symbols": _dedupe_texts(theme.get("linked_symbols") or []),
        "key_evidence_news_ids": _dedupe_texts(theme.get("key_evidence_news_ids") or []),
        "aliases": aliases,
        "metadata": metadata,
    }
    if not normalized["theme_id"]:
        raise ValueError("theme_id 不能为空")
    if not normalized["theme_name"]:
        raise ValueError("theme_name 不能为空")
    return normalized


def _row_to_theme(row: sqlite3.Row) -> Dict[str, Any]:
    payload = {
        "theme_id": row["theme_id"],
        "theme_name": row["canonical_name"],
        "status": row["status"],
        "first_seen_at": row["first_seen_at"],
        "last_seen_at": row["last_seen_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "summary": row["summary"],
        "today_delta": row["today_delta"],
        "strength": row["strength"],
        "persistence_view": row["persistence_view"],
    }
    for column_name, default in _THEME_JSON_FIELDS.items():
        key = column_name.removesuffix("_json")
        payload[key] = _json_loads(row[column_name], default=default)
    return payload


class HotNewsStateStore:
    """Persistent theme-state store with SQLite tables and embedding cache."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS themes (
                    theme_id TEXT PRIMARY KEY,
                    canonical_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    summary TEXT NOT NULL DEFAULT '',
                    today_delta TEXT NOT NULL DEFAULT '',
                    strength TEXT NOT NULL DEFAULT 'emergence',
                    persistence_view TEXT NOT NULL DEFAULT '',
                    bull_case_json TEXT NOT NULL DEFAULT '[]',
                    bear_case_json TEXT NOT NULL DEFAULT '[]',
                    linked_boards_json TEXT NOT NULL DEFAULT '[]',
                    linked_symbols_json TEXT NOT NULL DEFAULT '[]',
                    key_evidence_news_ids_json TEXT NOT NULL DEFAULT '[]',
                    aliases_json TEXT NOT NULL DEFAULT '[]',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_themes_status_last_seen
                    ON themes(status, last_seen_at DESC);

                CREATE TABLE IF NOT EXISTS theme_daily_state (
                    run_date TEXT NOT NULL,
                    theme_id TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    PRIMARY KEY (run_date, theme_id)
                );

                CREATE TABLE IF NOT EXISTS theme_evidence (
                    theme_id TEXT NOT NULL,
                    run_date TEXT NOT NULL,
                    news_id TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    published_at TEXT NOT NULL DEFAULT '',
                    content_digest TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (theme_id, run_date, news_id)
                );

                CREATE INDEX IF NOT EXISTS idx_theme_evidence_theme_date
                    ON theme_evidence(theme_id, run_date DESC);

                CREATE TABLE IF NOT EXISTS theme_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_date TEXT NOT NULL,
                    op_type TEXT NOT NULL,
                    theme_id TEXT,
                    target_theme_id TEXT,
                    candidate_id TEXT,
                    reason TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_theme_operations_run_date
                    ON theme_operations(run_date);

                CREATE TABLE IF NOT EXISTS theme_aliases (
                    theme_id TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (theme_id, alias)
                );

                CREATE INDEX IF NOT EXISTS idx_theme_aliases_alias
                    ON theme_aliases(alias);

                CREATE TABLE IF NOT EXISTS embedding_cache (
                    text_hash TEXT NOT NULL,
                    model TEXT NOT NULL,
                    text TEXT NOT NULL DEFAULT '',
                    vector_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (text_hash, model)
                );
                """
            )

    def has_snapshot_for_run(self, run_date: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM theme_daily_state WHERE run_date = ? LIMIT 1",
                (run_date,),
            ).fetchone()
        return row is not None

    def has_future_snapshots(self, run_date: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM theme_daily_state WHERE run_date > ? LIMIT 1",
                (run_date,),
            ).fetchone()
        return row is not None

    def latest_snapshot_run_date(self) -> str:
        with self._connect() as connection:
            row = connection.execute("SELECT MAX(run_date) AS run_date FROM theme_daily_state").fetchone()
        if row is None:
            return ""
        return str(row["run_date"] or "")

    def load_themes_before(self, run_date: str) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT MAX(run_date) AS run_date FROM theme_daily_state WHERE run_date < ?",
                (run_date,),
            ).fetchone()
            snapshot_run_date = str(row["run_date"] or "") if row is not None else ""
            if not snapshot_run_date:
                return []

            rows = connection.execute(
                """
                SELECT snapshot_json
                FROM theme_daily_state
                WHERE run_date = ?
                ORDER BY theme_id ASC
                """,
                (snapshot_run_date,),
            ).fetchall()

        themes: List[Dict[str, Any]] = []
        for row in rows:
            snapshot = _json_loads(str(row["snapshot_json"] or ""), default={})
            if isinstance(snapshot, dict):
                themes.append(snapshot)
        return themes

    def rollback_run_date(self, run_date: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM theme_operations WHERE run_date = ?", (run_date,))
            connection.execute("DELETE FROM theme_evidence WHERE run_date = ?", (run_date,))
            connection.execute("DELETE FROM theme_daily_state WHERE run_date = ?", (run_date,))
            self._rebuild_latest_theme_table(connection)

        LOGGER.info("已删除并回滚 hot_news_state 当日快照: %s", run_date)

    def load_themes(self, *, statuses: Sequence[str] | None = None, limit: int | None = None) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)

        query = """
            SELECT *
            FROM themes
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += """
            ORDER BY
                CASE status
                    WHEN 'active' THEN 0
                    WHEN 'cooling' THEN 1
                    ELSE 2
                END,
                last_seen_at DESC,
                updated_at DESC
        """
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        with self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [_row_to_theme(row) for row in rows]

    def load_theme(self, theme_id: str) -> Dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM themes WHERE theme_id = ?", (theme_id,)).fetchone()
        if row is None:
            return None
        return _row_to_theme(row)

    def write_theme(self, theme: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = _normalize_theme_record(theme)
        with self._connect() as connection:
            self._write_theme(connection, normalized)
        return normalized

    def _write_theme(self, connection: sqlite3.Connection, theme: Mapping[str, Any]) -> None:
        normalized = _normalize_theme_record(theme)
        connection.execute(
            """
            INSERT INTO themes (
                theme_id,
                canonical_name,
                status,
                first_seen_at,
                last_seen_at,
                created_at,
                updated_at,
                summary,
                today_delta,
                strength,
                persistence_view,
                bull_case_json,
                bear_case_json,
                linked_boards_json,
                linked_symbols_json,
                key_evidence_news_ids_json,
                aliases_json,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme_id) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                status = excluded.status,
                first_seen_at = excluded.first_seen_at,
                last_seen_at = excluded.last_seen_at,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                summary = excluded.summary,
                today_delta = excluded.today_delta,
                strength = excluded.strength,
                persistence_view = excluded.persistence_view,
                bull_case_json = excluded.bull_case_json,
                bear_case_json = excluded.bear_case_json,
                linked_boards_json = excluded.linked_boards_json,
                linked_symbols_json = excluded.linked_symbols_json,
                key_evidence_news_ids_json = excluded.key_evidence_news_ids_json,
                aliases_json = excluded.aliases_json,
                metadata_json = excluded.metadata_json
            """,
            (
                normalized["theme_id"],
                normalized["theme_name"],
                normalized["status"],
                normalized["first_seen_at"],
                normalized["last_seen_at"],
                normalized["created_at"],
                normalized["updated_at"],
                normalized["summary"],
                normalized["today_delta"],
                normalized["strength"],
                normalized["persistence_view"],
                _json_dumps(normalized["bull_case"]),
                _json_dumps(normalized["bear_case"]),
                _json_dumps(normalized["linked_boards"]),
                _json_dumps(normalized["linked_symbols"]),
                _json_dumps(normalized["key_evidence_news_ids"]),
                _json_dumps(normalized["aliases"]),
                _json_dumps(normalized["metadata"]),
            ),
        )
        self._replace_theme_aliases(connection, normalized["theme_id"], normalized["aliases"])

    def _rebuild_latest_theme_table(self, connection: sqlite3.Connection) -> None:
        connection.execute("DELETE FROM themes")
        connection.execute("DELETE FROM theme_aliases")
        row = connection.execute("SELECT MAX(run_date) AS run_date FROM theme_daily_state").fetchone()
        latest_run_date = str(row["run_date"] or "") if row is not None else ""
        if not latest_run_date:
            return

        rows = connection.execute(
            """
            SELECT snapshot_json
            FROM theme_daily_state
            WHERE run_date = ?
            ORDER BY theme_id ASC
            """,
            (latest_run_date,),
        ).fetchall()
        for row in rows:
            snapshot = _json_loads(str(row["snapshot_json"] or ""), default={})
            if isinstance(snapshot, dict):
                self._write_theme(connection, snapshot)

    def _replace_theme_aliases(
        self,
        connection: sqlite3.Connection,
        theme_id: str,
        aliases: Sequence[str],
    ) -> None:
        connection.execute("DELETE FROM theme_aliases WHERE theme_id = ?", (theme_id,))
        updated_at = _now_iso()
        for alias in _dedupe_texts(aliases):
            connection.execute(
                "INSERT OR REPLACE INTO theme_aliases(theme_id, alias, updated_at) VALUES (?, ?, ?)",
                (theme_id, alias, updated_at),
            )

    def replace_theme_evidence(
        self,
        theme_id: str,
        run_date: str,
        evidence_items: Sequence[Mapping[str, Any]],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM theme_evidence WHERE theme_id = ? AND run_date = ?",
                (theme_id, run_date),
            )
            for item in evidence_items:
                news_id = str(item.get("news_id") or "").strip()
                if not news_id:
                    continue
                title = str(item.get("title") or "").strip()
                content = str(item.get("content") or "").strip()
                digest = sha1(content.encode("utf-8")).hexdigest()[:12] if content else ""
                connection.execute(
                    """
                    INSERT OR REPLACE INTO theme_evidence(
                        theme_id,
                        run_date,
                        news_id,
                        title,
                        source,
                        published_at,
                        content_digest,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        theme_id,
                        run_date,
                        news_id,
                        title,
                        str(item.get("source") or ""),
                        str(item.get("published_at") or ""),
                        digest,
                        _json_dumps(dict(item.get("metadata") or {})),
                    ),
                )

    def append_operation(
        self,
        *,
        run_date: str,
        op_type: str,
        theme_id: str | None = None,
        target_theme_id: str | None = None,
        candidate_id: str | None = None,
        reason: str = "",
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO theme_operations(
                    run_date,
                    op_type,
                    theme_id,
                    target_theme_id,
                    candidate_id,
                    reason,
                    payload_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_date,
                    op_type,
                    theme_id,
                    target_theme_id,
                    candidate_id,
                    reason,
                    _json_dumps(dict(payload or {})),
                    _now_iso(),
                ),
            )

    def persist_daily_snapshot(self, run_date: str, themes: Sequence[Mapping[str, Any]]) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM theme_daily_state WHERE run_date = ?", (run_date,))
            for theme in themes:
                normalized = _normalize_theme_record(theme)
                connection.execute(
                    """
                    INSERT OR REPLACE INTO theme_daily_state(run_date, theme_id, snapshot_json)
                    VALUES (?, ?, ?)
                    """,
                    (
                        run_date,
                        normalized["theme_id"],
                        _json_dumps(normalized),
                    ),
                )

    def load_cached_embedding(self, *, text_hash: str, model: str) -> List[float] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT vector_json FROM embedding_cache WHERE text_hash = ? AND model = ?",
                (text_hash, model),
            ).fetchone()
        if row is None:
            return None
        payload = _json_loads(str(row["vector_json"] or ""), default=[])
        if not isinstance(payload, list):
            return None
        try:
            return [float(value) for value in payload]
        except (TypeError, ValueError):
            return None

    def save_cached_embedding(self, *, text_hash: str, model: str, text: str, vector: Sequence[float]) -> None:
        serialized = [float(value) for value in vector]
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO embedding_cache(text_hash, model, text, vector_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    text_hash,
                    model,
                    text,
                    _json_dumps(serialized),
                    _now_iso(),
                ),
            )
