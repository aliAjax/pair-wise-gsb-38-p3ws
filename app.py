"""Subtitle localization quality-control and delivery service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "subtitle_qc.db"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class Database:
    # In-process singleflight: the first packaging request for a
    # (version, channel, content) slot does the work; concurrent followers
    # wait and receive the occupying package instead of repacking.
    _pack_locks: dict[tuple[int, int, str], threading.Lock] = {}
    _pack_guards = threading.Lock()

    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB, packer=None):
        self.path = str(path)
        # Callable(channel, payload) -> {"files": [{"name","size","sha256"}], "manifest": {...}, "manifest_hash": ...}
        self.packer = packer or self._default_pack
        self._init_schema()

    def _slot_lock(self, key: tuple[int, int, str]) -> threading.Lock:
        with self._pack_guards:
            lock = self._pack_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._pack_locks[key] = lock
            return lock

    @staticmethod
    def _default_pack(channel: sqlite3.Row, payload: dict[str, Any]) -> dict[str, Any]:
        """Build deterministic channel files. Raises DomainError when the
        locked content violates the channel's subtitle spec."""
        fmt = channel["subtitle_format"]
        cues = payload["cues"]
        max_cues = channel["max_cues"]
        if max_cues is not None and len(cues) > int(max_cues):
            raise DomainError(f"渠道 {channel['code']} 最多 {max_cues} 条字幕，当前 {len(cues)} 条", 422)
        if fmt == "ttml":
            body = '<tt xmlns="http://www.w3.org/ns/ttml"><body><div>\n'
            body += "".join(Database.render_cue("ttml", c["cue_index"], c["start_ms"], c["end_ms"], c["text"]) for c in cues)
            body += "</div></body></tt>\n"
        elif fmt == "vtt":
            body = "WEBVTT\n\n" + "".join(
                Database.render_cue("vtt", c["cue_index"], c["start_ms"], c["end_ms"], c["text"]) for c in cues)
        else:
            body = "".join(
                Database.render_cue("srt", c["cue_index"], c["start_ms"], c["end_ms"], c["text"]) for c in cues)
        ext = fmt
        sub_bytes = body.encode("utf-8")
        base = f"{payload['language']}_v{payload['version_no']}_{channel['code']}"
        manifest = {
            "channel_code": channel["code"],
            "subtitle_format": fmt,
            "content_hash": payload["content_hash"],
            "cues": cues,
            "glossary": payload["glossary"],
        }
        manifest_text = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        files = []
        sub_name = f"{base}.{ext}"
        files.append({"name": sub_name, "size": len(sub_bytes),
                      "sha256": hashlib.sha256(sub_bytes).hexdigest()})
        man_bytes = manifest_text.encode("utf-8")
        files.append({"name": f"{base}.manifest.json", "size": len(man_bytes),
                      "sha256": hashlib.sha256(man_bytes).hexdigest()})
        checksum_text = "\n".join(f"{f['sha256']}  {f['name']}" for f in files) + "\n"
        cb = checksum_text.encode("utf-8")
        files.append({"name": f"{base}.sha256", "size": len(cb),
                      "sha256": hashlib.sha256(cb).hexdigest()})
        manifest_hash = hashlib.sha256(
            json.dumps({"files": files, "manifest": manifest}, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")).encode()
        ).hexdigest()
        return {"files": files, "manifest": manifest, "manifest_hash": manifest_hash}

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    source_language TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL CHECK(duration_ms > 0),
                    owner TEXT NOT NULL,
                    media_name TEXT NOT NULL,
                    media_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL REFERENCES projects(id),
                    language TEXT NOT NULL,
                    version_no INTEGER NOT NULL,
                    parent_id INTEGER REFERENCES versions(id),
                    status TEXT NOT NULL DEFAULT 'draft',
                    revision INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(project_id,language,version_no)
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    user TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('translator','timeline','reviewer')),
                    assigned_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(version_id,user,role)
                );
                CREATE TABLE IF NOT EXISTS cues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    cue_index INTEGER NOT NULL,
                    start_ms INTEGER NOT NULL CHECK(start_ms >= 0),
                    end_ms INTEGER NOT NULL CHECK(end_ms > start_ms),
                    text TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(version_id,cue_index)
                );
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    cue_id INTEGER REFERENCES cues(id) ON DELETE SET NULL,
                    user TEXT NOT NULL,
                    time_ms INTEGER NOT NULL CHECK(time_ms >= 0),
                    body TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS glossaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    source_term TEXT NOT NULL,
                    required_translation TEXT NOT NULL,
                    forbidden_terms TEXT NOT NULL DEFAULT '[]',
                    notes TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    UNIQUE(project_id,source_term)
                );
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    reviewer TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    comment TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL UNIQUE REFERENCES versions(id),
                    supersedes_version_id INTEGER REFERENCES versions(id),
                    snapshot_hash TEXT NOT NULL,
                    manifest TEXT NOT NULL,
                    delivered_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    subtitle_format TEXT NOT NULL CHECK(subtitle_format IN ('srt','vtt','ttml')),
                    max_cues INTEGER,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    UNIQUE(project_id,code)
                );
                CREATE TABLE IF NOT EXISTS version_locks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    lock_no INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    locked_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(version_id,lock_no)
                );
                CREATE TABLE IF NOT EXISTS package_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    requested_by TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'packing' CHECK(status IN ('packing','partial','done')),
                    created_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS packages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
                    channel_id INTEGER NOT NULL REFERENCES channels(id),
                    batch_id INTEGER REFERENCES package_batches(id) ON DELETE SET NULL,
                    lock_id INTEGER REFERENCES version_locks(id),
                    content_hash TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'channel' CHECK(kind IN ('channel','legacy')),
                    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','packing','packed','failed','invalidated')),
                    spec_summary TEXT NOT NULL DEFAULT '',
                    manifest TEXT,
                    manifest_hash TEXT,
                    files TEXT,
                    pack_error TEXT,
                    legacy_delivery_id INTEGER UNIQUE REFERENCES deliveries(id),
                    confirmed INTEGER NOT NULL DEFAULT 0,
                    requested_by TEXT,
                    created_at TEXT NOT NULL,
                    packed_at TEXT,
                    invalidated_at TEXT,
                    UNIQUE(version_id,channel_id,content_hash)
                );
                CREATE TABLE IF NOT EXISTS receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_id INTEGER NOT NULL UNIQUE REFERENCES packages(id),
                    external_ref TEXT NOT NULL,
                    file_count INTEGER NOT NULL,
                    manifest_hash TEXT NOT NULL,
                    files TEXT NOT NULL,
                    raw TEXT NOT NULL DEFAULT '',
                    received_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
        self._migrate_legacy_deliveries()

    # ---- delivery line helpers -------------------------------------------------

    @staticmethod
    def render_cue(fmt: str, cue_index: int, start_ms: int, end_ms: int, text: str) -> str:
        def ts_srt(v: int) -> str:
            h, rem = divmod(v, 3_600_000)
            m, rem = divmod(rem, 60_000)
            s, ms = divmod(rem, 1000)
            return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

        def ts_vtt(v: int) -> str:
            h, rem = divmod(v, 3_600_000)
            m, rem = divmod(rem, 60_000)
            s, ms = divmod(rem, 1000)
            return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

        if fmt == "srt":
            return f"{cue_index}\n{ts_srt(start_ms)} --> {ts_srt(end_ms)}\n{text}\n"
        if fmt == "vtt":
            stamp = ts_vtt(start_ms).lstrip("0:") or "0.000"
            return f"{stamp} --> {ts_vtt(end_ms)}\n{text}\n"
        safe = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f'<p begin="{Database._ts_ttml(start_ms)}" end="{Database._ts_ttml(end_ms)}">{safe}</p>\n'

    @staticmethod
    def _ts_ttml(v: int) -> str:
        h, rem = divmod(v, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, _ = divmod(rem, 1000)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _content_payload(self, conn: sqlite3.Connection, version: sqlite3.Row) -> dict[str, Any]:
        """Locked content fingerprint inputs: cues plus the project glossary."""
        cues = [dict(r) for r in conn.execute(
            "SELECT cue_index,start_ms,end_ms,text FROM cues WHERE version_id=? ORDER BY cue_index", (version["id"],))]
        glossary = [dict(r) for r in conn.execute(
            "SELECT source_term,required_translation,forbidden_terms FROM glossaries WHERE project_id=? ORDER BY source_term",
            (version["project_id"],))]
        return {
            "project_id": int(version["project_id"]),
            "version_id": int(version["id"]),
            "language": version["language"],
            "version_no": int(version["version_no"]),
            "cues": cues,
            "glossary": glossary,
        }

    @staticmethod
    def _hash_payload(payload: Any) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def _current_lock(self, conn: sqlite3.Connection, version_id: int) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM version_locks WHERE version_id=? ORDER BY lock_no DESC LIMIT 1", (version_id,)
        ).fetchone()

    def _migrate_legacy_deliveries(self) -> None:
        """Upgrade pre-pipeline deliveries into legacy channel records.

        Backfilled packages carry the original manifest and stay unconfirmed;
        they are excluded from delivery completion until a human confirms them.
        Idempotent: deliveries already linked to a package are skipped.
        """
        with self.connect() as conn:
            # The legacy channel stays inactive while records are unconfirmed,
            # so it does not count into delivery completion beforehand. It is
            # activated when a human confirms the backfilled record.
            conn.execute("INSERT OR IGNORE INTO channels(project_id,code,name,subtitle_format,max_cues,active,created_at)"
                         " SELECT p.id,'legacy','遗留交付渠道','srt',NULL,0,? FROM projects p", (utcnow(),))
            rows = conn.execute(
                """SELECT d.*, v.project_id, v.language, v.version_no FROM deliveries d
                   JOIN versions v ON v.id=d.version_id
                   WHERE d.id NOT IN (SELECT legacy_delivery_id FROM packages WHERE legacy_delivery_id IS NOT NULL)"""
            ).fetchall()
            for d in rows:
                channel = conn.execute(
                    "SELECT * FROM channels WHERE project_id=? AND code='legacy'", (d["project_id"],)).fetchone()
                manifest = json.loads(d["manifest"])
                lock = conn.execute(
                    "SELECT * FROM version_locks WHERE version_id=? ORDER BY lock_no DESC LIMIT 1",
                    (d["version_id"],)).fetchone()
                if lock is None:
                    lock_no = 1
                    cur = conn.execute(
                        "INSERT INTO version_locks(version_id,lock_no,content_hash,locked_by,created_at) VALUES(?,?,?,?,?)",
                        (d["version_id"], lock_no, d["snapshot_hash"], d["delivered_by"], d["created_at"]))
                    lock_id = cur.lastrowid
                else:
                    lock_id = lock["id"]
                legacy_hash = "legacy:" + d["snapshot_hash"]
                conn.execute(
                    """INSERT INTO packages(version_id,channel_id,batch_id,lock_id,content_hash,kind,status,
                       spec_summary,manifest,manifest_hash,files,pack_error,legacy_delivery_id,confirmed,
                       requested_by,created_at,packed_at,invalidated_at)
                       VALUES(?,?,NULL,?,?,'legacy','packed','遗留渠道',?,?,?,NULL,?,0,?,?,?,NULL)""",
                    (d["version_id"], channel["id"], lock_id, legacy_hash,
                     json.dumps(manifest, ensure_ascii=False, sort_keys=True), d["snapshot_hash"],
                     json.dumps([{"name": f"{d['version_id']}.snapshot.json", "sha256": d["snapshot_hash"]}]),
                     d["id"], d["delivered_by"], d["created_at"], d["created_at"]))
                # Nothing has been re-signed on the new pipeline; the version
                # waits at locked and does not count as fully delivered.
                conn.execute("UPDATE versions SET status='locked',updated_at=? WHERE id=? AND status='delivered'",
                             (utcnow(), d["version_id"]))
                self._audit(conn, "system", "legacy.migrated", "package", None,
                            {"delivery_id": d["id"], "version_id": d["version_id"]})

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

    def create_project(self, actor: str, payload: dict[str, Any], role: str = "owner") -> dict[str, Any]:
        if role not in {"owner", "admin"}:
            raise DomainError("只有项目负责人可以创建项目", 403)
        name = str(payload.get("name", "")).strip()
        source_language = str(payload.get("source_language", "")).strip()
        media_name = str(payload.get("media_name", "")).strip()
        media_sha = str(payload.get("media_sha256", "")).lower()
        try:
            duration_ms = int(payload.get("duration_ms"))
        except (TypeError, ValueError) as exc:
            raise DomainError("成片时长必须是毫秒整数") from exc
        if not name or not source_language or not media_name or duration_ms <= 0 or len(media_sha) != 64:
            raise DomainError("项目名称、源语言、成片、时长或校验值不完整")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    "INSERT INTO projects(name,source_language,duration_ms,owner,media_name,media_sha256,created_at) VALUES(?,?,?,?,?,?,?)",
                    (name, source_language, duration_ms, actor, media_name, media_sha, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("项目名称已存在", 409) from exc
            self._audit(conn, actor, "project.created", "project", cur.lastrowid, {"name": name})
            return dict(conn.execute("SELECT * FROM projects WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_glossary(self, project_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            project = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
            if not project:
                raise DomainError("项目不存在", 404)
            if actor != project["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以维护术语表", 403)
            source_term = str(payload.get("source_term", "")).strip()
            required = str(payload.get("required_translation", "")).strip()
            forbidden = payload.get("forbidden_terms", [])
            if not source_term or not required or not isinstance(forbidden, list):
                raise DomainError("术语、指定译法和禁用词格式不合法")
            conn.execute(
                """INSERT INTO glossaries(project_id,source_term,required_translation,forbidden_terms,notes,created_at)
                   VALUES(?,?,?,?,?,?) ON CONFLICT(project_id,source_term) DO UPDATE SET
                   required_translation=excluded.required_translation,forbidden_terms=excluded.forbidden_terms,notes=excluded.notes""",
                (project_id, source_term, required, json.dumps(forbidden, ensure_ascii=False), str(payload.get("notes", "")), utcnow()),
            )
            # A glossary change shifts every locked fingerprint; unsigned
            # channel packages become stale. Signed packages keep their
            # manifests and are never touched.
            stale = conn.execute(
                """SELECT p.id FROM packages p JOIN versions v ON v.id=p.version_id
                   WHERE v.project_id=? AND p.kind='channel' AND p.status IN ('queued','packing','packed','failed')
                   AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.package_id=p.id)""",
                (project_id,)).fetchall()
            conn.execute(
                """UPDATE packages SET status='invalidated',invalidated_at=?
                   WHERE id IN (SELECT p.id FROM packages p JOIN versions v ON v.id=p.version_id
                     WHERE v.project_id=? AND p.kind='channel' AND p.status IN ('queued','packing','packed','failed')
                     AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.package_id=p.id))""",
                (utcnow(), project_id))
            self._audit(conn, actor, "glossary.saved", "project", project_id,
                        {"source_term": source_term, "invalidated_packages": len(stale)})
        return {"project_id": project_id, "source_term": source_term, "required_translation": required, "forbidden_terms": forbidden}

    def create_version(self, project_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        language = str(payload.get("language", "")).strip()
        if not language:
            raise DomainError("目标语言不能为空")
        parent_id = payload.get("parent_id")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            project = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
            if not project:
                raise DomainError("项目不存在", 404)
            if actor != project["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以创建版本", 403)
            if parent_id is not None:
                parent = conn.execute("SELECT * FROM versions WHERE id=? AND project_id=?", (int(parent_id), project_id)).fetchone()
                if not parent or parent["language"] != language:
                    raise DomainError("父版本不存在或目标语言不一致", 409)
            next_no = int(conn.execute("SELECT COALESCE(MAX(version_no),0)+1 value FROM versions WHERE project_id=? AND language=?", (project_id, language)).fetchone()["value"])
            cur = conn.execute(
                "INSERT INTO versions(project_id,language,version_no,parent_id,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (project_id, language, next_no, parent_id, actor, utcnow(), utcnow()),
            )
            self._audit(conn, actor, "version.created", "version", cur.lastrowid, {"language": language, "version_no": next_no})
            return dict(conn.execute("SELECT * FROM versions WHERE id=?", (cur.lastrowid,)).fetchone())

    def assign(self, version_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        user = str(payload.get("user", "")).strip()
        assignment_role = str(payload.get("role", "")).strip()
        if not user or assignment_role not in {"translator", "timeline", "reviewer"}:
            raise DomainError("人员或角色不合法")
        with self.connect() as conn:
            version = conn.execute("SELECT v.*,p.owner FROM versions v JOIN projects p ON p.id=v.project_id WHERE v.id=?", (version_id,)).fetchone()
            if not version:
                raise DomainError("版本不存在", 404)
            if actor != version["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以分配人员", 403)
            conn.execute("INSERT OR IGNORE INTO assignments(version_id,user,role,assigned_by,created_at) VALUES(?,?,?,?,?)", (version_id, user, assignment_role, actor, utcnow()))
            self._audit(conn, actor, "assignment.saved", "version", version_id, {"user": user, "role": assignment_role})
        return {"version_id": version_id, "user": user, "role": assignment_role}

    def _version(self, conn: sqlite3.Connection, version_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT v.*,p.owner,p.duration_ms FROM versions v JOIN projects p ON p.id=v.project_id WHERE v.id=?", (version_id,)).fetchone()
        if not row:
            raise DomainError("字幕版本不存在", 404)
        return row

    def _can_edit(self, conn: sqlite3.Connection, version: sqlite3.Row, actor: str) -> bool:
        if actor == version["owner"]:
            return True
        return bool(conn.execute("SELECT 1 FROM assignments WHERE version_id=? AND user=? AND role IN ('translator','timeline')", (version["id"], actor)).fetchone())

    def _validate_glossary(self, conn: sqlite3.Connection, project_id: int, text: str) -> None:
        for row in conn.execute("SELECT * FROM glossaries WHERE project_id=?", (project_id,)):
            forbidden = json.loads(row["forbidden_terms"])
            for term in forbidden:
                if term and term in text:
                    raise DomainError(f"字幕包含禁用译法: {term}")
            # The glossary is enforced only when the corresponding source term
            # appears in the localized cue. This keeps it useful without making
            # every cue repeat every glossary word.
            if row["source_term"] in text and row["required_translation"] not in text:
                raise DomainError(f"术语 {row['source_term']} 必须使用指定译法 {row['required_translation']}")

    def save_cue(self, version_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            if version["status"] != "draft":
                raise DomainError("只有草稿版本可以修改字幕", 409)
            if not self._can_edit(conn, version, actor):
                raise DomainError("没有该版本的翻译或时间轴权限", 403)
            expected = payload.get("expected_revision")
            if expected is not None and int(expected) != int(version["revision"]):
                raise DomainError("版本已被其他成员修改，请刷新后重试", 409)
            try:
                cue_index = int(payload.get("cue_index"))
                start_ms = int(payload.get("start_ms"))
                end_ms = int(payload.get("end_ms"))
            except (TypeError, ValueError) as exc:
                raise DomainError("字幕序号和时间必须是整数") from exc
            text = str(payload.get("text", "")).strip()
            if cue_index < 0 or start_ms < 0 or end_ms <= start_ms or end_ms > int(version["duration_ms"]) or not text:
                raise DomainError("字幕时间、序号或内容不合法")
            self._validate_glossary(conn, int(version["project_id"]), text)
            cue_id = payload.get("cue_id")
            existing = None
            if cue_id is not None:
                existing = conn.execute("SELECT * FROM cues WHERE id=? AND version_id=?", (int(cue_id), version_id)).fetchone()
                if not existing:
                    raise DomainError("字幕条目不存在", 404)
            overlap = conn.execute(
                "SELECT * FROM cues WHERE version_id=? AND id<>? AND start_ms<? AND end_ms>? LIMIT 1",
                (version_id, int(cue_id or -1), end_ms, start_ms),
            ).fetchone()
            if overlap:
                raise DomainError("字幕时间轴发生重叠", 409)
            index_owner = conn.execute("SELECT * FROM cues WHERE version_id=? AND cue_index=? AND id<>?", (version_id, cue_index, int(cue_id or -1))).fetchone()
            if index_owner:
                raise DomainError("字幕序号已被使用", 409)
            if existing:
                conn.execute("UPDATE cues SET cue_index=?,start_ms=?,end_ms=?,text=?,updated_by=?,updated_at=? WHERE id=?", (cue_index, start_ms, end_ms, text, actor, utcnow(), existing["id"]))
                saved_id = existing["id"]
            else:
                cur = conn.execute("INSERT INTO cues(version_id,cue_index,start_ms,end_ms,text,updated_by,updated_at) VALUES(?,?,?,?,?,?,?)", (version_id, cue_index, start_ms, end_ms, text, actor, utcnow()))
                saved_id = cur.lastrowid
            revision = int(version["revision"]) + 1
            conn.execute("UPDATE versions SET revision=?,updated_at=? WHERE id=?", (revision, utcnow(), version_id))
            self._audit(conn, actor, "cue.saved", "version", version_id, {"cue_id": saved_id, "revision": revision})
        return dict(conn.execute("SELECT * FROM cues WHERE id=?", (saved_id,)).fetchone()) | {"version_revision": revision}

    def add_comment(self, version_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        body = str(payload.get("body", "")).strip()
        try:
            time_ms = int(payload.get("time_ms"))
        except (TypeError, ValueError) as exc:
            raise DomainError("评论时间必须是毫秒整数") from exc
        with self.connect() as conn:
            version = self._version(conn, version_id)
            allowed = actor == version["owner"] or conn.execute("SELECT 1 FROM assignments WHERE version_id=? AND user=?", (version_id, actor)).fetchone()
            if not allowed:
                raise DomainError("只有项目成员可以评论", 403)
            if not body or time_ms < 0 or time_ms > int(version["duration_ms"]):
                raise DomainError("评论内容或时间点不合法")
            cue_id = payload.get("cue_id")
            if cue_id is not None and not conn.execute("SELECT 1 FROM cues WHERE id=? AND version_id=?", (int(cue_id), version_id)).fetchone():
                raise DomainError("评论关联的字幕不存在", 404)
            cur = conn.execute("INSERT INTO comments(version_id,cue_id,user,time_ms,body,created_at) VALUES(?,?,?,?,?,?)", (version_id, cue_id, actor, time_ms, body, utcnow()))
            self._audit(conn, actor, "comment.added", "version", version_id, {"comment_id": cur.lastrowid, "time_ms": time_ms})
        return {"id": int(cur.lastrowid), "version_id": version_id, "cue_id": cue_id, "user": actor, "time_ms": time_ms, "body": body, "status": "open"}

    def submit(self, version_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            if version["status"] != "draft" or not self._can_edit(conn, version, actor):
                raise DomainError("只有草稿版本的翻译或时间轴人员可以提交复核", 409)
            if not conn.execute("SELECT 1 FROM cues WHERE version_id=?", (version_id,)).fetchone():
                raise DomainError("空版本不能提交复核", 409)
            conn.execute("UPDATE versions SET status='review',updated_at=? WHERE id=?", (utcnow(), version_id))
            self._audit(conn, actor, "version.submitted", "version", version_id, {})
        return dict(conn.execute("SELECT * FROM versions WHERE id=?", (version_id,)).fetchone())

    def review(self, version_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        decision = str(payload.get("decision", "")).strip()
        if decision not in {"approve", "reject"}:
            raise DomainError("复核决定必须是 approve 或 reject")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            if version["status"] != "review":
                raise DomainError("版本当前不在复核阶段", 409)
            assigned = conn.execute("SELECT 1 FROM assignments WHERE version_id=? AND user=? AND role='reviewer'", (version_id, actor)).fetchone()
            if not assigned and actor != version["owner"]:
                raise DomainError("没有该版本的复核权限", 403)
            if actor == version["created_by"]:
                raise DomainError("创建人不能复核自己的版本", 403)
            conn.execute("INSERT INTO reviews(version_id,reviewer,decision,comment,created_at) VALUES(?,?,?,?,?)", (version_id, actor, decision, str(payload.get("comment", "")), utcnow()))
            status = "approved" if decision == "approve" else "draft"
            conn.execute("UPDATE versions SET status=?,updated_at=? WHERE id=?", (status, utcnow(), version_id))
            self._audit(conn, actor, f"version.{decision}", "version", version_id, {"comment": payload.get("comment", "")})
        return dict(conn.execute("SELECT * FROM versions WHERE id=?", (version_id,)).fetchone())

    def lock(self, version_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            if version["status"] != "approved":
                raise DomainError("只有已批准版本可以锁定", 409)
            if actor != version["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以锁定版本", 403)
            payload = self._content_payload(conn, version)
            content_hash = self._hash_payload(payload)
            next_no = int(conn.execute(
                "SELECT COALESCE(MAX(lock_no),0)+1 value FROM version_locks WHERE version_id=?", (version_id,)).fetchone()["value"])
            cur = conn.execute(
                "INSERT INTO version_locks(version_id,lock_no,content_hash,locked_by,created_at) VALUES(?,?,?,?,?)",
                (version_id, next_no, content_hash, actor, utcnow()))
            lock_id = cur.lastrowid
            # Packages tied to a previous lock, not yet signed, and whose
            # content differs from the new fingerprint are no longer the
            # current locked version. Same-content packages stay usable, and
            # signed receipts always keep their manifest.
            stale = conn.execute(
                """SELECT p.id FROM packages p WHERE p.version_id=? AND p.kind='channel'
                   AND p.lock_id IS NOT NULL AND p.lock_id<>? AND p.content_hash<>?
                   AND p.status IN ('queued','packing','packed','failed')
                   AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.package_id=p.id)""",
                (version_id, lock_id, content_hash)).fetchall()
            conn.execute(
                """UPDATE packages SET status='invalidated',invalidated_at=? WHERE id IN (
                     SELECT p.id FROM packages p WHERE p.version_id=? AND p.kind='channel'
                     AND p.lock_id IS NOT NULL AND p.lock_id<>? AND p.content_hash<>?
                     AND p.status IN ('queued','packing','packed','failed')
                     AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.package_id=p.id))""",
                (utcnow(), version_id, lock_id, content_hash))
            conn.execute("UPDATE versions SET status='locked',updated_at=? WHERE id=?", (utcnow(), version_id))
            self._audit(conn, actor, "version.locked", "version", version_id,
                        {"lock_no": next_no, "content_hash": content_hash, "invalidated_packages": len(stale)})
        return dict(conn.execute("SELECT * FROM versions WHERE id=?", (version_id,)).fetchone()) | {
            "lock_id": lock_id, "lock_no": next_no, "content_hash": content_hash}

    def unlock(self, version_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        """Reopen a locked version for subtitle/term corrections. Unsigned
        packages are invalidated; signed packages remain as records."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            if actor != version["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以解锁版本", 403)
            if version["status"] != "locked":
                raise DomainError("只有锁定版本可以解锁", 409)
            stale = conn.execute(
                """SELECT p.id FROM packages p WHERE p.version_id=? AND p.kind='channel'
                   AND p.status IN ('queued','packing','packed','failed')
                   AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.package_id=p.id)""",
                (version_id,)).fetchall()
            conn.execute(
                """UPDATE packages SET status='invalidated',invalidated_at=? WHERE id IN (
                     SELECT p.id FROM packages p WHERE p.version_id=? AND p.kind='channel'
                     AND p.status IN ('queued','packing','packed','failed')
                     AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.package_id=p.id))""",
                (utcnow(), version_id))
            conn.execute("UPDATE versions SET status='draft',updated_at=? WHERE id=?", (utcnow(), version_id))
            self._audit(conn, actor, "version.unlocked", "version", version_id,
                        {"invalidated_packages": len(stale)})
        return dict(conn.execute("SELECT * FROM versions WHERE id=?", (version_id,)).fetchone())

    def deliver(self, version_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            if actor != version["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以交付", 403)
            if version["status"] not in {"approved", "locked"}:
                raise DomainError("只有批准或锁定版本可以交付", 409)
            if conn.execute("SELECT 1 FROM deliveries WHERE version_id=?", (version_id,)).fetchone():
                raise DomainError("该版本已经交付，不能用新内容覆盖", 409)
            cues = [dict(r) for r in conn.execute("SELECT cue_index,start_ms,end_ms,text FROM cues WHERE version_id=? ORDER BY cue_index", (version_id,))]
            glossary = [dict(r) for r in conn.execute("SELECT source_term,required_translation,forbidden_terms FROM glossaries WHERE project_id=? ORDER BY source_term", (version["project_id"],))]
            manifest = {"project_id": version["project_id"], "version_id": version_id, "language": version["language"], "version_no": version["version_no"], "cues": cues, "glossary": glossary}
            snapshot_hash = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            previous = conn.execute("SELECT id FROM deliveries WHERE version_id IN (SELECT id FROM versions WHERE project_id=? AND language=? AND id<>?) ORDER BY id DESC LIMIT 1", (version["project_id"], version["language"], version_id)).fetchone()
            if previous:
                conn.execute("UPDATE versions SET status='superseded',updated_at=? WHERE id=(SELECT version_id FROM deliveries WHERE id=?)", (utcnow(), previous["id"]))
            cur = conn.execute(
                "INSERT INTO deliveries(version_id,supersedes_version_id,snapshot_hash,manifest,delivered_by,created_at) VALUES(?,?,?,?,?,?)",
                (version_id, previous["id"] if previous else None, snapshot_hash, json.dumps(manifest, ensure_ascii=False, sort_keys=True), actor, utcnow()),
            )
            conn.execute("UPDATE versions SET status='delivered',updated_at=? WHERE id=?", (utcnow(), version_id))
            self._audit(conn, actor, "version.delivered", "version", version_id, {"snapshot_hash": snapshot_hash})
        return dict(conn.execute("SELECT * FROM deliveries WHERE id=?", (cur.lastrowid,)).fetchone())

    # ---- channel rules ---------------------------------------------------------

    def upsert_channel(self, project_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        code = str(payload.get("code", "")).strip()
        name = str(payload.get("name", "")).strip()
        fmt = str(payload.get("subtitle_format", "")).strip().lower()
        if not code or not name or fmt not in {"srt", "vtt", "ttml"}:
            raise DomainError("渠道代号、名称和字幕格式(srt/vtt/ttml)不合法")
        max_cues = payload.get("max_cues")
        if max_cues is not None:
            try:
                max_cues = int(max_cues)
            except (TypeError, ValueError) as exc:
                raise DomainError("渠道字幕条数上限必须是整数") from exc
            if max_cues <= 0:
                raise DomainError("渠道字幕条数上限必须大于 0")
        active = 1 if payload.get("active", True) else 0
        with self.connect() as conn:
            project = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
            if not project:
                raise DomainError("项目不存在", 404)
            if actor != project["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以维护渠道规则", 403)
            conn.execute(
                """INSERT INTO channels(project_id,code,name,subtitle_format,max_cues,active,created_at)
                   VALUES(?,?,?,?,?,?,?) ON CONFLICT(project_id,code) DO UPDATE SET
                   name=excluded.name,subtitle_format=excluded.subtitle_format,
                   max_cues=excluded.max_cues,active=excluded.active""",
                (project_id, code, name, fmt, max_cues, active, utcnow()))
            self._audit(conn, actor, "channel.saved", "channel", None,
                        {"project_id": project_id, "code": code, "format": fmt})
            row = conn.execute("SELECT * FROM channels WHERE project_id=? AND code=?", (project_id, code)).fetchone()
        return self._channel_dict(row)

    @staticmethod
    def _channel_dict(row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["active"] = bool(d["active"])
        return d

    def list_channels(self, project_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if project_id is None:
                rows = conn.execute("SELECT * FROM channels ORDER BY project_id,id").fetchall()
            else:
                rows = conn.execute("SELECT * FROM channels WHERE project_id=? ORDER BY id", (project_id,)).fetchall()
            return [self._channel_dict(r) for r in rows]

    # ---- packaging batch -------------------------------------------------------

    def request_packages(self, version_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        """Create one packaging batch over the requested channels. Each channel
        slot is singleflight: a concurrent second request to the same channel
        gets the occupying package instead of a second compression."""
        with self.connect() as conn:
            version = self._version(conn, version_id)
            if actor != version["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以发起打包", 403)
            if version["status"] != "locked":
                raise DomainError("只有锁定版本可以打包", 409)
            lock = self._current_lock(conn, version_id)
            if lock is None:
                raise DomainError("锁定记录不存在，请重新锁定", 409)
            content = self._content_payload(conn, version)
            current_hash = self._hash_payload(content)
            if lock["content_hash"] != current_hash:
                raise DomainError("锁定内容已变化（字幕或术语），请重新锁定后再打包", 409)
            content["content_hash"] = current_hash
            channels = conn.execute(
                "SELECT * FROM channels WHERE project_id=? AND active=1 ORDER BY id", (version["project_id"],)).fetchall()
            by_code = {c["code"]: c for c in channels}
            requested = payload.get("channels")
            if requested is None:
                targets = channels
            else:
                if not isinstance(requested, list) or not requested:
                    raise DomainError("渠道列表不合法")
                targets = []
                for code in requested:
                    ch = by_code.get(str(code))
                    if ch is None:
                        raise DomainError(f"渠道 {code} 不存在或未启用", 404)
                    if ch not in targets:
                        targets.append(ch)
            if not targets:
                raise DomainError("项目没有可打包的启用渠道", 409)
            batch_cur = conn.execute(
                "INSERT INTO package_batches(version_id,requested_by,status,created_at) VALUES(?,?, 'packing',?)",
                (version_id, actor, utcnow()))
            batch_id = batch_cur.lastrowid
            self._audit(conn, actor, "package.batch_created", "batch", batch_id,
                        {"version_id": version_id, "channels": [c["code"] for c in targets]})

        results: list[dict[str, Any]] = []
        any_failed = False
        for channel in targets:
            outcome = self._occupy_and_pack(version_id, channel, lock, content, batch_id, actor, payload)
            results.append(outcome)
            if outcome["status"] == "failed":
                any_failed = True

        # Keep the channels that finished; the batch only marks partial.
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            failed = sum(1 for r in results if r["status"] == "failed")
            status = "partial" if failed else "done"
            conn.execute("UPDATE package_batches SET status=?,finished_at=? WHERE id=?", (status, utcnow(), batch_id))
            self._audit(conn, actor, "package.batch_finished", "batch", batch_id,
                        {"status": status, "failed": failed})
        return {"batch_id": batch_id, "version_id": version_id, "status": "partial" if any_failed else "done",
                "packages": results}

    def _occupy_and_pack(self, version_id: int, channel: sqlite3.Row, lock: sqlite3.Row,
                         content: dict[str, Any], batch_id: int, actor: str, payload: dict[str, Any]) -> dict[str, Any]:
        content_hash = content["content_hash"]
        key = (version_id, int(channel["id"]), content_hash)
        slot = self._slot_lock(key)

        # Fast path: a packed package or a concurrent in-flight occupant is
        # the slot's single answer. Failed/invalidated slots fall through to
        # be retried inside the slot lock ("continue from the gap").
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT * FROM packages WHERE version_id=? AND channel_id=? AND content_hash=? AND kind='channel'",
                (version_id, channel["id"], content_hash)).fetchone()
            if existing is not None and existing["status"] in {"packed", "queued", "packing"}:
                in_flight = existing["status"] in {"queued", "packing"}
                return self._package_dict(existing, channel["code"]) | {
                    "occupant": True,
                    "note": "并发占位：先到的压包请求进行中，返回占位结果，未重复压包" if in_flight
                    else "已有压包结果，未重复压包",
                }

        with slot:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                existing = conn.execute(
                    "SELECT * FROM packages WHERE version_id=? AND channel_id=? AND content_hash=? AND kind='channel'",
                    (version_id, channel["id"], content_hash)).fetchone()
                if existing is not None:
                    if existing["status"] in {"packed", "queued", "packing"}:
                        in_flight = existing["status"] in {"queued", "packing"}
                        return self._package_dict(existing, channel["code"]) | {
                            "occupant": True,
                            "note": "并发占位：沿用先到请求的压包结果，未重复压包" if not in_flight
                            else "并发占位：先到请求进行中，返回占位结果",
                        }
                    # failed / invalidated: reuse the slot row and repack, so
                    # completed channels in the batch stay untouched.
                    pkg_id = existing["id"]
                    conn.execute(
                        "UPDATE packages SET status='packing',batch_id=?,lock_id=?,pack_error=NULL,"
                        "invalidated_at=NULL,requested_by=?,created_at=? WHERE id=?",
                        (batch_id, lock["id"], actor, utcnow(), pkg_id))
                    retry_of = existing["status"]
                else:
                    spec = {"subtitle_format": channel["subtitle_format"], "max_cues": channel["max_cues"]}
                    cur = conn.execute(
                        """INSERT INTO packages(version_id,channel_id,batch_id,lock_id,content_hash,kind,status,
                           spec_summary,requested_by,created_at) VALUES(?,?,?,?,?, 'channel','packing',?,?,?)""",
                        (version_id, channel["id"], batch_id, lock["id"], content_hash,
                         json.dumps(spec, ensure_ascii=False, sort_keys=True), actor, utcnow()))
                    pkg_id = cur.lastrowid
                    retry_of = None
                self._audit(conn, actor, "package.slot_taken", "package", pkg_id,
                            {"channel": channel["code"], "batch_id": batch_id, "retry_of": retry_of})

            # Compression runs outside the write transaction so followers can
            # see the occupied slot and wait on the in-process lock.
            try:
                result = self.packer(channel, content)
            except DomainError as exc:
                with self.connect() as conn:
                    conn.execute("UPDATE packages SET status='failed',pack_error=? WHERE id=?", (str(exc), pkg_id))
                    self._audit(conn, actor, "package.failed", "package", pkg_id,
                                {"channel": channel["code"], "error": str(exc)})
                row = self._get_package(pkg_id)
                return self._package_dict(row, channel["code"])
            except Exception as exc:  # packaging infrastructure failure
                with self.connect() as conn:
                    conn.execute("UPDATE packages SET status='failed',pack_error=? WHERE id=?", (repr(exc), pkg_id))
                    self._audit(conn, actor, "package.failed", "package", pkg_id,
                                {"channel": channel["code"], "error": repr(exc)})
                row = self._get_package(pkg_id)
                return self._package_dict(row, channel["code"])

            with self.connect() as conn:
                conn.execute(
                    "UPDATE packages SET status='packed',manifest=?,manifest_hash=?,files=?,pack_error=NULL,packed_at=? WHERE id=?",
                    (json.dumps(result["manifest"], ensure_ascii=False, sort_keys=True),
                     result["manifest_hash"], json.dumps(result["files"], ensure_ascii=False), utcnow(), pkg_id))
                self._audit(conn, actor, "package.packed", "package", pkg_id,
                            {"channel": channel["code"], "manifest_hash": result["manifest_hash"]})
            row = self._get_package(pkg_id)
            return self._package_dict(row, channel["code"])

    def _get_package(self, pkg_id: int) -> sqlite3.Row:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM packages WHERE id=?", (pkg_id,)).fetchone()

    @staticmethod
    def _package_dict(row: sqlite3.Row, channel_code: str | None = None) -> dict[str, Any]:
        d = dict(row)
        d["confirmed"] = bool(d["confirmed"])
        for col in ("manifest", "files"):
            if d.get(col):
                try:
                    d[col] = json.loads(d[col])
                except (TypeError, json.JSONDecodeError):
                    pass
        if channel_code:
            d["channel_code"] = channel_code
        return d

    # ---- external receipts -----------------------------------------------------

    def record_receipt(self, package_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        external_ref = str(payload.get("external_ref", "")).strip()
        if not external_ref:
            raise DomainError("回执缺少外部系统流水号")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pkg = conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone()
            if not pkg:
                raise DomainError("包不存在", 404)
            channel = conn.execute("SELECT * FROM channels WHERE id=?", (pkg["channel_id"],)).fetchone()
            version = conn.execute(
                "SELECT v.*,p.owner FROM versions v JOIN projects p ON p.id=v.project_id WHERE v.id=?",
                (pkg["version_id"],)).fetchone()
            if actor != version["owner"] and role not in {"admin"}:
                raise DomainError("只有项目负责人可以登记外部回执", 403)
            if pkg["kind"] != "channel":
                raise DomainError("遗留渠道记录不接受外部系统回执，请走确认接口", 409)
            existing = conn.execute("SELECT * FROM receipts WHERE package_id=?", (package_id,)).fetchone()
            if existing:
                return self._receipt_dict(existing, channel["code"]) | {"idempotent": True}
            if pkg["status"] != "packed":
                raise DomainError(f"包当前状态为 {pkg['status']}，无法签收", 409)
            lock = self._current_lock(conn, pkg["version_id"])
            if lock is None or lock["content_hash"] != pkg["content_hash"]:
                raise DomainError("包不属于当前锁定版本，渠道不能签收", 409)
            files = payload.get("files")
            if not isinstance(files, list) or not files:
                raise DomainError("回执必须带文件清单", 422)
            try:
                receipt_hashes = {str(f["name"]): str(f["sha256"]) for f in files}
            except (KeyError, TypeError) as exc:
                raise DomainError("回执文件清单格式不合法，需含 name 与 sha256", 422) from exc
            packed_files = json.loads(pkg["files"])
            packed_hashes = {f["name"]: f["sha256"] for f in packed_files}
            missing = sorted(set(packed_hashes) - set(receipt_hashes))
            extra = sorted(set(receipt_hashes) - set(packed_hashes))
            mismatched = sorted(n for n in packed_hashes.keys() & receipt_hashes
                                if receipt_hashes[n] != packed_hashes[n])
            if missing or extra or mismatched:
                details = {"missing": missing, "extra": extra, "mismatched": mismatched}
                self._audit(conn, actor, "receipt.rejected", "package", package_id, details)
                raise DomainError(
                    f"回执清单不符（缺文件 {missing or '无'}，摘要不符 {mismatched or '无'}，多余 {extra or '无'}），该渠道不能签收", 422)
            manifest_hash = str(payload.get("manifest_hash", "")).strip()
            if not manifest_hash or manifest_hash != pkg["manifest_hash"]:
                self._audit(conn, actor, "receipt.rejected", "package", package_id, {"reason": "manifest_hash"})
                raise DomainError("回执清单摘要不符，该渠道不能签收", 422)
            file_count = len(receipt_hashes)
            cur = conn.execute(
                """INSERT INTO receipts(package_id,external_ref,file_count,manifest_hash,files,raw,received_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (package_id, external_ref, file_count, manifest_hash,
                 json.dumps(files, ensure_ascii=False), str(payload.get("raw", "")), actor, utcnow()))
            self._audit(conn, actor, "receipt.recorded", "package", package_id,
                        {"channel": channel["code"], "external_ref": external_ref})
            row = conn.execute("SELECT * FROM receipts WHERE id=?", (cur.lastrowid,)).fetchone()
        return self._receipt_dict(row, channel["code"])

    @staticmethod
    def _receipt_dict(row: sqlite3.Row, channel_code: str) -> dict[str, Any]:
        d = dict(row)
        try:
            d["files"] = json.loads(d["files"])
        except (TypeError, json.JSONDecodeError):
            pass
        d["channel_code"] = channel_code
        return d

    # ---- legacy confirmation ---------------------------------------------------

    def confirm_legacy(self, package_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        """Confirm a backfilled legacy record against current knowledge. Until
        confirmed it is visible but excluded from delivery completion."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pkg = conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone()
            if not pkg:
                raise DomainError("包不存在", 404)
            version = conn.execute(
                "SELECT v.*,p.owner FROM versions v JOIN projects p ON p.id=v.project_id WHERE v.id=?",
                (pkg["version_id"],)).fetchone()
            if actor != version["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以确认遗留记录", 403)
            if pkg["kind"] != "legacy":
                raise DomainError("该包不是遗留渠道记录", 409)
            if not int(pkg["confirmed"]):
                conn.execute("UPDATE packages SET confirmed=1 WHERE id=?", (package_id,))
                conn.execute("UPDATE channels SET active=1 WHERE id=?", (pkg["channel_id"],))
                self._audit(conn, actor, "legacy.confirmed", "package", package_id,
                            {"legacy_delivery_id": pkg["legacy_delivery_id"]})
            row = conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone()
            channel = conn.execute("SELECT * FROM channels WHERE id=?", (row["channel_id"],)).fetchone()
        return self._package_dict(row, channel["code"])

    # ---- delivery state --------------------------------------------------------

    def delivery_state(self, version_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            version = self._version(conn, version_id)
            channels = conn.execute(
                "SELECT * FROM channels WHERE project_id=? ORDER BY id", (version["project_id"],)).fetchall()
            lock = self._current_lock(conn, version_id)
            current_hash = self._hash_payload(self._content_payload(conn, version))
            rows = conn.execute(
                "SELECT p.*, c.code channel_code, c.active channel_active FROM packages p JOIN channels c ON c.id=p.channel_id"
                " WHERE p.version_id=? ORDER BY p.id", (version_id,)).fetchall()
            channel_states = []
            done = 0
            counted = 0
            for ch in channels:
                accepted = None
                pending_legacy = False
                for p in rows:
                    if p["channel_code"] != ch["code"]:
                        continue
                    is_current = lock is not None and p["content_hash"] == lock["content_hash"]
                    signed = conn.execute("SELECT 1 FROM receipts WHERE package_id=?", (p["id"],)).fetchone()
                    if signed and p["status"] == "packed" and is_current and p["content_hash"] == current_hash:
                        accepted = self._package_dict(p, ch["code"])
                        break
                    if p["kind"] == "legacy" and int(p["confirmed"]):
                        accepted = self._package_dict(p, ch["code"])
                        break
                    if p["kind"] == "legacy":
                        pending_legacy = True
                # An inactive channel holding only unconfirmed legacy records
                # is excluded from the denominator until confirmation.
                counts = not (ch["code"] == "legacy" and pending_legacy)
                if accepted:
                    done += 1
                if counts:
                    counted += 1
                latest = None
                for p in rows:
                    if p["channel_code"] == ch["code"]:
                        latest = self._package_dict(p, ch["code"])
                channel_states.append({"channel": self._channel_dict(ch), "delivered": accepted is not None,
                                       "pending_legacy_confirmation": pending_legacy and accepted is None,
                                       "counts_into_completion": counts,
                                       "accepted_package": accepted, "latest_package": latest})
            complete = counted > 0 and done == counted
            if complete and version["status"] == "locked":
                conn.execute("UPDATE versions SET status='delivered',updated_at=? WHERE id=?", (utcnow(), version_id))
                new_status = "delivered"
            else:
                new_status = version["status"]
        return {"version_id": version_id, "status": new_status, "complete": complete,
                "channels_done": done, "channels_total": counted,
                "current_lock": {"lock_no": lock["lock_no"], "content_hash": lock["content_hash"]} if lock else None,
                "content_changed": bool(lock and lock["content_hash"] != current_hash),
                "channels": channel_states}

    def list_packages(self, version_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            sql = ("SELECT p.*, c.code channel_code FROM packages p JOIN channels c ON c.id=p.channel_id")
            args: tuple[Any, ...] = ()
            if version_id is not None:
                sql += " WHERE p.version_id=?"
                args = (version_id,)
            sql += " ORDER BY p.id"
            return [self._package_dict(r, r["channel_code"]) for r in conn.execute(sql, args).fetchall()]

    def list_batches(self, version_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if version_id is None:
                rows = conn.execute("SELECT * FROM package_batches ORDER BY id DESC").fetchall()
            else:
                rows = conn.execute("SELECT * FROM package_batches WHERE version_id=? ORDER BY id DESC", (version_id,)).fetchall()
            return [dict(r) for r in rows]

    def list_receipts(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT r.*, c.code channel_code FROM receipts r JOIN packages p ON p.id=r.package_id"
                " JOIN channels c ON c.id=p.channel_id ORDER BY r.id DESC").fetchall()
            return [self._receipt_dict(r, r["channel_code"]) for r in rows]

    def list_projects(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM projects ORDER BY id").fetchall()]

    def list_versions(self, project_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if project_id:
                rows = conn.execute("SELECT * FROM versions WHERE project_id=? ORDER BY id", (project_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM versions ORDER BY id").fetchall()
            return [dict(r) for r in rows]

    def list_cues(self, version_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM cues WHERE version_id=? ORDER BY cue_index", (version_id,)).fetchall()]

    def list_comments(self, version_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM comments WHERE version_id=? ORDER BY id", (version_id,)).fetchall()]

    def list_deliveries(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM deliveries ORDER BY id DESC").fetchall()]

    def audit(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM audit_log ORDER BY id DESC").fetchall()]


def seed_demo(db: Database) -> dict[str, int]:
    if db.list_projects():
        return {"project": int(db.list_projects()[0]["id"])}
    project = db.create_project("alice", {"name": "极地纪录片字幕", "source_language": "en", "media_name": "polar.mp4", "media_sha256": "b" * 64, "duration_ms": 120000}, "owner")
    db.set_glossary(project["id"], "alice", {"source_term": "seal", "required_translation": "海豹", "forbidden_terms": ["密封"], "notes": "动物学语境"}, "owner")
    db.upsert_channel(project["id"], "alice", {"code": "stream-a", "name": "流媒体A台", "subtitle_format": "vtt", "max_cues": 500}, "owner")
    db.upsert_channel(project["id"], "alice", {"code": "broadcast-b", "name": "广电B频道", "subtitle_format": "srt", "max_cues": 300}, "owner")
    db.upsert_channel(project["id"], "alice", {"code": "cinema-c", "name": "院线C", "subtitle_format": "ttml"}, "owner")
    version = db.create_version(project["id"], "alice", {"language": "zh-CN"}, "owner")
    return {"project": int(project["id"]), "version": int(version["id"])}


class Handler(BaseHTTPRequestHandler):
    db: Database
    server_version = "SubtitleQC/1.0"

    def _send(self, payload: Any, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _html(self) -> None:
        data = (ROOT / "static" / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise DomainError("请求体不是合法 JSON") from exc

    def _auth(self) -> tuple[str, str]:
        return self.headers.get("X-User", "anonymous"), self.headers.get("X-Role", "viewer")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"/", "/index.html"}:
                return self._html()
            if parsed.path == "/api/health":
                return self._send({"ok": True})
            if parsed.path == "/api/projects":
                return self._send({"projects": self.db.list_projects()})
            if parsed.path == "/api/versions":
                return self._send({"versions": self.db.list_versions()})
            if parsed.path == "/api/deliveries":
                return self._send({"deliveries": self.db.list_deliveries()})
            if parsed.path == "/api/packages":
                return self._send({"packages": self.db.list_packages()})
            if parsed.path == "/api/batches":
                return self._send({"batches": self.db.list_batches()})
            if parsed.path == "/api/receipts":
                return self._send({"receipts": self.db.list_receipts()})
            if parsed.path == "/api/audit":
                return self._send({"audit": self.db.audit()})
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[3] == "channels":
                return self._send({"channels": self.db.list_channels(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "cues":
                return self._send({"cues": self.db.list_cues(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "comments":
                return self._send({"comments": self.db.list_comments(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "packages":
                return self._send({"packages": self.db.list_packages(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "batches":
                return self._send({"batches": self.db.list_batches(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "delivery-state":
                return self._send(self.db.delivery_state(int(parts[2])))
            raise DomainError("接口不存在", 404)
        except (ValueError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            actor, role = self._auth()
            body = self._body()
            parts = [p for p in parsed.path.split("/") if p]
            if parts == ["api", "projects"]:
                return self._send(self.db.create_project(actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[3] == "versions":
                return self._send(self.db.create_version(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[3] == "glossary":
                return self._send(self.db.set_glossary(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[3] == "channels":
                return self._send(self.db.upsert_channel(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "assignments":
                return self._send(self.db.assign(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "cues":
                return self._send(self.db.save_cue(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "comments":
                return self._send(self.db.add_comment(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] in {"submit", "lock", "deliver", "unlock", "packages"}:
                version_id = int(parts[2])
                if parts[3] == "submit":
                    return self._send(self.db.submit(version_id, actor, role))
                if parts[3] == "lock":
                    return self._send(self.db.lock(version_id, actor, role))
                if parts[3] == "unlock":
                    return self._send(self.db.unlock(version_id, actor, role))
                if parts[3] == "packages":
                    return self._send(self.db.request_packages(version_id, actor, body, role), 202)
                return self._send(self.db.deliver(version_id, actor, role))
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "review":
                return self._send(self.db.review(int(parts[2]), actor, body, role))
            if len(parts) == 4 and parts[:2] == ["api", "packages"] and parts[3] == "receipts":
                return self._send(self.db.record_receipt(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "packages"] and parts[3] == "confirm-legacy":
                return self._send(self.db.confirm_legacy(int(parts[2]), actor, role))
            raise DomainError("接口不存在", 404)
        except (ValueError, TypeError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[subtitle] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="字幕本地化质检与交付服务")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8009")))
    parser.add_argument("--db", default=os.getenv("SUBTITLE_DB", str(DEFAULT_DB)))
    parser.add_argument("--init", action="store_true", help="创建数据库和示例项目")
    args = parser.parse_args()
    db = Database(args.db)
    if args.init:
        seed = seed_demo(db)
        print(f"initialized database at {args.db}; project={seed['project']} version={seed['version']}")
        return
    Handler.db = db
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"subtitle-qc listening on http://127.0.0.1:{args.port} (db={args.db})")
    server.serve_forever()


if __name__ == "__main__":
    main()
