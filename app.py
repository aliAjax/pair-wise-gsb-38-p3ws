"""Subtitle localization quality-control and delivery service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
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
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB):
        self.path = str(path)
        self._init_schema()

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
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    format TEXT NOT NULL CHECK(format IN ('srt','vtt','ass')),
                    max_chars_per_cue INTEGER NOT NULL CHECK(max_chars_per_cue > 0),
                    min_display_ms INTEGER NOT NULL DEFAULT 0 CHECK(min_display_ms >= 0),
                    kind TEXT NOT NULL DEFAULT 'channel' CHECK(kind IN ('channel','legacy')),
                    created_at TEXT NOT NULL,
                    UNIQUE(project_id,code)
                );
                CREATE TABLE IF NOT EXISTS locks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id INTEGER NOT NULL UNIQUE REFERENCES versions(id),
                    project_id INTEGER NOT NULL REFERENCES projects(id),
                    language TEXT NOT NULL,
                    cues_hash TEXT NOT NULL,
                    glossary_hash TEXT NOT NULL,
                    is_current INTEGER NOT NULL DEFAULT 1,
                    locked_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS packages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lock_id INTEGER NOT NULL REFERENCES locks(id),
                    channel_id INTEGER NOT NULL REFERENCES channels(id),
                    cues_hash TEXT NOT NULL,
                    glossary_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN
                        ('packing','ready','failed','invalidated','superseded','signed','legacy')),
                    attempt INTEGER NOT NULL DEFAULT 1,
                    manifest TEXT,
                    manifest_digest TEXT,
                    artifact TEXT,
                    fail_reason TEXT,
                    legacy INTEGER NOT NULL DEFAULT 0,
                    confirmed INTEGER NOT NULL DEFAULT 1,
                    packed_by TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    signed_at TEXT,
                    UNIQUE(lock_id,channel_id)
                );
                CREATE TABLE IF NOT EXISTS receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    package_id INTEGER NOT NULL REFERENCES packages(id),
                    system_name TEXT NOT NULL DEFAULT '',
                    manifest_digest TEXT NOT NULL DEFAULT '',
                    accepted INTEGER NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
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
                """
            )
            self._backfill_legacy(conn)

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

    # ------------------------------------------------------------------
    # 锁定指纹 / 清单摘要 / 渠道字幕格式
    # ------------------------------------------------------------------
    @staticmethod
    def _canonical(payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _digest(self, payload: Any) -> str:
        return hashlib.sha256(self._canonical(payload).encode()).hexdigest()

    def _cues_payload(self, conn: sqlite3.Connection, version_id: int) -> list[dict[str, Any]]:
        return [dict(r) for r in conn.execute(
            "SELECT cue_index,start_ms,end_ms,text FROM cues WHERE version_id=? ORDER BY cue_index",
            (version_id,))]

    def _glossary_payload(self, conn: sqlite3.Connection, project_id: int) -> list[dict[str, Any]]:
        return [dict(r) for r in conn.execute(
            "SELECT source_term,required_translation,forbidden_terms FROM glossaries WHERE project_id=? ORDER BY source_term",
            (project_id,))]

    def _content_hashes(self, conn: sqlite3.Connection, version: sqlite3.Row) -> tuple[str, str]:
        return (self._digest(self._cues_payload(conn, version["id"])),
                self._digest(self._glossary_payload(conn, version["project_id"])))

    @staticmethod
    def _fmt_srt_time(ms: int) -> str:
        h, rem = divmod(ms, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, milli = divmod(rem, 1000)
        return f"{h:02d}:{m:02d}:{s:02d},{milli:03d}"

    @staticmethod
    def _fmt_vtt_time(ms: int) -> str:
        h, rem = divmod(ms, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, milli = divmod(rem, 1000)
        return f"{h:02d}:{m:02d}:{s:02d}.{milli:03d}"

    @classmethod
    def _fmt_ass_time(cls, ms: int) -> str:
        h, rem = divmod(ms, 3_600_000)
        m, rem = divmod(rem, 60_000)
        s, milli = divmod(rem, 1000)
        return f"{h:d}:{m:02d}:{s:02d}.{milli // 10:02d}"

    def render_subtitles(self, fmt: str, cues: list[dict[str, Any]]) -> str:
        fmt = fmt.lower()
        if fmt == "srt":
            blocks = []
            for i, cue in enumerate(cues, 1):
                blocks.append(f"{i}\n{self._fmt_srt_time(int(cue['start_ms']))} --> {self._fmt_srt_time(int(cue['end_ms']))}\n{cue['text']}")
            return "\n\n".join(blocks) + "\n"
        if fmt == "vtt":
            blocks = ["WEBVTT"]
            for cue in cues:
                blocks.append(f"{self._fmt_vtt_time(int(cue['start_ms']))} --> {self._fmt_vtt_time(int(cue['end_ms']))}\n{cue['text']}")
            return "\n\n".join(blocks) + "\n"
        if fmt == "ass":
            head = "[Script Info]\nScriptType: v4.00+\n\n[V4+ Styles]\nFormat: Name, Fontname\nStyle: Default,Arial\n\n[Events]\nFormat: Layer, Start, End, Text\n"
            lines = [head]
            for cue in cues:
                text = str(cue["text"]).replace("\n", "\\N")
                lines.append(f"Dialogue: 0,{self._fmt_ass_time(int(cue['start_ms']))},{self._fmt_ass_time(int(cue['end_ms']))},Default,,0,0,0,,{text}\n")
            return "".join(lines)
        raise DomainError(f"不支持的字幕格式: {fmt}")

    def _backfill_legacy(self, conn: sqlite3.Connection) -> None:
        """历史交付按现有 delivery 快照补成遗留渠道记录；确认前不进交片完成度。"""
        rows = conn.execute(
            """SELECT d.*, v.project_id, v.language FROM deliveries d JOIN versions v ON v.id=d.version_id
               WHERE NOT EXISTS (SELECT 1 FROM packages pk JOIN locks l ON l.id=pk.lock_id
                                 WHERE l.version_id=d.version_id AND pk.legacy=1)"""
        ).fetchall()
        for d in rows:
            now = d["created_at"]
            ch = conn.execute(
                "INSERT OR IGNORE INTO channels(project_id,code,name,format,max_chars_per_cue,min_display_ms,kind,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (d["project_id"], "legacy", "历史交付渠道", "srt", 10_000, 0, "legacy", now),
            )
            channel = conn.execute("SELECT * FROM channels WHERE project_id=? AND kind='legacy'", (d["project_id"],)).fetchone()
            old = conn.execute("SELECT * FROM locks WHERE version_id=?", (d["version_id"],)).fetchone()
            if old:
                conn.execute("UPDATE locks SET is_current=0 WHERE id=?", (old["id"],))
            cur = conn.execute(
                """INSERT INTO locks(version_id,project_id,language,cues_hash,glossary_hash,is_current,locked_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (d["version_id"], d["project_id"], d["language"], "", "", 0, d["delivered_by"], now),
            )
            lock_id = int(cur.lastrowid)
            cues = json.loads(d["manifest"]).get("cues", [])
            cues_hash = self._digest(cues)
            glossary_hash = self._digest(json.loads(d["manifest"]).get("glossary", []))
            conn.execute("UPDATE locks SET cues_hash=?,glossary_hash=? WHERE id=?", (cues_hash, glossary_hash, lock_id))
            file_hash = self._digest(d["manifest"])
            manifest = {
                "project_id": d["project_id"], "version_id": d["version_id"],
                "language": d["language"], "channel_id": channel["id"], "channel_code": "legacy",
                "kind": "legacy", "cues_hash": cues_hash, "glossary_hash": glossary_hash,
                "files": [{"path": "delivery_snapshot.json", "sha256": file_hash}],
            }
            conn.execute(
                """INSERT INTO packages(lock_id,channel_id,cues_hash,glossary_hash,status,attempt,
                   manifest,manifest_digest,artifact,fail_reason,legacy,confirmed,packed_by,created_at,updated_at,signed_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (lock_id, channel["id"], cues_hash, glossary_hash, "legacy", 1,
                 self._canonical(manifest), d["snapshot_hash"],
                 json.dumps({"delivery_snapshot.json": d["manifest"]}, ensure_ascii=False),
                 None, 1, 0, d["delivered_by"], now, now, None),
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
            # 术语改动让当前锁定版本上尚未签收的包失效；已签收包保留原清单。
            locks = conn.execute("SELECT * FROM locks WHERE project_id=? AND is_current=1", (project_id,)).fetchall()
            for lock in locks:
                cur = conn.execute(
                    "UPDATE packages SET status='invalidated',updated_at=? WHERE lock_id=? AND status IN ('ready','failed','invalidated')",
                    (utcnow(), lock["id"]),
                )
                if cur.rowcount:
                    self._audit(conn, actor, "package.invalidated", "lock", lock["id"], {"reason": "glossary"})
            self._audit(conn, actor, "glossary.saved", "project", project_id, {"source_term": source_term})
        return {"project_id": project_id, "source_term": source_term, "required_translation": required, "forbidden_terms": forbidden}

    def create_channel(self, project_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        code = str(payload.get("code", "")).strip()
        name = str(payload.get("name", "")).strip()
        fmt = str(payload.get("format", "")).strip().lower()
        try:
            max_chars = int(payload.get("max_chars_per_cue", 42))
            min_display = int(payload.get("min_display_ms", 0))
        except (TypeError, ValueError) as exc:
            raise DomainError("渠道规格中的字数和时长必须是整数") from exc
        if not code or not name or fmt not in {"srt", "vtt", "ass"} or max_chars <= 0 or min_display < 0:
            raise DomainError("渠道编码、名称、格式或规格不合法")
        with self.connect() as conn:
            project = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
            if not project:
                raise DomainError("项目不存在", 404)
            if actor != project["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以维护渠道规则", 403)
            try:
                cur = conn.execute(
                    "INSERT INTO channels(project_id,code,name,format,max_chars_per_cue,min_display_ms,kind,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (project_id, code, name, fmt, max_chars, min_display, "channel", utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("渠道编码已存在", 409) from exc
            self._audit(conn, actor, "channel.created", "project", project_id, {"code": code, "format": fmt})
            return dict(conn.execute("SELECT * FROM channels WHERE id=?", (cur.lastrowid,)).fetchone())

    def list_channels(self, project_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if project_id:
                rows = conn.execute("SELECT * FROM channels WHERE project_id=? ORDER BY id", (project_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM channels ORDER BY id").fetchall()
            return [dict(r) for r in rows]

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
            existing = conn.execute("SELECT * FROM locks WHERE version_id=?", (version_id,)).fetchone()
            if existing:
                raise DomainError("该版本已经锁定，锁定指纹不可覆盖", 409)
            cues_hash, glossary_hash = self._content_hashes(conn, version)
            # 同语言的旧锁定版本让出“当前锁定版本”位置。
            conn.execute(
                "UPDATE locks SET is_current=0 WHERE project_id=? AND language=? AND is_current=1",
                (version["project_id"], version["language"]),
            )
            # 旧锁定版本上尚未签收的包随之作废；已签收包保留原清单。
            stale = conn.execute(
                """SELECT pk.id,pk.lock_id FROM packages pk JOIN locks l ON l.id=pk.lock_id
                   WHERE l.project_id=? AND l.language=? AND l.is_current=0
                     AND pk.status IN ('ready','failed','invalidated','packing')""",
                (version["project_id"], version["language"]),
            ).fetchall()
            for row in stale:
                conn.execute("UPDATE packages SET status='superseded',updated_at=? WHERE id=?", (utcnow(), row["id"]))
            cur = conn.execute(
                """INSERT INTO locks(version_id,project_id,language,cues_hash,glossary_hash,is_current,locked_by,created_at)
                   VALUES(?,?,?,?,?,1,?,?)""",
                (version_id, version["project_id"], version["language"], cues_hash, glossary_hash, actor, utcnow()),
            )
            conn.execute("UPDATE versions SET status='locked',updated_at=? WHERE id=?", (utcnow(), version_id))
            self._audit(conn, actor, "version.locked", "version", version_id,
                        {"lock_id": cur.lastrowid, "cues_hash": cues_hash, "glossary_hash": glossary_hash})
            lock_row = dict(conn.execute("SELECT * FROM locks WHERE id=?", (cur.lastrowid,)).fetchone())
            version_row = dict(conn.execute("SELECT * FROM versions WHERE id=?", (version_id,)).fetchone())
            return {"lock": lock_row, "version": version_row}

    # ------------------------------------------------------------------
    # 交片线：渠道独立包 / 批次占位 / 回执
    # ------------------------------------------------------------------
    def _get_lock_version(self, conn: sqlite3.Connection, version_id: int) -> sqlite3.Row:
        version = self._version(conn, version_id)
        lock = conn.execute("SELECT * FROM locks WHERE version_id=?", (version_id,)).fetchone()
        if not lock:
            raise DomainError("版本尚未锁定，不能压包", 409)
        if not lock["is_current"]:
            raise DomainError("该锁定版本已被同语言新版本取代，渠道只认当前锁定版本", 409)
        return lock

    def _ensure_pack_permission(self, conn: sqlite3.Connection, version: sqlite3.Row, actor: str, role: str) -> None:
        if actor != version["owner"] and role != "admin":
            raise DomainError("只有项目负责人可以压包交付", 403)

    def _build_package(self, conn: sqlite3.Connection, lock: sqlite3.Row, channel: sqlite3.Row,
                       cues_hash: str, glossary_hash: str) -> tuple[dict[str, Any], str, dict[str, str]]:
        cues = self._cues_payload(conn, lock["version_id"])
        if not cues:
            raise DomainError("锁定版本没有字幕，无法压包")
        for cue in cues:
            length = sum(1 for ch in cue["text"] if not ch.isspace())
            if length > int(channel["max_chars_per_cue"]):
                raise DomainError(
                    f"渠道 {channel['code']} 规格不符：第 {cue['cue_index']} 条字幕 {length} 字，超出 {channel['max_chars_per_cue']} 字上限", 422)
            display = int(cue["end_ms"]) - int(cue["start_ms"])
            if display < int(channel["min_display_ms"]):
                raise DomainError(
                    f"渠道 {channel['code']} 规格不符：第 {cue['cue_index']} 条字幕显示时长不足 {channel['min_display_ms']}ms", 422)
        sub_text = self.render_subtitles(channel["format"], cues)
        sub_name = f"subtitles.{channel['format']}"
        files_payload = [
            {"path": sub_name, "format": channel["format"], "cues": len(cues),
             "sha256": hashlib.sha256(sub_text.encode()).hexdigest()},
        ]
        manifest = {
            "project_id": lock["project_id"], "version_id": lock["version_id"],
            "language": lock["language"], "channel_id": channel["id"], "channel_code": channel["code"],
            "cues_hash": cues_hash, "glossary_hash": glossary_hash, "files": files_payload,
        }
        # 顶层摘要覆盖清单本身与每个文件，外部系统逐项核对。
        manifest_digest = self._digest({"manifest": manifest, "files": {sub_name: hashlib.sha256(sub_text.encode()).hexdigest()}})
        artifact = {sub_name: sub_text, "manifest.json": self._canonical(manifest)}
        return manifest, manifest_digest, artifact

    def pack_channel(self, version_id: int, channel_code: str, actor: str, role: str = "viewer") -> dict[str, Any]:
        """同一渠道同一锁定版本只压一次；并发时后来者拿到先到者的占位结果。"""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            version = self._version(conn, version_id)
            self._ensure_pack_permission(conn, version, actor, role)
            lock = self._get_lock_version(conn, version_id)
            channel = conn.execute(
                "SELECT * FROM channels WHERE project_id=? AND code=? AND kind='channel'",
                (lock["project_id"], channel_code),
            ).fetchone()
            if not channel:
                raise DomainError(f"渠道 {channel_code} 的规则不存在", 404)
            now = utcnow()
            existing = conn.execute("SELECT * FROM packages WHERE lock_id=? AND channel_id=?", (lock["id"], channel["id"])).fetchone()
            if existing:
                if existing["status"] in {"ready", "packing", "signed"}:
                    return {"package": dict(existing), "deduped": True}
                if existing["status"] == "superseded":
                    raise DomainError("该锁定版本已被取代，不能续压", 409)
                # failed / invalidated：从缺口继续，沿用同一个包位；
                # 若规格仍不符，异常直接抛出，包保持 failed，失败原因已在首次失败时留痕。
                attempt = int(existing["attempt"]) + 1
                manifest, digest, artifact = self._build_package(
                    conn, lock, channel, existing["cues_hash"], existing["glossary_hash"])
                conn.execute(
                    """UPDATE packages SET status='ready',attempt=?,manifest=?,manifest_digest=?,artifact=?,
                       fail_reason=NULL,packed_by=?,updated_at=? WHERE id=?""",
                    (attempt, self._canonical(manifest), digest, json.dumps(artifact, ensure_ascii=False), actor, now, existing["id"]),
                )
                self._audit(conn, actor, "channel.packed", "package", existing["id"],
                            {"channel": channel_code, "attempt": attempt})
                return {"package": dict(conn.execute("SELECT * FROM packages WHERE id=?", (existing["id"],)).fetchone()), "deduped": False}
            cur = conn.execute(
                """INSERT INTO packages(lock_id,channel_id,cues_hash,glossary_hash,status,attempt,
                   manifest,fail_reason,legacy,confirmed,packed_by,created_at,updated_at)
                   VALUES(?,?,?,?,'packing',1,NULL,NULL,0,1,?,?,?)""",
                (lock["id"], channel["id"], lock["cues_hash"], lock["glossary_hash"], actor, now, now),
            )
            pkg_id = cur.lastrowid
            try:
                manifest, digest, artifact = self._build_package(conn, lock, channel, lock["cues_hash"], lock["glossary_hash"])
            except DomainError as exc:
                conn.execute(
                    "UPDATE packages SET status='failed',fail_reason=?,updated_at=? WHERE id=?",
                    (str(exc), utcnow(), pkg_id),
                )
                self._audit(conn, actor, "channel.pack_failed", "package", pkg_id,
                            {"channel": channel_code, "reason": str(exc)})
                conn.commit()  # 失败痕迹必须保留，随后抛出的业务异常不能回滚它。
                raise
            conn.execute(
                "UPDATE packages SET status='ready',manifest=?,manifest_digest=?,artifact=?,updated_at=? WHERE id=?",
                (self._canonical(manifest), digest, json.dumps(artifact, ensure_ascii=False), utcnow(), pkg_id),
            )
            self._audit(conn, actor, "channel.packed", "package", pkg_id, {"channel": channel_code, "attempt": 1})
            return {"package": dict(conn.execute("SELECT * FROM packages WHERE id=?", (pkg_id,)).fetchone()), "deduped": False}

    def pack_batch(self, version_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        """整批压包：已完成渠道保留，失败渠道之后再次调用即可续压。"""
        with self.connect() as conn:
            version = self._version(conn, version_id)
            lock_row = conn.execute("SELECT * FROM locks WHERE version_id=?", (version_id,)).fetchone()
        if not lock_row:
            raise DomainError("版本尚未锁定，不能压包", 409)
        if not lock_row["is_current"]:
            raise DomainError("该锁定版本已被同语言新版本取代", 409)
        channels = self.list_channels(int(lock_row["project_id"]))
        channels = [c for c in channels if c["kind"] == "channel"]
        if not channels:
            raise DomainError("项目还没有渠道规则，无法打包", 409)
        packed, skipped, failed = [], [], []
        for channel in channels:
            try:
                result = self.pack_channel(version_id, channel["code"], actor, role)
                (skipped if result.get("deduped") else packed).append(channel["code"])
            except DomainError as exc:
                failed.append({"channel": channel["code"], "error": str(exc), "status": getattr(exc, "status", 400)})
                # 保留此前已完成的渠道，批次在缺口处停下，后续调用从该渠道继续。
                break
        done = set(packed) | set(skipped) | {f["channel"] for f in failed}
        remaining = [c["code"] for c in channels if c["code"] not in done]
        return {"version_id": version_id, "packed": packed, "deduped": skipped,
                "failed": failed, "remaining": remaining}

    def submit_receipt(self, package_id: int, actor: str, payload: dict[str, Any], role: str = "viewer") -> dict[str, Any]:
        """外部系统回执：按清单摘要逐项核对，缺文件或摘要不符都签不了收。"""
        system_name = str(payload.get("system_name", "")).strip()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            package = conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone()
            if not package:
                raise DomainError("包不存在", 404)
            now = utcnow()
            if package["status"] == "signed":
                receipt = conn.execute("SELECT * FROM receipts WHERE package_id=? AND accepted=1 ORDER BY id DESC LIMIT 1", (package_id,)).fetchone()
                return {"package": dict(package), "receipt": dict(receipt) if receipt else None, "deduped": True}
            if package["status"] not in {"ready"}:
                reason = f"包状态为 {package['status']}，不能签收"
                cur = conn.execute(
                    "INSERT INTO receipts(package_id,system_name,manifest_digest,accepted,reason,created_at) VALUES(?,?,?,?,?,?)",
                    (package_id, system_name, str(payload.get("manifest_digest", "")), 0, reason, now),
                )
                self._audit(conn, actor, "receipt.rejected", "package", package_id, {"reason": reason})
                conn.commit()  # 被拒回执要留痕，不能随业务异常回滚。
                raise DomainError(reason, 409)
            stored_manifest = json.loads(package["manifest"])
            stored_digest = package["manifest_digest"]
            reported_digest = str(payload.get("manifest_digest", "")).strip().lower()
            reported_files = payload.get("files")
            problems: list[str] = []
            if not isinstance(reported_files, dict):
                problems.append("回执缺少文件清单 files")
                reported_files = {}
            for entry in stored_manifest["files"]:
                path = entry["path"]
                if path not in reported_files:
                    problems.append(f"缺少文件: {path}")
                elif str(reported_files[path]).strip().lower() != entry["sha256"]:
                    problems.append(f"文件摘要不符: {path}")
            for path in reported_files:
                if not any(f["path"] == path for f in stored_manifest["files"]):
                    problems.append(f"回执包含清单外文件: {path}")
            if not reported_digest or reported_digest != stored_digest:
                problems.append("清单摘要不符")
            if problems:
                reason = "；".join(problems)
                conn.execute(
                    "INSERT INTO receipts(package_id,system_name,manifest_digest,accepted,reason,created_at) VALUES(?,?,?,?,?,?)",
                    (package_id, system_name, reported_digest, 0, reason, now),
                )
                self._audit(conn, actor, "receipt.rejected", "package", package_id, {"problems": problems})
                conn.commit()  # 缺文件/摘要不符的被拒回执要留痕。
                raise DomainError(f"外部回执核验失败：{reason}", 409)
            cur = conn.execute(
                "INSERT INTO receipts(package_id,system_name,manifest_digest,accepted,reason,created_at) VALUES(?,?,?,1,'',?)",
                (package_id, system_name, reported_digest, now),
            )
            conn.execute("UPDATE packages SET status='signed',signed_at=?,updated_at=? WHERE id=?", (now, now, package_id))
            self._audit(conn, actor, "receipt.accepted", "package", package_id, {"system": system_name})
            return {"package": dict(conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone()),
                    "receipt": dict(conn.execute("SELECT * FROM receipts WHERE id=?", (cur.lastrowid,)).fetchone())}

    def confirm_legacy_package(self, package_id: int, actor: str, role: str = "viewer") -> dict[str, Any]:
        """历史交付补出的遗留记录，需要负责人核对原快照后手动确认才进完成度。"""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            package = conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone()
            if not package or not package["legacy"]:
                raise DomainError("遗留渠道记录不存在", 404)
            lock = conn.execute("SELECT * FROM locks WHERE id=?", (package["lock_id"],)).fetchone()
            project = conn.execute("SELECT * FROM projects WHERE id=?", (lock["project_id"],)).fetchone()
            if actor != project["owner"] and role != "admin":
                raise DomainError("只有项目负责人可以确认遗留交付", 403)
            delivery = conn.execute("SELECT * FROM deliveries WHERE version_id=?", (lock["version_id"],)).fetchone()
            expected = delivery["snapshot_hash"] if delivery else None
            if expected != package["manifest_digest"]:
                raise DomainError("遗留快照摘要不符，不能确认", 409)
            conn.execute("UPDATE packages SET confirmed=1,updated_at=? WHERE id=?", (utcnow(), package_id))
            self._audit(conn, actor, "legacy.confirmed", "package", package_id, {})
            return {"package": dict(conn.execute("SELECT * FROM packages WHERE id=?", (package_id,)).fetchone())}

    def delivery_status(self, version_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            version = self._version(conn, version_id)
            lock = conn.execute("SELECT * FROM locks WHERE version_id=?", (version_id,)).fetchone()
            if not lock:
                return {"version_id": version_id, "locked": False, "is_current": False,
                        "total_channels": 0, "signed_channels": 0, "packages": [],
                        "complete": False, "completion": 0}
            rows = conn.execute(
                """SELECT pk.*,c.code channel_code,c.name channel_name,c.kind channel_kind
                   FROM packages pk JOIN channels c ON c.id=pk.channel_id
                   WHERE pk.lock_id=? ORDER BY c.id""", (lock["id"],)).fetchall()
            packages = [dict(r) for r in rows]
            regular = [p for p in packages if p["channel_kind"] == "channel"]
            legacy = [p for p in packages if p["channel_kind"] == "legacy"]
            signed = [p for p in regular if p["status"] == "signed"]
            # 当前锁定版本以项目全部普通渠道为分母（未建包的新渠道也算未交）；
            # 已被取代的历史锁只看当时实际建过的包。
            if lock["is_current"]:
                total_channels = int(conn.execute(
                    "SELECT COUNT(*) n FROM channels WHERE project_id=? AND kind='channel'",
                    (lock["project_id"],)).fetchone()["n"])
            else:
                total_channels = len(regular)
            # 遗留渠道记录确认后才允许进入该历史版本的完成度；未确认单列。
            confirmed_legacy = [p for p in legacy if p["confirmed"]]
            total = total_channels + len(confirmed_legacy)
            signed_count = len(signed) + len(confirmed_legacy)
            complete = total > 0 and signed_count == total
            return {
                "version_id": version_id, "locked": True, "is_current": bool(lock["is_current"]),
                "lock_id": lock["id"], "total_channels": total,
                "signed_channels": signed_count,
                "completion": round(signed_count / total, 4) if total else 0,
                "complete": complete,
                "legacy_pending": [p["id"] for p in legacy if not p["confirmed"]],
                "packages": packages,
            }

    def project_delivery_status(self, project_id: int) -> dict[str, Any]:
        versions = self.list_versions(project_id)
        per_version = [self.delivery_status(v["id"]) for v in versions]
        current = [s for s in per_version if s.get("is_current")]
        all_complete = bool(current) and all(s["complete"] for s in current)
        return {"project_id": project_id, "complete": all_complete, "versions": per_version}

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
            # 交片线门槛：当前锁定版本的每个渠道都必须有已签收包，整版才算已交。
            if version["status"] == "locked":
                lock = conn.execute("SELECT * FROM locks WHERE version_id=? AND is_current=1", (version_id,)).fetchone()
                channels = conn.execute("SELECT COUNT(*) n FROM channels WHERE project_id=? AND kind='channel'", (version["project_id"],)).fetchone()["n"]
                signed = conn.execute("SELECT COUNT(*) n FROM packages WHERE lock_id=? AND status='signed'", (lock["id"],)).fetchone()["n"] if lock else 0
                if not lock or channels == 0:
                    raise DomainError("交片线尚未建立渠道包，整版不能标记已交", 409)
                if signed < channels:
                    raise DomainError(f"尚有 {channels - signed} 个渠道未签收，整版不能显示已交", 409)
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

    def list_packages(self, version_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if version_id:
                rows = conn.execute(
                    """SELECT pk.*,c.code channel_code,c.name channel_name,c.format,c.kind channel_kind
                       FROM packages pk JOIN channels c ON c.id=pk.channel_id JOIN locks l ON l.id=pk.lock_id
                       WHERE l.version_id=? ORDER BY pk.id""", (version_id,)).fetchall()
            else:
                rows = conn.execute(
                    """SELECT pk.*,c.code channel_code,c.name channel_name,c.format,c.kind channel_kind
                       FROM packages pk JOIN channels c ON c.id=pk.channel_id ORDER BY pk.id DESC""").fetchall()
            return [dict(r) for r in rows]

    def get_package(self, package_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT pk.*,c.code channel_code,c.name channel_name,c.format,c.kind channel_kind
                   FROM packages pk JOIN channels c ON c.id=pk.channel_id WHERE pk.id=?""",
                (package_id,)).fetchone()
            if not row:
                raise DomainError("包不存在", 404)
            return dict(row)

    def audit(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM audit_log ORDER BY id DESC").fetchall()]


def seed_demo(db: Database) -> dict[str, int]:
    if db.list_projects():
        return {"project": int(db.list_projects()[0]["id"])}
    project = db.create_project("alice", {"name": "极地纪录片字幕", "source_language": "en", "media_name": "polar.mp4", "media_sha256": "b" * 64, "duration_ms": 120000}, "owner")
    db.set_glossary(project["id"], "alice", {"source_term": "seal", "required_translation": "海豹", "forbidden_terms": ["密封"], "notes": "动物学语境"}, "owner")
    db.create_channel(project["id"], "alice", {"code": "cinema", "name": "院线数字拷贝", "format": "ass", "max_chars_per_cue": 30, "min_display_ms": 1000}, "owner")
    db.create_channel(project["id"], "alice", {"code": "streaming", "name": "流媒体平台", "format": "srt", "max_chars_per_cue": 42, "min_display_ms": 500}, "owner")
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
            if parsed.path == "/api/channels":
                return self._send({"channels": self.db.list_channels()})
            if parsed.path == "/api/audit":
                return self._send({"audit": self.db.audit()})
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "cues":
                return self._send({"cues": self.db.list_cues(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "comments":
                return self._send({"comments": self.db.list_comments(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "packages":
                return self._send({"packages": self.db.list_packages(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "delivery-status":
                return self._send(self.db.delivery_status(int(parts[2])))
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[2].isdigit() and parts[3] == "channels":
                return self._send({"channels": self.db.list_channels(int(parts[2]))})
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[2].isdigit() and parts[3] == "delivery-status":
                return self._send(self.db.project_delivery_status(int(parts[2])))
            if len(parts) == 4 and parts[:2] == ["api", "packages"] and parts[2].isdigit():
                return self._send(self.db.get_package(int(parts[2])))
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
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[2].isdigit() and parts[3] == "glossary":
                return self._send(self.db.set_glossary(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "projects"] and parts[2].isdigit() and parts[3] == "channels":
                return self._send(self.db.create_channel(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "assignments":
                return self._send(self.db.assign(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "cues":
                return self._send(self.db.save_cue(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "comments":
                return self._send(self.db.add_comment(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] in {"submit", "lock", "deliver"}:
                version_id = int(parts[2])
                if parts[3] == "submit":
                    return self._send(self.db.submit(version_id, actor, role))
                if parts[3] == "lock":
                    return self._send(self.db.lock(version_id, actor, role))
                return self._send(self.db.deliver(version_id, actor, role))
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "review":
                return self._send(self.db.review(int(parts[2]), actor, body, role))
            if len(parts) == 5 and parts[:2] == ["api", "versions"] and parts[3] == "pack":
                return self._send(self.db.pack_channel(int(parts[2]), parts[4], actor, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "versions"] and parts[3] == "pack-batch":
                return self._send(self.db.pack_batch(int(parts[2]), actor, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "packages"] and parts[2].isdigit() and parts[3] == "receipt":
                return self._send(self.db.submit_receipt(int(parts[2]), actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "packages"] and parts[2].isdigit() and parts[3] == "confirm-legacy":
                return self._send(self.db.confirm_legacy_package(int(parts[2]), actor, role))
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
