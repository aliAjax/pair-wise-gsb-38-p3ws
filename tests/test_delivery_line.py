import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from app import Database, DomainError, seed_demo


def approved_locked(db: Database, project: int, version: int, user: str = "bob", reviewer: str = "carol",
                    text: str = "seal 海豹在冰面", length_ms: int = 2000) -> None:
    db.save_cue(version, user, {"cue_index": 1, "start_ms": 1000, "end_ms": 1000 + length_ms,
                                "text": text, "expected_revision": 0})
    db.submit(version, user)
    db.review(version, reviewer, {"decision": "approve", "comment": "通过"}, "reviewer")
    db.lock(version, "alice")


def sign(pkg: dict, db: Database, actor: str = "external-bot", overrides: dict | None = None) -> dict:
    manifest = json.loads(pkg["manifest"])
    payload = {"system_name": "external", "manifest_digest": pkg["manifest_digest"],
               "files": {f["path"]: f["sha256"] for f in manifest["files"]}}
    if overrides:
        payload.update(overrides)
    return db.submit_receipt(pkg["id"], actor, payload)


class DeliveryLineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        seed = seed_demo(self.db)
        self.project, self.version = seed["project"], seed["version"]
        self.db.assign(self.version, "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(self.version, "alice", {"user": "carol", "role": "reviewer"}, "owner")
        approved_locked(self.db, self.project, self.version)

    def tearDown(self):
        self.tmp.cleanup()

    def packages(self):
        return {p["channel_code"]: p for p in self.db.list_packages(self.version)}

    def test_lock_creates_per_channel_packages_with_same_lock_identity(self):
        batch = self.db.pack_batch(self.version, "alice")
        self.assertEqual(set(batch["packed"]), {"cinema", "streaming"})
        packages = self.packages()
        self.assertEqual(packages["cinema"]["format"], "ass")
        self.assertEqual(packages["streaming"]["format"], "srt")
        lock_ids = {p["lock_id"] for p in packages.values()}
        self.assertEqual(len(lock_ids), 1)
        status = self.db.delivery_status(self.version)
        self.assertEqual(status["total_channels"], 2)
        self.assertFalse(status["complete"])

    def test_concurrent_pack_requests_same_channel_single_pack(self):
        results, errors = [], []

        def worker():
            try:
                results.append(self.db.pack_channel(self.version, "cinema", "alice"))
            except DomainError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        self.assertEqual(len(results), 4)
        ids = {r["package"]["id"] for r in results}
        self.assertEqual(len(ids), 1)
        self.assertEqual(sum(1 for r in results if r["deduped"]), 3)
        self.assertEqual(self.packages()["cinema"]["attempt"], 1)

    def test_batch_failure_keeps_completed_channels_and_resumes(self):
        # cinema 要求单条 <=30 非空白字且显示 >=1000ms；超长字幕在 cinema 处失败，
        # streaming 尚未压到。
        long_text = "海豹" + "长" * 40
        v2 = self.db.create_version(self.project, "alice", {"language": "zh-CN", "parent_id": self.version}, "owner")
        self.db.assign(v2["id"], "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(v2["id"], "alice", {"user": "dave", "role": "reviewer"}, "owner")
        approved_locked(self.db, self.project, v2["id"], reviewer="dave", text=long_text)
        batch = self.db.pack_batch(v2["id"], "alice")
        self.assertEqual(batch["packed"], [])
        self.assertEqual(batch["failed"][0]["channel"], "cinema")
        self.assertIn("streaming", batch["remaining"])
        packages = {p["channel_code"]: p for p in self.db.list_packages(v2["id"])}
        self.assertEqual(packages["cinema"]["status"], "failed")
        # streaming 可以单独先压，已完成渠道保留。
        ok = self.db.pack_channel(v2["id"], "streaming", "alice")
        self.assertFalse(ok["deduped"])
        # cinema 仍失败，缺口未补齐。
        with self.assertRaisesRegex(DomainError, "超出"):
            self.db.pack_channel(v2["id"], "cinema", "alice")

    def test_spec_failure_then_repack_after_content_fix(self):
        self.db.create_channel(self.project, "alice", {"code": "mobile", "name": "手机端",
                                                        "format": "srt", "max_chars_per_cue": 5, "min_display_ms": 0}, "owner")
        with self.assertRaisesRegex(DomainError, "超出"):
            self.db.pack_channel(self.version, "mobile", "alice")
        pkg = next(p for p in self.db.list_packages(self.version) if p["channel_code"] == "mobile")
        self.assertEqual(pkg["status"], "failed")
        self.assertEqual(pkg["attempt"], 1)
        # 新建修正版本并锁定后续压成功。
        v2 = self.db.create_version(self.project, "alice", {"language": "zh-CN", "parent_id": self.version}, "owner")
        self.db.assign(v2["id"], "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(v2["id"], "alice", {"user": "dave", "role": "reviewer"}, "owner")
        approved_locked(self.db, self.project, v2["id"], reviewer="dave", text="海豹")
        result = self.db.pack_channel(v2["id"], "mobile", "alice")
        self.assertEqual(result["package"]["status"], "ready")

    def test_glossary_change_invalidates_unsigned_packages_signed_keep_manifest(self):
        self.db.pack_batch(self.version, "alice")
        packages = self.packages()
        sign(packages["cinema"], self.db)
        signed_manifest = packages["cinema"]["manifest"]
        signed_digest = packages["cinema"]["manifest_digest"]
        self.db.set_glossary(self.project, "alice", {"source_term": "seal", "required_translation": "海豹",
                                                     "forbidden_terms": ["密封", "海狗"]}, "owner")
        packages = self.packages()
        self.assertEqual(packages["cinema"]["status"], "signed")
        self.assertEqual(packages["cinema"]["manifest"], signed_manifest)
        self.assertEqual(packages["cinema"]["manifest_digest"], signed_digest)
        self.assertEqual(packages["streaming"]["status"], "invalidated")
        # 失效包不能直接签收。
        with self.assertRaisesRegex(DomainError, "不能签收"):
            sign(packages["streaming"], self.db)
        # 重新压包后用新清单可以签收。
        self.db.pack_channel(self.version, "streaming", "alice")
        repacked = self.packages()["streaming"]
        self.assertEqual(repacked["status"], "ready")
        sign(repacked, self.db)

    def test_new_lock_supersedes_unsigned_packages_only(self):
        self.db.pack_batch(self.version, "alice")
        first = self.packages()
        sign(first["cinema"], self.db)
        v2 = self.db.create_version(self.project, "alice", {"language": "zh-CN", "parent_id": self.version}, "owner")
        self.db.assign(v2["id"], "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(v2["id"], "alice", {"user": "dave", "role": "reviewer"}, "owner")
        approved_locked(self.db, self.project, v2["id"], reviewer="dave", text="海豹修订")
        old = self.packages()
        self.assertEqual(old["cinema"]["status"], "signed")
        self.assertEqual(old["streaming"]["status"], "superseded")
        # 旧锁定版本不再接受压包：渠道只认当前锁定版本。
        with self.assertRaisesRegex(DomainError, "当前锁定版本"):
            self.db.pack_channel(self.version, "streaming", "alice")
        # 同语言新锁定，cinema 也要在新锁上独立压包签收。
        self.db.pack_batch(v2["id"], "alice")
        self.assertFalse(self.db.delivery_status(self.version)["complete"])
        self.assertFalse(self.db.delivery_status(v2["id"])["complete"])

    def test_receipt_missing_file_and_digest_mismatch_block_signoff(self):
        self.db.pack_channel(self.version, "streaming", "alice")
        pkg = self.packages()["streaming"]
        manifest = json.loads(pkg["manifest"])
        with self.assertRaisesRegex(DomainError, "缺少文件"):
            sign(pkg, self.db, overrides={"files": {}})
        bad = {f["path"]: "0" * 64 for f in manifest["files"]}
        with self.assertRaisesRegex(DomainError, "文件摘要不符"):
            sign(pkg, self.db, overrides={"files": bad})
        with self.assertRaisesRegex(DomainError, "清单摘要不符"):
            sign(pkg, self.db, overrides={"manifest_digest": "f" * 64})
        self.assertEqual(self.packages()["streaming"]["status"], "ready")
        sign(pkg, self.db)

    def test_version_cannot_be_delivered_until_all_channels_signed(self):
        self.db.pack_channel(self.version, "cinema", "alice")
        sign(self.packages()["cinema"], self.db)
        with self.assertRaisesRegex(DomainError, "1 个渠道未签收"):
            self.db.deliver(self.version, "alice")
        self.db.pack_channel(self.version, "streaming", "alice")
        sign(self.packages()["streaming"], self.db)
        status = self.db.delivery_status(self.version)
        self.assertTrue(status["complete"])
        self.db.deliver(self.version, "alice")

    def test_channel_added_after_signoff_reopens_completion(self):
        self.db.pack_batch(self.version, "alice")
        for pkg in self.packages().values():
            sign(pkg, self.db)
        self.assertTrue(self.db.delivery_status(self.version)["complete"])
        # 锁定签收后新增渠道：整版不再完整，且不能交付，直到新渠道补齐签收。
        self.db.create_channel(self.project, "alice", {"code": "broadcast", "name": "电视台",
                                                        "format": "vtt", "max_chars_per_cue": 42, "min_display_ms": 500}, "owner")
        status = self.db.delivery_status(self.version)
        self.assertFalse(status["complete"])
        self.assertEqual(status["total_channels"], 3)
        with self.assertRaisesRegex(DomainError, "1 个渠道未签收"):
            self.db.deliver(self.version, "alice")
        self.db.pack_channel(self.version, "broadcast", "alice")
        sign(self.packages()["broadcast"], self.db)
        self.assertTrue(self.db.delivery_status(self.version)["complete"])

    def test_duplicate_receipt_is_idempotent(self):
        self.db.pack_channel(self.version, "streaming", "alice")
        pkg = self.packages()["streaming"]
        sign(pkg, self.db)
        again = sign(pkg, self.db)
        self.assertTrue(again.get("deduped"))

    def test_pack_before_lock_rejected(self):
        v = self.db.create_version(self.project, "alice", {"language": "ja"}, "owner")
        with self.assertRaisesRegex(DomainError, "尚未锁定"):
            self.db.pack_channel(v["id"], "cinema", "alice")


class LegacyBackfillTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "legacy.db"
        # 用全新 Database 建库建项目，然后模拟“老版本数据库”：删掉新表，
        # 直接走旧的交付快照流程，再重新打开触发历史补录。
        self.db = Database(self.db_path)
        seed = seed_demo(self.db)
        self.project, self.version = seed["project"], seed["version"]
        self.db.assign(self.version, "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(self.version, "alice", {"user": "carol", "role": "reviewer"}, "owner")
        self.db.save_cue(self.version, "bob", {"cue_index": 1, "start_ms": 1000, "end_ms": 3000,
                                               "text": "seal 海豹在冰面", "expected_revision": 0})
        self.db.submit(self.version, "bob")
        self.db.review(self.version, "carol", {"decision": "approve"}, "reviewer")
        # 旧系统：批准后直接交付。
        delivery = self.db.deliver(self.version, "alice")
        self.snapshot_hash = delivery["snapshot_hash"]
        conn = sqlite3.connect(self.db_path)
        conn.executescript("DROP TABLE receipts; DROP TABLE packages; DROP TABLE locks; DROP TABLE channels;")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_historical_deliveries_backfilled_as_unconfirmed_legacy(self):
        db = Database(self.db_path)  # 触发升级补录
        packages = db.list_packages(self.version)
        self.assertEqual(len(packages), 1)
        legacy = packages[0]
        self.assertEqual(legacy["channel_kind"], "legacy")
        self.assertEqual(legacy["status"], "legacy")
        self.assertEqual(legacy["confirmed"], 0)
        self.assertEqual(legacy["manifest_digest"], self.snapshot_hash)
        # 确认前不进交片完成度：没有任何普通渠道，整版不算已交。
        status = db.delivery_status(self.version)
        self.assertFalse(status["complete"])
        self.assertEqual(status["legacy_pending"], [legacy["id"]])
        confirmed = db.confirm_legacy_package(legacy["id"], "alice")
        self.assertEqual(confirmed["package"]["confirmed"], 1)
        # 非负责人不能确认。
        with self.assertRaisesRegex(DomainError, "只有项目负责人"):
            db.confirm_legacy_package(legacy["id"], "bob")
        # 补录幂等：再次打开不会重复补。
        db2 = Database(self.db_path)
        self.assertEqual(len(db2.list_packages(self.version)), 1)


if __name__ == "__main__":
    unittest.main()
