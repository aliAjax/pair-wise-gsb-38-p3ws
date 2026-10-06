import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from app import Database, DomainError, seed_demo


def prepare_locked_version(db, project, version, actor="bob", reviewer="carol", owner="alice"):
    """draft -> review -> approved -> locked, returns lock result"""
    db.save_cue(version, actor,
                {"cue_index": 1, "start_ms": 1000, "end_ms": 3000, "text": "seal 海豹在冰面",
                 "expected_revision": 0})
    db.submit(version, actor)
    db.review(version, reviewer, {"decision": "approve", "comment": "通过"}, "reviewer")
    return db.lock(version, owner)


class BlockingPacker:
    """Packer that blocks until released; counts calls per channel."""

    def __init__(self):
        self.inside = threading.Event()
        self.release = threading.Event()
        self.calls: list[str] = []
        self.lock = threading.Lock()

    def __call__(self, channel, payload):
        with self.lock:
            self.calls.append(channel["code"])
        self.inside.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("packer timed out")
        return Database._default_pack(channel, payload)


class FailOncePacker:
    """Fails the first N attempts for a channel, then succeeds."""

    def __init__(self, fail_codes=()):
        self.fail_codes = set(fail_codes)
        self.attempts: dict[str, int] = {}

    def __call__(self, channel, payload):
        n = self.attempts.get(channel["code"], 0) + 1
        self.attempts[channel["code"]] = n
        if channel["code"] in self.fail_codes and n == 1:
            raise DomainError(f"渠道 {channel['code']} 压包机临时故障", 500)
        return Database._default_pack(channel, payload)


class DeliveryLineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        seed = seed_demo(self.db)
        self.project, self.version = seed["project"], seed["version"]
        self.db.assign(self.version, "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(self.version, "alice", {"user": "carol", "role": "reviewer"}, "owner")
        locked = prepare_locked_version(self.db, self.project, self.version)
        self.content_hash = locked["content_hash"]

    def tearDown(self):
        self.tmp.cleanup()

    def _codes(self, result):
        return [p["channel_code"] for p in result["packages"]]

    def test_lock_generates_independent_package_per_channel(self):
        result = self.db.request_packages(self.version, "alice", {})
        self.assertEqual(result["status"], "done")
        self.assertEqual(sorted(self._codes(result)), ["broadcast-b", "cinema-c", "stream-a"])
        for pkg in result["packages"]:
            self.assertEqual(pkg["status"], "packed")
            self.assertEqual(pkg["content_hash"], self.content_hash)
            self.assertTrue(pkg["manifest_hash"])
            self.assertEqual(len(pkg["files"]), 3)
        fmts = {p["channel_code"]: p["manifest"]["subtitle_format"] for p in result["packages"]}
        self.assertEqual(fmts, {"broadcast-b": "srt", "cinema-c": "ttml", "stream-a": "vtt"})

    def test_packaging_only_allowed_for_locked_version(self):
        new_version = self.db.create_version(self.project, "alice", {"language": "en-US"}, "owner")["id"]
        with self.assertRaisesRegex(DomainError, "锁定版本"):
            self.db.request_packages(new_version, "alice", {})

    def test_concurrent_requests_same_channel_single_occupant(self):
        packer = BlockingPacker()
        self.db.packer = packer
        results = []
        errors = []

        def worker():
            try:
                results.append(self.db.request_packages(
                    self.version, "alice", {"channels": ["stream-a"]}))
            except Exception as exc:  # pragma: no cover - failure diagnostics
                errors.append(exc)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        self.assertTrue(packer.inside.wait(timeout=5))
        t2.start()
        # Give the follower time to enter the slot lock and observe occupancy.
        time.sleep(0.2)
        packer.release.set()
        t1.join(timeout=5)
        t2.join(timeout=5)
        self.assertFalse(errors)
        self.assertEqual(packer.calls, ["stream-a"])
        self.assertEqual(len(results), 2)
        ids = {r["packages"][0]["id"] for r in results}
        self.assertEqual(len(ids), 1, "后来者必须拿到同一占位包")
        follower = next(r for r in results if r["packages"][0].get("occupant"))
        self.assertIn("占位", follower["packages"][0]["note"])

    def test_duplicate_request_after_packed_does_not_repack(self):
        first = self.db.request_packages(self.version, "alice", {"channels": ["stream-a"]})
        second = self.db.request_packages(self.version, "alice", {"channels": ["stream-a"]})
        self.assertEqual(first["packages"][0]["id"], second["packages"][0]["id"])
        self.assertTrue(second["packages"][0]["occupant"])

    def test_failed_channel_kept_and_batch_resumes_from_gap(self):
        self.db.packer = FailOncePacker(fail_codes={"broadcast-b"})
        first = self.db.request_packages(self.version, "alice", {})
        self.assertEqual(first["status"], "partial")
        statuses = {p["channel_code"]: p["status"] for p in first["packages"]}
        self.assertEqual(statuses["broadcast-b"], "failed")
        self.assertTrue(statuses["stream-a"] == statuses["cinema-c"] == "packed")
        failed_id = next(p["id"] for p in first["packages"] if p["channel_code"] == "broadcast-b")

        # Fix the packer and retry only the failed channel: same slot row.
        self.db.packer = Database._default_pack
        again = self.db.request_packages(self.version, "alice", {"channels": ["broadcast-b"]})
        self.assertEqual(again["status"], "done")
        self.assertEqual(again["packages"][0]["id"], failed_id)
        self.assertEqual(again["packages"][0]["status"], "packed")

    def test_channel_spec_violation_fails_that_channel_only(self):
        # broadcast-b caps at 300 cues; add 300 more valid cues under a new lock.
        self.db.unlock(self.version, "alice")
        for i in range(2, 302):
            self.db.save_cue(self.version, "bob",
                             {"cue_index": i, "start_ms": 3000 + i * 100, "end_ms": 3000 + i * 100 + 50,
                              "text": f"海豹镜头 {i}", "expected_revision": i - 1})
        self.db.submit(self.version, "bob")
        self.db.review(self.version, "carol", {"decision": "approve"}, "reviewer")
        self.db.lock(self.version, "alice")
        result = self.db.request_packages(self.version, "alice", {})
        statuses = {p["channel_code"]: p["status"] for p in result["packages"]}
        self.assertEqual(statuses["broadcast-b"], "failed")
        self.assertEqual(statuses["stream-a"], "packed")  # cap 500
        self.assertEqual(statuses["cinema-c"], "packed")  # no cap
        self.assertIn("300", result["packages"][0]["pack_error"] or
                      next(p["pack_error"] for p in result["packages"] if p["channel_code"] == "broadcast-b"))

    def test_glossary_change_invalidates_unsigned_but_keeps_signed(self):
        packed = self.db.request_packages(self.version, "alice", {"channels": ["stream-a", "broadcast-b"]})
        stream = next(p for p in packed["packages"] if p["channel_code"] == "stream-a")
        broadcast = next(p for p in packed["packages"] if p["channel_code"] == "broadcast-b")
        receipt = self._sign(stream)
        self.assertTrue(receipt["external_ref"])

        # Term change after lock: unsigned packages die; signed one is untouched.
        self.db.set_glossary(self.project, "alice",
                             {"source_term": "seal", "required_translation": "海豹（修正）",
                              "forbidden_terms": ["密封"]}, "owner")
        packages = {p["id"]: p for p in self.db.list_packages(self.version)}
        self.assertEqual(packages[broadcast["id"]]["status"], "invalidated")
        self.assertEqual(packages[stream["id"]]["status"], "packed")
        # Signed package still shows its original manifest summary.
        self.assertEqual(packages[stream["id"]]["manifest_hash"], stream["manifest_hash"])
        state = self.db.delivery_state(self.version)
        self.assertFalse(state["complete"])

    def test_signed_package_for_old_lock_blocks_after_relock(self):
        packed = self.db.request_packages(self.version, "alice", {"channels": ["stream-a"]})
        stream = packed["packages"][0]
        self._sign(stream)
        # Subtitle change requires unlock flow; new lock fingerprint differs.
        cue_id = self.db.list_cues(self.version)[0]["id"]
        self.db.unlock(self.version, "alice")
        self.db.save_cue(self.version, "bob",
                         {"cue_id": cue_id,
                          "cue_index": 1, "start_ms": 1000, "end_ms": 2800,
                          "text": "海豹在冰面（修订）", "expected_revision": 1})
        self.db.submit(self.version, "bob")
        self.db.review(self.version, "carol", {"decision": "approve"}, "reviewer")
        new_lock = self.db.lock(self.version, "alice")
        self.assertNotEqual(new_lock["content_hash"], self.content_hash)
        # Old signed receipt can't sign again for the new lock; version not done.
        state = self.db.delivery_state(self.version)
        self.assertFalse(state["complete"])
        repacked = self.db.request_packages(self.version, "alice", {})
        by_code = {p["channel_code"]: p for p in repacked["packages"]}
        self.assertEqual(by_code["stream-a"]["status"], "packed")
        self.assertNotEqual(by_code["stream-a"]["id"], stream["id"])
        for code, pkg in by_code.items():
            self._sign(pkg, ref=f"EXT-NEW-{code}")
        state = self.db.delivery_state(self.version)
        self.assertTrue(state["complete"])
        self.assertEqual(state["status"], "delivered")

    def test_receipt_missing_file_rejected(self):
        packed = self.db.request_packages(self.version, "alice", {"channels": ["stream-a"]})
        pkg = packed["packages"][0]
        files = pkg["files"][1:]  # drop one file
        with self.assertRaisesRegex(DomainError, "缺文件"):
            self.db.record_receipt(pkg["id"], "alice",
                                   {"external_ref": "EXT-1", "manifest_hash": pkg["manifest_hash"],
                                    "files": files})
        state = self.db.delivery_state(self.version)
        self.assertEqual(state["channels_done"], 0)

    def test_receipt_wrong_manifest_hash_rejected(self):
        packed = self.db.request_packages(self.version, "alice", {"channels": ["stream-a"]})
        pkg = packed["packages"][0]
        with self.assertRaisesRegex(DomainError, "摘要不符"):
            self.db.record_receipt(pkg["id"], "alice",
                                   {"external_ref": "EXT-2", "manifest_hash": "0" * 64,
                                    "files": pkg["files"]})

    def test_full_delivery_requires_every_channel_signed(self):
        packed = self.db.request_packages(self.version, "alice", {})
        state = self.db.delivery_state(self.version)
        self.assertFalse(state["complete"])
        self.assertEqual(state["channels_total"], 3)
        self.assertEqual(state["channels_done"], 0)
        for pkg in packed["packages"]:
            self._sign(pkg)
        state = self.db.delivery_state(self.version)
        self.assertTrue(state["complete"])
        self.assertEqual(state["channels_done"], 3)
        self.assertEqual(state["status"], "delivered")

    def test_receipt_idempotent(self):
        packed = self.db.request_packages(self.version, "alice", {"channels": ["stream-a"]})
        pkg = packed["packages"][0]
        first = self._sign(pkg, ref="EXT-IDEM")
        second = self.db.record_receipt(pkg["id"], "alice",
                                        {"external_ref": "EXT-IDEM", "manifest_hash": pkg["manifest_hash"],
                                         "files": pkg["files"]})
        self.assertEqual(first["id"], second["id"])
        self.assertTrue(second["idempotent"])

    def _sign(self, pkg, ref=None):
        ref = ref or f"EXT-{pkg['id']}"
        return self.db.record_receipt(pkg["id"], "alice",
                                      {"external_ref": ref, "manifest_hash": pkg["manifest_hash"],
                                       "files": pkg["files"]})


class LegacyMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "old.db")
        seed = seed_demo(self.db)
        self.project, self.version = seed["project"], seed["version"]
        self.db.assign(self.version, "alice", {"user": "bob", "role": "translator"}, "owner")
        self.db.assign(self.version, "alice", {"user": "carol", "role": "reviewer"}, "owner")
        prepare_locked_version(self.db, self.project, self.version)
        # Simulate a pre-pipeline delivery recorded by the old code path.
        self.old_delivery = self.db.deliver(self.version, "alice")

    def tearDown(self):
        self.tmp.cleanup()

    def test_legacy_delivery_backfilled_unconfirmed_and_excluded(self):
        # Build a genuinely old project: it only has a legacy delivery, none of
        # the seed streaming channels.
        tmpdb = self.db
        old_project = tmpdb.create_project(
            "alice", {"name": "老电影字幕", "source_language": "en", "media_name": "old.mp4",
                      "media_sha256": "a" * 64, "duration_ms": 60000}, "owner")
        old_version = tmpdb.create_version(old_project["id"], "alice", {"language": "zh-CN"}, "owner")["id"]
        tmpdb.assign(old_version, "alice", {"user": "bob", "role": "translator"}, "owner")
        tmpdb.assign(old_version, "alice", {"user": "carol", "role": "reviewer"}, "owner")
        prepare_locked_version(tmpdb, old_project["id"], old_version)
        old_delivery = tmpdb.deliver(old_version, "alice")

        # Reopen the database against the existing file to trigger migration.
        db = Database(Path(self.tmp.name) / "old.db")
        packages = db.list_packages(old_version)
        legacy = [p for p in packages if p["kind"] == "legacy"]
        self.assertEqual(len(legacy), 1)
        rec = legacy[0]
        self.assertEqual(rec["status"], "packed")
        self.assertFalse(rec["confirmed"])
        self.assertEqual(rec["manifest_hash"], old_delivery["snapshot_hash"])
        state = db.delivery_state(old_version)
        self.assertFalse(state["complete"])
        # Before confirmation the legacy channel is kept out of the denominator.
        self.assertEqual(state["channels_total"], 0)
        self.assertEqual(state["channels_done"], 0)
        self.assertEqual(state["status"], "locked")
        db.confirm_legacy(rec["id"], "alice")
        state = db.delivery_state(old_version)
        self.assertTrue(state["complete"])
        self.assertEqual(state["channels_total"], 1)
        self.assertEqual(state["channels_done"], 1)
        self.assertEqual(state["status"], "delivered")

    def test_migration_is_idempotent(self):
        Database(Path(self.tmp.name) / "old.db")
        db2 = Database(Path(self.tmp.name) / "old.db")
        legacy = [p for p in db2.list_packages(self.version) if p["kind"] == "legacy"]
        self.assertEqual(len(legacy), 1)


if __name__ == "__main__":
    unittest.main()
