import json
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from pathlib import Path

import app as app_module


def request(url, method="GET", body=None, user="alice", role="owner"):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-User", user)
    req.add_header("X-Role", role)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


class DeliveryHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = app_module.Database(Path(cls.tmp.name) / "http.db")
        seed = app_module.seed_demo(cls.db)
        cls.project, cls.version = seed["project"], seed["version"]
        cls.db.assign(cls.version, "alice", {"user": "bob", "role": "translator"}, "owner")
        cls.db.assign(cls.version, "alice", {"user": "carol", "role": "reviewer"}, "owner")
        cls.db.save_cue(cls.version, "bob",
                        {"cue_index": 1, "start_ms": 1000, "end_ms": 3000, "text": "海豹在冰面",
                         "expected_revision": 0})
        cls.db.submit(cls.version, "bob")
        cls.db.review(cls.version, "carol", {"decision": "approve"}, "reviewer")
        cls.db.lock(cls.version, "alice")
        app_module.Handler.db = cls.db
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app_module.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.port}/api"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.tmp.cleanup()

    def test_pack_sign_and_complete_over_http(self):
        status, body = request(f"{self.base}/versions/{self.version}/packages", "POST", {})
        self.assertEqual(status, 202)
        self.assertEqual(body["status"], "done")
        packages = body["packages"]
        self.assertEqual(len(packages), 3)

        # A repeated submission must occupy the same package ids.
        _, again = request(f"{self.base}/versions/{self.version}/packages", "POST", {})
        self.assertEqual({p["id"] for p in packages}, {p["id"] for p in again["packages"]})
        self.assertTrue(all(p.get("occupant") for p in again["packages"]))

        # Sign every channel using the external receipt payload.
        for pkg in packages:
            receipt = {"external_ref": f"EXT-{pkg['id']}", "manifest_hash": pkg["manifest_hash"],
                       "files": pkg["files"]}
            code, out = request(f"{self.base}/packages/{pkg['id']}/receipts", "POST", receipt)
            self.assertEqual(code, 201, out)

        status, state = request(f"{self.base}/versions/{self.version}/delivery-state")
        self.assertEqual(status, 200)
        self.assertTrue(state["complete"])

    def test_bad_receipt_rejected_over_http(self):
        _, body = request(f"{self.base}/versions/{self.version}/packages", "POST",
                          {"channels": ["cinema-c"]})
        pkg = body["packages"][0]
        # Tamper with one file digest.
        files = json.loads(json.dumps(pkg["files"]))
        files[0]["sha256"] = "f" * 64
        code, out = request(f"{self.base}/packages/{pkg['id']}/receipts", "POST",
                            {"external_ref": "EXT-BAD", "manifest_hash": pkg["manifest_hash"], "files": files})
        self.assertEqual(code, 422)
        self.assertIn("摘要不符", out["error"])


if __name__ == "__main__":
    unittest.main()
