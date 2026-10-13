import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from alpha_scanner_processing import DurableProcessing

T0 = "2026-10-14T10:00:00Z"
T1 = "2026-10-14T10:01:00Z"
T2 = "2026-10-14T10:02:00Z"


class DurableProcessingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.sqlite3"
        self.k = DurableProcessing(self.db)
        self.k.append_event(event_id="e1", stream="TRADES", available_at=T0, payload={"price": 10})

    def tearDown(self):
        self.tmp.cleanup()

    def commit(self, output=None):
        return self.k.commit_output(consumer="features-v1", stream="TRADES", event_id="e1",
                                   output=output or {"feature": 10}, processed_at=T1)

    def test_output_checkpoint_atomic_and_retry_idempotent(self):
        first = self.commit()
        self.assertFalse(first["already_committed"])
        retry = self.commit({"feature": 99})
        self.assertTrue(retry["already_committed"])
        self.assertFalse(retry["retry_output_matches"])
        self.assertEqual(retry["output"], {"feature": 10})
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM outputs").fetchone()[0], 1)
        self.assertTrue(self.k.verify()["ok"], self.k.verify()["errors"])

    def test_concurrent_duplicate_calls_create_one_output(self):
        def worker(_):
            return DurableProcessing(self.db).commit_output(
                consumer="features-v1", stream="TRADES", event_id="e1",
                output={"feature": 10}, processed_at=T1)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(worker, range(16)))
        self.assertEqual(sum(not r["already_committed"] for r in results), 1)
        self.assertEqual(len({r["output_id"] for r in results}), 1)
        self.assertTrue(self.k.verify()["ok"], self.k.verify()["errors"])

    def test_crash_rolls_back_output_evidence_and_checkpoint(self):
        code = """
import os,sys
from alpha_scanner_processing import DurableProcessing
class Crash(DurableProcessing):
    def _after_output_insert(self): os._exit(73)
k=Crash(sys.argv[1])
k.commit_output(consumer='features-v1',stream='TRADES',event_id='e1',
                output={'feature':10},processed_at='2026-10-14T10:01:00Z')
"""
        env = os.environ.copy()
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1]) + os.pathsep + env.get("PYTHONPATH","")
        result = subprocess.run([sys.executable,"-c",code,str(self.db)],env=env,check=False)
        self.assertEqual(result.returncode,73)
        with sqlite3.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM outputs").fetchone()[0],0)
            self.assertEqual(c.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0],0)
            self.assertEqual(c.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],1)
        self.assertFalse(self.commit()["already_committed"])
        self.assertTrue(self.k.verify()["ok"], self.k.verify()["errors"])

    def test_rejects_out_of_order_and_unavailable_events(self):
        self.k.append_event(event_id="e2",stream="TRADES",available_at=T2,payload={"price":11})
        with self.assertRaisesRegex(ValueError,"out-of-order"):
            self.k.commit_output(consumer="features-v1",stream="TRADES",event_id="e2",
                                 output={"feature":11},processed_at=T2)
        with self.assertRaisesRegex(ValueError,"not available"):
            self.k.commit_output(consumer="features-v1",stream="TRADES",event_id="e1",
                                 output={"feature":10},processed_at="2026-10-14T09:00:00Z")

    def test_output_rows_are_append_only(self):
        self.commit()
        with sqlite3.connect(self.db) as c:
            with self.assertRaisesRegex(sqlite3.IntegrityError,"append-only"):
                c.execute("UPDATE outputs SET output_json='{}' WHERE event_id='e1'")
            with self.assertRaisesRegex(sqlite3.IntegrityError,"append-only"):
                c.execute("DELETE FROM outputs WHERE event_id='e1'")


if __name__ == "__main__":
    unittest.main(verbosity=2)
