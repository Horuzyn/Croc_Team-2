"""Kiểm tra bộ đo INT-03. Các test truyền file cần bin/croc-base(.exe).

python -m unittest discover -s croc-main/scripts -p test_croc_bench.py -v
"""

import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import bench_merkle_resume
import croc_bench as bench


def row(status, speed="", dataset="text", profile="base"):
    return {
        "suite": "adaptive-compress", "profile": profile, "dataset": dataset,
        "file": "f", "bytes": "10", "status": status, "mib_per_s": speed,
        "seconds": speed,
    }  # fmt: skip


class SummaryTest(unittest.TestCase):
    def test_failed_trials_are_excluded(self):
        rows = [row("ok", "10"), row("failed", "999"), row("ok", "30"), row("ok", "11")]
        (entry,) = bench.summarize(rows)
        self.assertEqual((entry["trials_ok"], entry["trials_total"]), (3, 4))
        self.assertEqual(entry["median_mib_per_s"], 11)
        self.assertEqual(entry["max_mib_per_s"], 30)

    def test_group_without_success_has_no_numbers(self):
        (entry,) = bench.summarize([row("failed")])
        self.assertIsNone(entry["median_mib_per_s"])
        self.assertIn("| 0/1 |", bench.markdown_table([entry]))


class CorruptTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.file = Path(temporary.name) / "partial file.bin"
        self.data = os.urandom(10 * 1024 + 5)
        self.file.write_bytes(self.data)

    def test_changes_one_byte_in_each_of_n_chunks(self):
        changes = bench_merkle_resume.corrupt_chunks(self.file, 11, chunk_size=1024, seed=7)
        after = self.file.read_bytes()
        different = [i for i, (a, b) in enumerate(zip(self.data, after)) if a != b]
        self.assertEqual(len(after), len(self.data))
        self.assertEqual(different, [change["offset"] for change in changes])
        self.assertEqual([offset // 1024 for offset in different], list(range(11)))

    def test_refuses_too_many_chunks_and_source_data(self):
        with self.assertRaises(bench.BenchError):
            bench_merkle_resume.corrupt_chunks(self.file, 12, chunk_size=1024)
        (self.file.parent / "SHA256SUMS.txt").write_text(f"{'0' * 64} *{self.file.name}\n")
        with self.assertRaises(bench.BenchError):
            bench_merkle_resume.corrupt_chunks(self.file, 1)
        self.assertEqual(self.file.read_bytes(), self.data)


class ProbeTest(unittest.TestCase):
    def probe(self, lines, status="ok"):
        def run_transfer(run_dir, *, global_args, **kwargs):
            if lines is not None:
                Path(global_args[-1]).write_text(lines, encoding="utf-8")
            return {"status": status, "error": "hỏng"}

        with (
            mock.patch.object(bench, "run_transfer", run_transfer),
            mock.patch.object(bench, "sha256", lambda path: "0"),
        ):
            return bench.probe_events("new", "relay", ["--adaptive-compress"])

    def test_flag_without_behaviour_emits_nothing(self):
        self.assertEqual(self.probe(""), set())
        self.assertEqual(self.probe(None), set())

    def test_collects_event_names(self):
        lines = '{"event":"chunk_compress"}\n{"event":"stream_stat"}\n'
        self.assertEqual(self.probe(lines), {"chunk_compress", "stream_stat"})

    def test_failed_probe_and_bad_line_are_errors(self):
        for lines, status in (("", "failed"), ("not json\n", "ok")):
            with self.assertRaises(bench.BenchError):
                self.probe(lines, status)


@unittest.skipUnless(bench.DEFAULT_BASE.is_file(), "chưa build croc-base")
class TransferTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        # Đường dẫn có khoảng trắng ở cả nguồn lẫn nơi chạy.
        self.root = Path(temporary.name) / "thư mục có khoảng trắng"
        self.root.mkdir()
        self.source = self.root / "du lieu.bin"
        self.source.write_bytes(os.urandom(1 << 20))
        self.digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.spawned = []
        original = bench.ProcessGroup.spawn

        def spawn(group, name, args, **kwargs):
            child = original(group, name, args, **kwargs)
            self.spawned.append(child)
            return child

        patcher = mock.patch.object(bench.ProcessGroup, "spawn", spawn)
        patcher.start()
        self.addCleanup(patcher.stop)

    def transfer(self, **overrides):
        options = {
            "binary": bench.DEFAULT_BASE,
            "relay_binary": bench.DEFAULT_BASE,
            "source": self.source,
            "expected_sha256": self.digest,
            "timeout": 60,
        }
        return bench.run_transfer(self.root / "run", **{**options, **overrides})

    def assert_cleaned(self):
        self.assertTrue(self.spawned)
        for child in self.spawned:
            self.assertIsNotNone(child.process.poll(), f"{child.name} còn chạy")
        self.assertFalse((self.root / "run" / "send" / self.source.name).exists())
        self.assertFalse((self.root / "run" / "recv" / self.source.name).exists())
        self.assertTrue(self.source.is_file())

    def test_success(self):
        result = self.transfer()
        self.assertEqual(result["status"], "ok", result.get("error"))
        self.assertTrue(result["sha256_ok"])
        self.assertAlmostEqual(result["mib_per_s"] * result["seconds"], 1.0)
        self.assert_cleaned()

    def test_checksum_mismatch_is_failed(self):
        result = self.transfer(expected_sha256="0" * 64)
        self.assertEqual(result["status"], "failed")
        self.assertIn("SHA-256", result["error"])
        self.assertNotIn("mib_per_s", result)
        self.assert_cleaned()

    def test_receiver_failure_stops_everything(self):
        result = self.transfer(receive_args=["--khong-co-co-nay"])
        self.assertEqual(result["status"], "failed")
        self.assertIn("receiver", result["error"])
        self.assert_cleaned()

    def test_timeout_stops_everything(self):
        result = self.transfer(timeout=0.01)
        self.assertEqual(result["status"], "failed")
        self.assertIn("quá thời gian", result["error"])
        self.assert_cleaned()

    def test_interrupt_stops_everything(self):
        original = bench.Child.wait

        def wait(child, timeout):
            if child.name == "receiver":
                raise KeyboardInterrupt
            return original(child, timeout)

        with mock.patch.object(bench.Child, "wait", wait):
            with self.assertRaises(KeyboardInterrupt):
                self.transfer()
        self.assert_cleaned()


if __name__ == "__main__":
    unittest.main()
