"""Kiểm tra bộ đo INT-03. Các test truyền file cần bin/croc-base(.exe).

python -m unittest discover -s croc-main/scripts -p test_croc_bench.py -v
"""

import hashlib
import json
import os
from pathlib import Path
import socket
import sys
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

    def test_only_received_chunks_are_candidates(self):
        # Giống file dở dang của croc: chunk chưa nhận là toàn byte 0.
        data = bytearray(self.data)
        data[2048:5120] = bytes(3072)  # chunk 2, 3, 4
        data[10240:] = bytes(len(data) - 10240)  # chunk cuối (5 byte)
        self.file.write_bytes(data)
        present = bench_merkle_resume.present_chunks(self.file, chunk_size=1024)
        self.assertEqual(present, [0, 1, 5, 6, 7, 8, 9])
        changes = bench_merkle_resume.corrupt_chunks(
            self.file, 7, chunk_size=1024, seed=3, candidates=present
        )
        self.assertEqual([change["chunk"] for change in changes], present)
        with self.assertRaises(bench.BenchError):
            bench_merkle_resume.corrupt_chunks(
                self.file, 8, chunk_size=1024, candidates=present
            )


class ResumeHelpersTest(unittest.TestCase):
    def test_request_rounds_from_debug_log(self):
        log = (
            b"[debug] croc.go:3448: sending recipient ready with 131 chunks\r\n"
            b" 100% |####|\r[debug] croc.go:3448: sending recipient ready with 256 chunks\n"
        )
        self.assertEqual(bench_merkle_resume.requested_chunks(log), [131, 256])
        self.assertEqual(bench_merkle_resume.requested_chunks(b"No files transferred."), [])

    def test_merkle_totals(self):
        events = [
            {"event": "chunk_compress"},
            {"event": "merkle_verify", "corrupt_chunks": 2, "retransmitted_bytes": 65536,
             "saved_bytes": 100},
            {"event": "merkle_verify", "corrupt_chunks": 1, "retransmitted_bytes": 32768,
             "saved_bytes": 50},
        ]  # fmt: skip
        self.assertEqual(
            bench_merkle_resume.merkle_totals(events),
            {"mv_events": 2, "mv_corrupt_chunks": 3,
             "mv_retransmitted_bytes": 98304, "mv_saved_bytes": 150},
        )  # fmt: skip
        self.assertEqual(
            bench_merkle_resume.merkle_totals([])["mv_retransmitted_bytes"], None
        )

    def test_ss_command_targets_only_receiver_sockets(self):
        self.assertEqual(
            bench_merkle_resume.ss_kill_command([9009, 9010]),
            ["ss", "-K", "-t", "(", "dst", "127.0.0.2:9009", "or",
             "dst", "127.0.0.2:9010", ")"],
        )  # fmt: skip

    @unittest.skipIf(sys.platform.startswith("linux"), "chỉ kiểm tra OS không có ss -K")
    def test_ss_cut_refused_outside_linux(self):
        with self.assertRaises(bench.BenchError):
            bench_merkle_resume.check_cut_method("ss")

    def test_without_binaries_or_data_only_writes_status(self):
        with tempfile.TemporaryDirectory() as work:
            work = Path(work)
            code = bench_merkle_resume.main(
                ["--base", str(work / "khong-co"), "--data-dir", str(work),
                 "--out", str(work / "out")]
            )  # fmt: skip
            self.assertEqual(code, 0)
            status = json.loads((work / "out" / "status.json").read_text("utf-8"))
            self.assertEqual(status["status"], "pending")
            self.assertEqual(len(status["missing"]), 2)


class ProxyTest(unittest.TestCase):
    def setUp(self):
        self.server = socket.socket()
        self.server.bind((bench_merkle_resume.RELAY_HOST, 0))
        self.server.listen()
        self.addCleanup(self.server.close)
        self.port = self.server.getsockname()[1]
        self.proxy = bench_merkle_resume.CountingProxy([self.port])
        self.addCleanup(self.proxy.close)

    def connect(self):
        client = socket.create_connection((bench_merkle_resume.PROXY_HOST, self.port), 5)
        self.addCleanup(client.close)
        upstream, _ = self.server.accept()
        self.addCleanup(upstream.close)
        upstream.settimeout(5)
        return client, upstream

    def recv_exactly(self, sock, size):
        data = b""
        while len(data) < size and (block := sock.recv(size - len(data))):
            data += block
        return data

    def test_forwards_and_counts_each_direction(self):
        client, upstream = self.connect()
        payload = os.urandom(300_000)
        upstream.sendall(payload)
        self.assertEqual(self.recv_exactly(client, len(payload)), payload)
        client.sendall(b"ack")
        self.assertEqual(self.recv_exactly(upstream, 3), b"ack")
        self.assertEqual(self.proxy.counters(), (len(payload), 3))
        self.proxy.reset_counters()
        self.assertEqual(self.proxy.counters(), (0, 0))

    def assert_dropped(self, sock):
        # Đầu bị cắt thấy RST (lỗi) hoặc EOF, tùy OS; không được treo hay nhận dữ liệu.
        sock.settimeout(5)
        try:
            self.assertEqual(sock.recv(1), b"")
        except (ConnectionResetError, ConnectionAbortedError):
            pass

    def test_cut_drops_connections_and_blocks_new_ones(self):
        client, upstream = self.connect()
        self.proxy.cut()
        self.assert_dropped(client)
        self.assert_dropped(upstream)
        late = socket.create_connection((bench_merkle_resume.PROXY_HOST, self.port), 5)
        self.addCleanup(late.close)
        self.assert_dropped(late)
        self.assertEqual(self.proxy.active(), 0)


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

    def test_cut_corrupt_and_resume(self):
        # croc-base chỉ xin lại chunk thiếu; chunk bị lật byte làm hash cả file
        # sai nên nó xin lại toàn bộ file ở vòng thứ hai.
        self.source.write_bytes(os.urandom(2 << 20))
        result = bench_merkle_resume.run_resume_trial(
            self.root / "run",
            binary=bench.DEFAULT_BASE,
            relay_binary=bench.DEFAULT_BASE,
            source=self.source,
            expected_sha256=hashlib.sha256(self.source.read_bytes()).hexdigest(),
            cut="reset",
            cut_at=0.5,
            corrupt=1,
            seed=1,
            rate=1 << 20,
            timeout=60,
        )
        self.assertEqual(result["status"], "ok", result.get("error"))
        total = (2 << 20) // bench_merkle_resume.CHUNK_SIZE
        self.assertEqual(result["present_chunks"] + result["missing_chunks"], total)
        self.assertEqual(result["requested_chunks"], result["missing_chunks"])
        self.assertEqual(result["request_rounds"], 2)
        self.assertGreater(result["resume_wire_bytes"], 2 << 20)
        self.assert_cleaned()


if __name__ == "__main__":
    unittest.main()
