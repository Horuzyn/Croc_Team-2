#!/usr/bin/env python3
"""INT-03: Merkle resume (MRK) - Bài 4 của Doc_1 và kịch bản 3 của Doc_2.

Mỗi lượt đo:
  1. Truyền file qua relay riêng trên localhost. Receiver đi qua một proxy đếm
     byte (127.0.0.2 -> relay 127.0.0.1) nên biết chính xác tiến độ.
  2. Khi receiver đã nhận --cut-at (mặc định 50%) thì ngắt kết nối:
       kill   Ctrl+C vào receiver (SIGINT / CTRL_BREAK), như Bước 4.1;
       ss     Linux `ss -K` hủy socket TCP của receiver (cần root);
       reset  proxy gửi RST trên mọi kết nối của receiver ("rút cáp", mọi OS).
     Sau khi cắt, proxy chặn kết nối mới cho tới lượt resume.
  3. Lật 1 byte trong N chunk 32 KiB ĐÃ NHẬN của file dở dang (--corrupt-chunks).
  4. Chờ hai bên bỏ cuộc (croc tự thử nối lại vài giây), rồi chạy lại sender và
     receiver với cùng mã, receiver vào đúng thư mục cũ (Bước 4.2). Đo byte
     relay gửi tới receiver trong lượt này, thời gian,
     số chunk receiver xin lại (dòng log "sending recipient ready with N chunks")
     và SHA-256 của file cuối cùng.

croc-base chỉ xin lại chunk toàn byte 0, nên chunk bị lật byte vẫn hỏng: lượt đó
có status "corrupt". Bản nâng cấp chạy với --merkle-resume --metrics-out; cột
mv_* lấy từ sự kiện merkle_verify nếu có.

Đo:
    python bench_merkle_resume.py --data-dir DIR --out DIR_MỚI
    python bench_merkle_resume.py --new bin/croc-upgraded --cut ss --out DIR_MỚI
Chỉ làm hỏng N chunk của một file (sửa tại chỗ):
    python bench_merkle_resume.py --corrupt FILE --chunks 3 --seed 1
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time

import croc_bench as bench
from bundle_bench_common import sha256, stop_process, transfer_secret

SUITE = "merkle-resume"
FEATURE_FLAG = "--merkle-resume"
FEATURE_EVENT = "merkle_verify"
# croc xin lại theo khối models.TCP_BUFFER_SIZE/2; lá Merkle của MRK cùng cỡ.
CHUNK_SIZE = 32 * 1024
RELAY_HOST = "127.0.0.1"
PROXY_HOST = "127.0.0.2"
CUT_METHODS = ("kill", "ss", "reset")
READY_LINE = re.compile(rb"sending recipient ready with (\d+) chunks")# struct linger: hai u_short trên Windows, hai int trên Linux/macOS.
LINGER_RST = struct.pack("HH" if os.name == "nt" else "ii", 1, 0)

MEASUREMENT = {
    "resume_wire_bytes": "byte relay gửi tới receiver trong lượt resume (đếm ở proxy, "
    "gồm mã hóa, khung tin và bắt tay PAKE)",
    "resume_seconds": "từ khi khởi chạy lại receiver đến khi receiver thoát",
    "needed_bytes": "(chunk thiếu + chunk bị làm hỏng) x 32 KiB, cận dưới của một "
    "lần sửa đúng",
    "requested_chunks": "số chunk receiver xin lại ở vòng đầu, từ log --debug "
    "('sending recipient ready with N chunks')",
    "request_rounds": "số vòng xin; > 1 nghĩa là hash cả file sai và croc xin lại "
    "toàn bộ file",
    "requested_chunks_total": "tổng số chunk đã xin qua mọi vòng",
    "status": "ok = SHA-256 khớp; corrupt = truyền xong nhưng file sai; failed = lỗi",
    "network": "relay riêng trên 127.0.0.1, receiver đi qua proxy 127.0.0.2",
}

TRIAL_FIELDS = [
    "suite", "profile", "file", "bytes", "trial", "cut", "cut_at",
    "corrupt_chunks", "status", "sha256_ok", "cut_wire_bytes",
    "receiver_exit_after_cut", "sender_exit_after_cut", "present_chunks",
    "missing_chunks", "requested_chunks", "request_rounds",
    "requested_chunks_total", "needed_bytes", "resume_wire_bytes",
    "resume_seconds", "full_resend_ratio", "mv_events", "mv_corrupt_chunks",
    "mv_retransmitted_bytes", "mv_saved_bytes", "binary_sha256", "extra_args",
    "started_at", "run_dir", "error",
]  # fmt: skip

SUMMARY_FIELDS = [
    "suite", "profile", "cut", "corrupt_chunks", "bytes", "trials_total",
    "trials_ok", "trials_corrupt", "median_requested_chunks",
    "median_request_rounds", "median_requested_chunks_total", "median_needed_bytes", "median_resume_wire_bytes", "median_resume_seconds",
    "median_full_resend_ratio",
]  # fmt: skip


# --- Làm hỏng chunk ----------------------------------------------------------


def present_chunks(path, *, chunk_size=CHUNK_SIZE):
    """Chỉ số các chunk không toàn byte 0, tức đã nhận (cách croc tự đoán)."""
    found = []
    zero = bytes(chunk_size)
    with Path(path).open("rb") as stream:
        index = 0
        while block := stream.read(chunk_size):
            if block != zero[: len(block)]:
                found.append(index)
            index += 1
    return found


def corrupt_chunks(path, count, *, chunk_size=CHUNK_SIZE, seed=None, candidates=None):
    """Lật một byte trong count chunk khác nhau. Trả về danh sách vị trí đã sửa.

    candidates giới hạn các chunk được chọn (mặc định: mọi chunk).
    """
    path = Path(path)
    if not path.is_file():
        raise bench.BenchError(f"không thấy file: {path}")
    manifest = path.parent / "SHA256SUMS.txt"
    if manifest.is_file() and path.name in bench.read_checksums(path.parent):
        raise bench.BenchError(f"{path} là dữ liệu gốc trong SHA256SUMS.txt; hãy dùng bản sao")
    if path.stat().st_nlink > 1:
        raise bench.BenchError(f"{path} là hardlink; sửa sẽ làm hỏng cả file kia")
    size = path.stat().st_size
    total = (size + chunk_size - 1) // chunk_size if chunk_size > 0 else 0
    pool = range(total) if candidates is None else sorted(set(candidates))
    if pool and not 0 <= pool[0] <= pool[-1] < total:
        raise bench.BenchError(f"chỉ số chunk phải trong khoảng 0..{total - 1}")
    if not 1 <= count <= len(pool):
        raise bench.BenchError(f"số chunk phải trong khoảng 1..{len(pool)}")
    rng = random.Random(seed)
    changes = []
    with path.open("r+b") as stream:
        for index in sorted(rng.sample(pool, count)):
            start = index * chunk_size
            offset = rng.randrange(start, min(start + chunk_size, size))
            stream.seek(offset)
            before = stream.read(1)[0]
            stream.seek(offset)
            stream.write(bytes([before ^ 0xFF]))
            changes.append(
                {"chunk": index, "offset": offset, "before": before, "after": before ^ 0xFF}
            )
    return changes


# --- Proxy đếm byte ----------------------------------------------------------


class CountingProxy:
    """Chuyển tiếp TCP từ PROXY_HOST:port tới RELAY_HOST:port, cùng số cổng.

    croc nối cổng dữ liệu theo host của --relay và danh sách cổng relay gửi về,
    nên receiver dùng --relay PROXY_HOST:port0 là mọi kết nối đều đi qua đây.
    down là byte relay -> receiver, up là chiều ngược lại.
    """

    def __init__(self, ports, *, listen=PROXY_HOST, target=RELAY_HOST, rate=None):
        self.ports, self.target, self.rate = list(ports), target, rate
        self.lock = threading.Lock()
        self.down = self.up = 0
        self.blocked = False
        self.closed = False
        self.pairs = []
        self.listeners = []
        self.threads = []
        self._budget_start = time.monotonic()
        self._budget_bytes = 0
        try:
            for port in self.ports:
                listener = socket.socket()
                listener.bind((listen, port))
                listener.listen(16)
                listener.settimeout(0.2)
                self.listeners.append(listener)
        except OSError:
            self.close()
            raise
        for listener in self.listeners:
            self._start(self._accept, listener)

    def _start(self, target, *args):
        thread = threading.Thread(target=target, args=args, daemon=True)
        thread.start()
        self.threads.append(thread)

    def _accept(self, listener):
        port = listener.getsockname()[1]
        while not self.closed:
            try:
                client, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            client.settimeout(None)
            if self.blocked or self.closed:
                _abort(client)
                continue
            try:
                upstream = socket.create_connection((self.target, port), timeout=10)
                upstream.settimeout(None)
            except OSError:
                _abort(client)
                continue
            with self.lock:
                # cut() có thể chạy giữa accept và đây: kiểm tra lại dưới lock.
                dropped = self.blocked or self.closed
                if not dropped:
                    self.pairs.append((client, upstream))
            if dropped:
                _abort(client)
                _abort(upstream)
                continue
            self._start(self._pump, upstream, client, "down")
            self._start(self._pump, client, upstream, "up")

    def _pump(self, source, sink, direction):
        try:
            while data := source.recv(64 << 10):
                if direction == "down":
                    self._throttle(len(data))
                # Đếm trước khi chuyển để bên nhận không bao giờ thấy nhiều hơn số đếm.
                with self.lock:
                    if direction == "down":
                        self.down += len(data)
                    else:
                        self.up += len(data)
                sink.sendall(data)
        except OSError:
            pass
        # Một chiều hỏng thì đóng cả cặp, như một đường truyền bị đứt.
        for sock in (source, sink):
            _abort(sock, reset=False)

    def _throttle(self, size):
        if not self.rate:
            return
        with self.lock:
            self._budget_bytes += size
            due = self._budget_start + self._budget_bytes / self.rate
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def counters(self):
        with self.lock:
            return self.down, self.up

    def reset_counters(self):
        with self.lock:
            self.down = self.up = 0
            self._budget_start, self._budget_bytes = time.monotonic(), 0

    def active(self):
        with self.lock:
            self.pairs = [pair for pair in self.pairs if pair[0].fileno() != -1]
            return len(self.pairs)

    def block(self, blocked=True):
        """Từ chối (blocked) hoặc nhận lại kết nối mới; không đụng kết nối đang có."""
        with self.lock:
            self.blocked = blocked

    def cut(self, *, reset=True):
        """Chặn kết nối mới và cắt mọi kết nối đang có (RST nếu reset)."""
        with self.lock:
            self.blocked = True
            pairs, self.pairs = self.pairs, []
        for client, upstream in pairs:
            _abort(client, reset=reset)
            _abort(upstream, reset=reset)

    def close(self):
        self.closed = True
        self.cut(reset=False)
        for listener in self.listeners:
            listener.close()
        for thread in self.threads:
            thread.join(timeout=2)


def _abort(sock, *, reset=True):
    try:
        if reset:
            # Đóng với linger 0 gửi RST thay vì FIN.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, LINGER_RST)
        # shutdown đánh thức luồng đang chờ recv trên Linux; close thì không.
        sock.shutdown(socket.SHUT_RD if reset else socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


# --- Cắt kết nối -------------------------------------------------------------


def check_cut_method(method):
    if method != "ss":
        return
    if not sys.platform.startswith("linux"):
        raise bench.BenchError("--cut ss chỉ chạy trên Linux; trên OS khác dùng reset hoặc kill")
    if not shutil.which("ss"):
        raise bench.BenchError("không thấy lệnh ss (cài iproute2)")
    if os.geteuid() != 0:
        raise bench.BenchError("--cut ss cần quyền root (sudo -E python3 ...)")


def ss_kill_command(ports, host=PROXY_HOST):
    terms = []
    for port in ports:
        terms += (["or"] if terms else []) + ["dst", f"{host}:{port}"]
    return ["ss", "-K", "-t", "(", *terms, ")"]


def interrupt(child):
    """Gửi Ctrl+C vào nhóm tiến trình của child (Windows: CTRL_BREAK_EVENT)."""
    if child.process.poll() is not None:
        return
    try:
        if bench.WINDOWS:
            os.kill(child.process.pid, signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(child.process.pid, signal.SIGINT)
    except (OSError, ProcessLookupError):
        pass


def cut_connection(method, proxy, receiver, ports):
    if method == "kill":
        proxy.block()
        interrupt(receiver)
    elif method == "reset":
        proxy.cut()
    else:
        proxy.block()
        done = subprocess.run(ss_kill_command(ports), capture_output=True, text=True)
        if done.returncode:
            raise bench.BenchError(f"ss -K lỗi {done.returncode}: {done.stderr.strip()}")
        deadline = time.monotonic() + 5
        while proxy.active() and time.monotonic() < deadline:
            time.sleep(0.05)
        if proxy.active():
            raise bench.BenchError(
                "ss -K không hủy được socket (kernel thiếu CONFIG_INET_DIAG_DESTROY?)"
            )


# --- Một lượt đo -------------------------------------------------------------


def read_events(path):
    if not Path(path).is_file():
        return []
    events = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        try:
            events.append(json.loads(line))
        except ValueError as error:
            raise bench.BenchError(f"dòng telemetry không hợp lệ: {line!r}") from error
    return events


def merkle_totals(events):
    found = [event for event in events if event.get("event") == FEATURE_EVENT]
    totals = {"mv_events": len(found)}
    for field in ("corrupt_chunks", "retransmitted_bytes", "saved_bytes"):
        values = [event[field] for event in found if isinstance(event.get(field), int)]
        totals[f"mv_{field}"] = sum(values) if values else None
    return totals


def requested_chunks(output):
    """Số chunk receiver xin trong từng vòng. croc-base thấy hash cả file sai
    thì xin lại một vòng mới với toàn bộ file."""
    return [int(count) for count in READY_LINE.findall(bytes(output))]


def run_resume_trial(
    run_dir,
    *,
    binary,
    relay_binary,
    source,
    expected_sha256,
    cut,
    cut_at,
    corrupt,
    seed,
    feature_args=(),
    metrics=False,
    rate=None,
    settle=15,
    timeout=600,
    ready_timeout=120,
):
    """Một lượt cắt-làm hỏng-resume. Trả về dict; status ok, corrupt hoặc failed."""
    run_dir = Path(run_dir).resolve()
    source = Path(source).resolve()
    run_dir.mkdir(parents=True)
    send_dir, receive_dir = run_dir / "send", run_dir / "recv"
    send_dir.mkdir()
    receive_dir.mkdir()
    staged, received = send_dir / source.name, receive_dir / source.name
    size = source.stat().st_size
    total_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE
    result = {"status": "failed", "sha256_ok": False}
    proxy = None
    try:
        bench._link_or_copy(source, staged)
        ports = bench.free_ports(5)
        proxy = CountingProxy(ports, rate=rate)
        secret = transfer_secret()

        def common(host, phase):
            args = [
                Path(binary).resolve(), "--debug", "--relay", f"{host}:{ports[0]}",
                "--relay6", "", "--no-update-check", "--ignore-stdin",
                "--disable-clipboard", "--yes", *feature_args,
            ]  # fmt: skip
            if metrics:
                args += ["--metrics-out", run_dir / f"metrics-{phase}.jsonl"]
            return args

        with bench.ProcessGroup() as group:
            relay = group.spawn(
                "relay",
                [
                    Path(relay_binary).resolve(), "--debug", "relay",
                    "--host", RELAY_HOST, "--ports", ",".join(map(str, ports)),
                ],  # fmt: skip
                cwd=run_dir,
                env=bench.child_env(run_dir / "config-relay"),
                log_path=run_dir / "relay.log",
            )
            bench.wait_ready(
                "relay mở đủ cổng",
                lambda: all(
                    f"started TCP server on {RELAY_HOST}:{port}".encode() in relay.output
                    for port in ports
                ),
                [relay],
                ready_timeout,
            )

            def start_sender(phase):
                rooms = relay.output.count(b"room has first peer")
                sender = group.spawn(
                    f"sender-{phase}",
                    [
                        *common(RELAY_HOST, f"sender-{phase}"), "send", "--no-local",
                        "--transport", "relay", source.name,
                    ],  # fmt: skip
                    cwd=send_dir,
                    env=bench.child_env(run_dir / "config-send", secret),
                    log_path=run_dir / f"sender-{phase}.log",
                )
                bench.wait_ready(
                    "sender chuẩn bị file và vào phòng relay",
                    lambda: relay.output.count(b"room has first peer") > rooms
                    and f"Sending '{source.name}' (".encode() in sender.output,
                    [relay, sender],
                    ready_timeout,
                )
                return sender

            def start_receiver(phase):
                # --yes không trả lời câu "Resume ...? (y/N)"; thiếu --overwrite
                # thì receiver bỏ qua file.
                return group.spawn(
                    f"receiver-{phase}",
                    [
                        *common(PROXY_HOST, f"receiver-{phase}"), "--overwrite",
                        "--out", receive_dir,
                    ],  # fmt: skip
                    cwd=receive_dir,
                    env=bench.child_env(run_dir / "config-recv", secret),
                    log_path=run_dir / f"receiver-{phase}.log",
                )

            # Lượt 1: truyền tới cut_at rồi cắt.
            sender = start_sender("first")
            receiver = start_receiver("first")
            bench.wait_ready(
                f"receiver nhận {cut_at:.0%}",
                lambda: proxy.counters()[0] >= cut_at * size,
                [relay, sender, receiver],
                timeout,
            )
            cut_connection(cut, proxy, receiver, ports)
            result["cut_wire_bytes"] = proxy.counters()[0]
            # croc tự thử nối lại một lúc; chờ cả hai bên bỏ cuộc rồi mới resume.
            deadline = time.monotonic() + settle
            for name, child in (("receiver", receiver), ("sender", sender)):
                while not child.done.is_set() and time.monotonic() < deadline:
                    time.sleep(0.05)
                if child.done.is_set():
                    result[f"{name}_exit_after_cut"] = child.process.returncode
                else:
                    result[f"{name}_exit_after_cut"] = "still-running"
                    stop_process(child.process, group=not bench.WINDOWS)
                    child.wait(10)
            if not received.is_file():
                raise bench.BenchError("receiver không giữ lại file dở dang sau khi bị cắt")
            if received.stat().st_size != size:
                raise bench.BenchError(
                    f"file dở dang có {received.stat().st_size} byte, cần {size}"
                )

            # Làm hỏng các chunk đã nhận.
            present = present_chunks(received)
            result["present_chunks"] = len(present)
            result["missing_chunks"] = total_chunks - len(present)
            if result["missing_chunks"] == 0:
                raise bench.BenchError("file đã nhận đủ trước khi cắt; giảm --cut-at hoặc đặt --rate")
            if corrupt:
                changes = corrupt_chunks(received, corrupt, seed=seed, candidates=present)
                bench.write_json(run_dir / "corruption.json", changes)
            result["needed_bytes"] = min(
                size, (result["missing_chunks"] + corrupt) * CHUNK_SIZE
            )

            # Lượt 2: chạy lại cả hai bên với cùng mã, receiver vào đúng thư mục cũ.
            sender = start_sender("resume")
            proxy.reset_counters()
            proxy.block(False)
            receiver = start_receiver("resume")
            code = receiver.wait(timeout)
            result["resume_wire_bytes"] = proxy.counters()[0]
            result["resume_seconds"] = receiver.ended - receiver.started
            rounds = requested_chunks(receiver.output)
            if rounds:
                result.update(
                    requested_chunks=rounds[0],
                    request_rounds=len(rounds),
                    requested_chunks_total=sum(rounds),
                )
            if code:
                raise bench.BenchError(f"receiver lượt resume thoát với mã {code}")
            if not rounds:
                raise bench.BenchError("receiver không resume (không thấy dòng 'recipient ready')")
            if code := sender.wait(30):
                raise bench.BenchError(f"sender thoát với mã {code}")

        result["full_resend_ratio"] = result["resume_wire_bytes"] / size
        if metrics:
            events = []
            for name in ("receiver-resume", "sender-first", "sender-resume"):
                events += read_events(run_dir / f"metrics-{name}.jsonl")
            result.update(merkle_totals(events))
        if not received.is_file():
            raise bench.BenchError("không thấy file nhận sau lượt resume")
        if sha256(received) == expected_sha256:
            result.update(status="ok", sha256_ok=True)
        else:
            result.update(status="corrupt", error="truyền xong nhưng SHA-256 lệch")
    except (bench.BenchError, OSError) as error:
        result["error"] = str(error)
    finally:
        if proxy:
            proxy.close()
        # Giữ log, bỏ payload để không đầy đĩa.
        staged.unlink(missing_ok=True)
        received.unlink(missing_ok=True)
    return result


# --- Tổng hợp ----------------------------------------------------------------


def summarize(rows):
    groups = {}
    for row in rows:
        key = tuple(str(row[name]) for name in ("profile", "cut", "corrupt_chunks", "bytes"))
        groups.setdefault(key, []).append(row)
    summary = []
    for (profile, cut, corrupt, size), group in groups.items():
        done = [row for row in group if row["status"] in ("ok", "corrupt")]

        def median(name):
            found = [float(row[name]) for row in done if row.get(name) not in (None, "")]
            return statistics.median(found) if found else None

        summary.append(
            {
                "suite": SUITE, "profile": profile, "cut": cut, "corrupt_chunks": corrupt,
                "bytes": size, "trials_total": len(group),
                "trials_ok": sum(row["status"] == "ok" for row in group),
                "trials_corrupt": sum(row["status"] == "corrupt" for row in group),
                "median_requested_chunks": median("requested_chunks"),
                "median_request_rounds": median("request_rounds"),
                "median_requested_chunks_total": median("requested_chunks_total"),
                "median_needed_bytes": median("needed_bytes"),
                "median_resume_wire_bytes": median("resume_wire_bytes"),
                "median_resume_seconds": median("resume_seconds"),
                "median_full_resend_ratio": median("full_resend_ratio"),
            }
        )  # fmt: skip
    return summary


def markdown_table(summary):
    def number(value, digits=0, scale=1):
        return "" if value is None else f"{value / scale:.{digits}f}"

    lines = [
        "| Profile | Cắt | Chunk hỏng | File đúng | Truyền xong, file sai "
        "| Chunk xin vòng đầu | Số vòng | Tổng chunk xin | Cần gửi (KiB) "
        "| Đã gửi (KiB) | % file | Resume (s) |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in summary:
        ratio = row["median_full_resend_ratio"]
        lines.append(
            f"| {row['profile']} | {row['cut']} | {row['corrupt_chunks']} "
            f"| {row['trials_ok']}/{row['trials_total']} "
            f"| {row['trials_corrupt']}/{row['trials_total']} "
            f"| {number(row['median_requested_chunks'])} "
            f"| {number(row['median_request_rounds'])} "
            f"| {number(row['median_requested_chunks_total'])} "
            f"| {number(row['median_needed_bytes'], 0, 1024)} "
            f"| {number(row['median_resume_wire_bytes'], 0, 1024)} "
            f"| {number(None if ratio is None else ratio * 100, 1)} "
            f"| {number(row['median_resume_seconds'], 3)} |"
        )
    return "\n".join(lines) + "\n"


# --- Chương trình ------------------------------------------------------------


def parse(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base", type=Path, default=bench.DEFAULT_BASE)
    parser.add_argument("--new", type=Path, help="binary croc-upgraded")
    parser.add_argument("--new-global-arg", action="append", default=[])
    parser.add_argument(
        "--data-dir", type=Path, default=bench.CROC_ROOT / "tmp" / "datasets"
    )
    parser.add_argument("--out", type=Path, help="thư mục kết quả mới hoặc rỗng")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cut", choices=CUT_METHODS, default="kill")
    parser.add_argument("--cut-at", type=float, default=0.5, help="tỉ lệ đã nhận khi cắt")
    parser.add_argument(
        "--corrupt-chunks", type=int, nargs="+", default=[0, 3],
        help="số chunk lật byte sau khi cắt; mỗi giá trị là một kịch bản",
    )  # fmt: skip
    parser.add_argument("--rate", type=float, help="giới hạn MiB/s về receiver (mặc định không)")
    parser.add_argument("--settle", type=float, default=15, help="giây chờ receiver tự thoát sau khi cắt")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=600, help="giây cho mỗi lượt truyền")
    # Công cụ làm hỏng chunk độc lập.
    parser.add_argument("--corrupt", type=Path, metavar="FILE")
    parser.add_argument("--chunks", type=int, default=1)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    args = parser.parse_args(argv)
    if args.repeats < 1:
        parser.error("--repeats phải >= 1")
    if not 0 < args.cut_at < 1:
        parser.error("--cut-at phải trong khoảng (0, 1)")
    if any(n < 0 for n in args.corrupt_chunks):
        parser.error("--corrupt-chunks phải >= 0")
    if args.rate is not None and args.rate <= 0:
        parser.error("--rate phải > 0")
    if args.timeout <= 0 or args.settle < 0:
        parser.error("--timeout phải > 0, --settle phải >= 0")
    return args


def run(args):
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out = bench.prepare_out_dir(
        args.out or bench.CROC_ROOT / "tmp" / "results" / f"{SUITE}-{stamp}"
    )

    missing = []
    if not args.base.is_file() and not args.new:
        missing.append(f"không thấy {args.base} và không có --new")
    if not (args.data_dir / "SHA256SUMS.txt").is_file():
        missing.append(f"không thấy bộ dữ liệu {args.data_dir} (chạy gen-datasets.sh)")
    if missing:
        bench.write_json(out / "status.json", {"suite": SUITE, "status": "pending", "missing": missing})
        print(f"[{SUITE}] KHÔNG ĐO, không có số liệu.")
        for item in missing:
            print(f"  [THIẾU] {item}")
        return 0
    check_cut_method(args.cut)

    profiles = []
    if args.base.is_file():
        profiles.append({"name": "base", **bench.binary_info(args.base), "global": [], "metrics": False})
    notes = []
    if args.new:
        new = bench.binary_info(args.new)
        if profiles and new["sha256"] == profiles[0]["sha256"]:
            raise bench.BenchError("--new giống hệt --base; không có gì để so sánh")
        absent = bench.missing_flags(args.new, [FEATURE_FLAG, "--metrics-out"])
        if absent:
            raise bench.BenchError(f"binary --new thiếu cờ {', '.join(absent)}")
        profiles.append(
            {
                "name": "upgraded", **new,
                "global": [FEATURE_FLAG, *args.new_global_arg], "metrics": True,
            }
        )  # fmt: skip
        notes.append(
            "profile upgraded chỉ thật sự sửa chunk khi cột mv_events > 0 "
            "(MRK-04/05 đã phát merkle_verify)"
        )
    relay_binary = profiles[0]["path"]

    (data,) = bench.load_datasets(args.data_dir, ["random"])
    if shutil.disk_usage(out).free < 2 * data["bytes"] + 256 * bench.MIB:
        raise bench.BenchError("không đủ đĩa trống cho lượt đo")

    meta = {
        "suite": SUITE,
        "status": "running",
        "started_at": datetime.datetime.now().astimezone().isoformat(),
        "argv": sys.argv,
        "host": bench.host_info(),
        "relay_binary": relay_binary,
        "profiles": profiles,
        "dataset": data,
        "repeats": args.repeats,
        "cut": args.cut,
        "cut_at": args.cut_at,
        "corrupt_chunks": args.corrupt_chunks,
        "rate_mib_s": args.rate,
        "seed": args.seed,
        "chunk_size": CHUNK_SIZE,
        "measurement": MEASUREMENT,
        "notes": notes,
    }
    bench.write_json(out / "meta.json", meta)

    rows = []
    log = bench.CsvLog(out / "trials.csv", TRIAL_FIELDS)
    try:
        for trial in range(1, args.repeats + 1):
            for corrupt in args.corrupt_chunks:
                for profile in profiles if trial % 2 else profiles[::-1]:
                    case = f"{trial:02}-corrupt{corrupt}-{profile['name']}"
                    started_at = datetime.datetime.now().astimezone().isoformat()
                    result = run_resume_trial(
                        out / "runs" / case,
                        binary=profile["path"],
                        relay_binary=relay_binary,
                        source=data["path"],
                        expected_sha256=data["sha256"],
                        cut=args.cut,
                        cut_at=args.cut_at,
                        corrupt=corrupt,
                        seed=args.seed * 1000 + trial,
                        feature_args=profile["global"],
                        metrics=profile["metrics"],
                        rate=args.rate * bench.MIB if args.rate else None,
                        settle=args.settle,
                        timeout=args.timeout,
                    )
                    row = {
                        "suite": SUITE, "profile": profile["name"],
                        "file": data["path"].name, "bytes": data["bytes"],
                        "trial": trial, "cut": args.cut, "cut_at": args.cut_at,
                        "corrupt_chunks": corrupt, "binary_sha256": profile["sha256"],
                        "extra_args": " ".join(profile["global"]),
                        "started_at": started_at, "run_dir": f"runs/{case}", **result,
                    }  # fmt: skip
                    log.write(row)
                    rows.append(row)
                    if result["status"] == "failed":
                        print(f"{case}: LỖI - {result['error']}", file=sys.stderr, flush=True)
                    else:
                        print(
                            f"{case}: xin lại {result['requested_chunks_total']} chunk "
                            f"qua {result['request_rounds']} vòng, "
                            f"gửi lại {result['resume_wire_bytes'] / 1024:.0f} KiB "
                            f"({result['full_resend_ratio']:.1%} file), "
                            f"{'SHA-256 khớp' if result['sha256_ok'] else 'FILE SAI'}",
                            flush=True,
                        )
        meta["status"] = "complete"
    finally:
        log.close()
        if meta["status"] != "complete":
            meta["status"] = "interrupted"
        meta["finished_at"] = datetime.datetime.now().astimezone().isoformat()
        bench.write_json(out / "meta.json", meta)
        summary = summarize(rows)
        summary_log = bench.CsvLog(out / "summary.csv", SUMMARY_FIELDS)
        for row in summary:
            summary_log.write(row)
        summary_log.close()
        table = markdown_table(summary)
        (out / "summary.md").write_text(table, encoding="utf-8")

    print("\n" + table)
    print(f"Kết quả: {out}")
    failed = sum(row["status"] == "failed" for row in rows)
    if failed:
        print(f"{failed}/{len(rows)} lượt lỗi, đã loại khỏi thống kê", file=sys.stderr)
    return 1 if failed else 0


def main(argv=None):
    bench.setup_console()
    args = parse(argv)
    try:
        if args.corrupt:
            changes = corrupt_chunks(
                args.corrupt, args.chunks, chunk_size=args.chunk_size, seed=args.seed
            )
            print(json.dumps({"file": str(args.corrupt), "changes": changes}, indent=2))
            return 0
        bench.install_signal_handlers()
        return run(args)
    except bench.BenchError as error:
        print(f"Lỗi: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Đã ngắt; relay, sender và receiver đã được dừng.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
