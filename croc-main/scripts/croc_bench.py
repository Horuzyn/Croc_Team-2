"""Phần đo dùng chung cho benchmark INT-03: gửi-nhận qua relay riêng trên localhost.

Chạy được trên Windows và Linux, chỉ dùng thư viện chuẩn.
"""

import csv
import ctypes
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time

from bundle_bench_common import sha256, stop_process, transfer_secret

WINDOWS = os.name == "nt"
CROC_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASE = CROC_ROOT / "bin" / ("croc-base.exe" if WINDOWS else "croc-base")
MIB = 1 << 20

DATASET_PATTERNS = {
    "text": "dataset_text_*.log",
    "zip": "dataset_compressed_*.zip",
    "random": "testfile_*.bin",
    "mixed": "dataset_mixed_*.bin",
}

TRIAL_FIELDS = [
    "suite", "profile", "dataset", "file", "bytes", "trial", "status",
    "seconds", "mib_per_s", "sha256_ok", "sender_ready_seconds",
    "sender_cpu_s", "receiver_cpu_s", "receiver_cpu_pct", "cpu_method",
    "binary_sha256", "extra_args", "started_at", "run_dir", "error",
]  # fmt: skip

SUMMARY_FIELDS = [
    "suite", "profile", "dataset", "file", "bytes", "trials_total", "trials_ok",
    "median_seconds", "median_mib_per_s", "min_mib_per_s", "max_mib_per_s",
    "median_sender_cpu_s", "median_receiver_cpu_s",
]  # fmt: skip


class BenchError(RuntimeError):
    """Lỗi cấu hình hoặc lỗi của một lượt đo."""


def setup_console():
    # Console và pipe trên Windows mặc định không phải UTF-8.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def install_signal_handlers():
    def interrupt(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, interrupt)


# --- Quản lý tiến trình ------------------------------------------------------

if WINDOWS:
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
    ]  # fmt: skip
    _kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
        ctypes.POINTER(wintypes.FILETIME)
    ] * 4

    class _BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimits),
            ("IoInfo", ctypes.c_ulonglong * 6),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    def _kill_on_close_job():
        # Windows tự giết mọi tiến trình trong job khi runner chết, kể cả bị kill cứng.
        job = _kernel32.CreateJobObjectW(None, None)
        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE
        if not job or not _kernel32.SetInformationJobObject(
            job, 9, ctypes.byref(limits), ctypes.sizeof(limits)
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        return job

    def _windows_cpu_seconds(handle):
        times = [wintypes.FILETIME() for _ in range(4)]
        if not _kernel32.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)):
            return None
        # times[2] = kernel, times[3] = user, đơn vị 100 ns.
        return sum((t.dwHighDateTime << 32) + t.dwLowDateTime for t in times[2:]) / 1e7


CPU_METHOD = (
    "GetProcessTimes"
    if WINDOWS
    else "procfs"
    if sys.platform.startswith("linux")
    else "unsupported"
)


class Child:
    """Một tiến trình con: gom log qua pipe, ghi nhận lúc thoát và CPU đã dùng."""

    def __init__(self, name, args, *, cwd, env, log_path):
        self.name = name
        self.output = bytearray()
        self.done = threading.Event()
        self.ended = None
        self.cpu_seconds = None
        isolation = (
            {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            if WINDOWS
            else {"start_new_session": True}
        )
        self.started = time.perf_counter()
        # Pipe thay vì file log trực tiếp: progressbar sẽ fsync file thường mỗi lần vẽ.
        self.process = subprocess.Popen(
            [str(arg) for arg in args],
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            **isolation,
        )
        self.drain = threading.Thread(target=self._drain, args=(log_path,), daemon=True)
        self.waiter = threading.Thread(target=self._wait, daemon=True)
        self.drain.start()
        self.waiter.start()

    def _drain(self, log_path):
        with self.process.stdout as stream, open(log_path, "wb", buffering=0) as log:
            while block := stream.read1(64 << 10):
                log.write(block)
                if len(self.output) < 4 * MIB:
                    self.output.extend(block)

    def _wait(self):
        try:
            if WINDOWS:
                self.process.wait()
                self.ended = time.perf_counter()
                self.cpu_seconds = _windows_cpu_seconds(int(self.process._handle))
                return
            try:
                # WNOWAIT giữ tiến trình ở trạng thái zombie để còn đọc /proc.
                os.waitid(os.P_PID, self.process.pid, os.WEXITED | os.WNOWAIT)
                self.ended = time.perf_counter()
                stat = Path(f"/proc/{self.process.pid}/stat").read_text()
                fields = stat.rsplit(")", 1)[1].split()
                ticks = int(fields[11]) + int(fields[12])  # utime + stime
                self.cpu_seconds = ticks / os.sysconf("SC_CLK_TCK")
            except (AttributeError, OSError):
                pass
            self.process.wait()
            if self.ended is None:
                self.ended = time.perf_counter()
        finally:
            self.done.set()

    def wait(self, timeout):
        deadline = time.monotonic() + timeout
        while not self.done.wait(0.1):
            if time.monotonic() > deadline:
                raise BenchError(f"{self.name}: quá thời gian {timeout:g}s")
        # Đợi log được đọc hết trước khi kiểm tra.
        self.drain.join(timeout=5)
        return self.process.returncode


class ProcessGroup:
    """Mọi tiến trình spawn trong khối with đều bị dừng khi thoát khối."""

    def __init__(self):
        self.children = []
        self.job = None

    def __enter__(self):
        if WINDOWS:
            self.job = _kill_on_close_job()
        return self

    def spawn(self, name, args, **kwargs):
        child = Child(name, args, **kwargs)
        self.children.append(child)
        if self.job and not _kernel32.AssignProcessToJobObject(
            self.job, int(child.process._handle)
        ):
            if child.process.poll() is None:
                raise ctypes.WinError(ctypes.get_last_error())
        return child

    def __exit__(self, *exc):
        try:
            for child in reversed(self.children):
                stop_process(child.process, group=not WINDOWS)
            for child in self.children:
                child.waiter.join(timeout=5)
                child.drain.join(timeout=5)
        finally:
            if self.job:
                _kernel32.CloseHandle(self.job)
                self.job = None


def wait_ready(what, condition, alive, timeout):
    deadline = time.monotonic() + timeout
    while not condition():
        for child in alive:
            if child.done.is_set():
                raise BenchError(
                    f"{child.name} thoát (mã {child.process.returncode}) khi chờ {what}"
                )
        if time.monotonic() > deadline:
            raise BenchError(f"quá {timeout:g}s khi chờ {what}")
        time.sleep(0.02)


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def child_env(config_dir, secret=None):
    # Bỏ cấu hình croc và proxy của máy để mọi lượt đo giống nhau.
    env = {
        key: value
        for key, value in os.environ.items()
        if not (key.upper().startswith("CROC_") or key.upper().endswith("_PROXY"))
    }
    env["CROC_CONFIG_DIR"] = str(config_dir)
    if secret:
        env["CROC_SECRET"] = secret
    return env


def _link_or_copy(source, target):
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


# --- Một lượt gửi-nhận -------------------------------------------------------


def run_transfer(
    run_dir,
    *,
    binary,
    relay_binary,
    source,
    expected_sha256,
    global_args=(),
    sender_global_args=(),
    send_args=(),
    receive_args=(),
    timeout=600,
    ready_timeout=120,
    ports=None,
):
    """Truyền source một lần. Trả về dict; status là "ok" hoặc "failed".

    seconds tính từ lúc khởi chạy receiver đến lúc receiver thoát, gồm cả bắt tay.
    sender_global_args là cờ chung chỉ cho sender (đặt trước lệnh send).
    ports: 5 cổng relay chọn sẵn (cổng đầu để bắt tay, các cổng sau cho dữ liệu).
    KeyboardInterrupt được ném tiếp sau khi đã dừng hết tiến trình.
    """
    run_dir = Path(run_dir).resolve()
    source = Path(source).resolve()
    run_dir.mkdir(parents=True)
    send_dir, receive_dir = run_dir / "send", run_dir / "recv"
    send_dir.mkdir()
    receive_dir.mkdir()
    staged, received = send_dir / source.name, receive_dir / source.name
    result = {"status": "failed", "sha256_ok": False, "cpu_method": CPU_METHOD}
    try:
        _link_or_copy(source, staged)
        ports = list(ports) if ports else free_ports(5)
        secret = transfer_secret()
        common = [
            Path(binary).resolve(), "--relay", f"127.0.0.1:{ports[0]}", "--relay6", "",
            "--no-update-check", "--ignore-stdin", "--disable-clipboard", "--yes",
            *global_args,
        ]  # fmt: skip
        with ProcessGroup() as group:
            relay = group.spawn(
                "relay",
                [
                    Path(relay_binary).resolve(), "--debug", "relay",
                    "--host", "127.0.0.1", "--ports", ",".join(map(str, ports)),
                ],  # fmt: skip
                cwd=run_dir,
                env=child_env(run_dir / "config-relay"),
                log_path=run_dir / "relay.log",
            )
            wait_ready(
                "relay mở đủ cổng",
                lambda: all(
                    f"started TCP server on 127.0.0.1:{port}".encode() in relay.output
                    for port in ports
                ),
                [relay],
                ready_timeout,
            )
            sender = group.spawn(
                "sender",
                [
                    *common, *sender_global_args,
                    "send", "--no-local", "--transport", "relay",
                    *send_args, source.name,
                ],  # fmt: skip
                cwd=send_dir,
                env=child_env(run_dir / "config-send", secret),
                log_path=run_dir / "sender.log",
            )
            # Sender hash file song song với lúc nối relay nên phải chờ cả hai.
            wait_ready(
                "sender chuẩn bị file và vào phòng relay",
                lambda: b"room has first peer" in relay.output
                and f"Sending '{source.name}' (".encode() in sender.output,
                [relay, sender],
                ready_timeout,
            )
            result["sender_ready_seconds"] = time.perf_counter() - sender.started
            receiver = group.spawn(
                "receiver",
                [*common, *receive_args, "--out", receive_dir],
                cwd=receive_dir,
                env=child_env(run_dir / "config-recv", secret),
                log_path=run_dir / "receiver.log",
            )
            if code := receiver.wait(timeout):
                raise BenchError(f"receiver thoát với mã {code}")
            if code := sender.wait(30):
                raise BenchError(f"sender thoát với mã {code}")
        if not received.is_file():
            raise BenchError("không thấy file nhận")
        actual = sha256(received)
        if actual != expected_sha256:
            raise BenchError(
                f"SHA-256 lệch: nhận {actual[:16]}..., gốc {expected_sha256[:16]}..."
            )
        seconds = receiver.ended - receiver.started
        result.update(
            status="ok",
            sha256_ok=True,
            seconds=seconds,
            mib_per_s=source.stat().st_size / MIB / seconds,
            sender_cpu_s=sender.cpu_seconds,
            receiver_cpu_s=receiver.cpu_seconds,
        )
        if receiver.cpu_seconds is None or sender.cpu_seconds is None:
            result["cpu_method"] = "unsupported"
        else:
            result["receiver_cpu_pct"] = receiver.cpu_seconds / seconds * 100
    except (BenchError, OSError) as error:
        result["error"] = str(error)
    finally:
        # Giữ log, bỏ payload để không đầy đĩa.
        staged.unlink(missing_ok=True)
        received.unlink(missing_ok=True)
    return result


# --- Chạy thử xem tính năng có thật không ------------------------------------


def probe_events(binary, relay_binary, global_args, *, size=512 * 1024):
    """Truyền một file nhỏ nén được và trả về tên các sự kiện telemetry đã phát.

    Có cờ trong --help chưa chứng tỏ tính năng đã chạy: INT-01 khai báo cờ trước
    khi các thành viên cài đặt hành vi.
    """
    with tempfile.TemporaryDirectory() as work:
        work = Path(work)
        source, metrics = work / "probe.log", work / "metrics.jsonl"
        line = b"INFO probe chunk acknowledged by receiver\n"
        source.write_bytes((line * (size // len(line) + 1))[:size])
        result = run_transfer(
            work / "run",
            binary=binary,
            relay_binary=relay_binary,
            source=source,
            expected_sha256=sha256(source),
            global_args=[*global_args, "--metrics-out", metrics],
            timeout=120,
        )
        if result["status"] != "ok":
            raise BenchError(f"chạy thử {binary} lỗi: {result['error']}")
        events = set()
        if metrics.is_file():
            for record in metrics.read_text(encoding="utf-8").splitlines():
                try:
                    events.add(json.loads(record)["event"])
                except (ValueError, KeyError, TypeError) as error:
                    raise BenchError(f"dòng telemetry không hợp lệ: {record!r}") from error
        return events


# --- Đầu vào -----------------------------------------------------------------


def binary_info(path):
    path = Path(path).resolve()
    if not path.is_file():
        raise BenchError(f"không thấy binary: {path}")
    return {
        "path": str(path),
        "sha256": sha256(path),
        "version": _capture(path, "--version").strip(),
    }


def _capture(binary, *args):
    with tempfile.TemporaryDirectory() as config:
        try:
            done = subprocess.run(
                [str(Path(binary).resolve()), *args],
                env=child_env(config),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=30,
                check=True,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise BenchError(f"không chạy được {binary}: {error}") from error
    return (done.stdout + done.stderr).decode("utf-8", "replace")


def missing_flags(binary, flags):
    text = _capture(binary, "--help") + _capture(binary, "send", "--help")
    words = {word.strip(",") for word in text.split()}
    return [flag for flag in flags if flag not in words]


def read_checksums(data_dir):
    manifest = Path(data_dir) / "SHA256SUMS.txt"
    if not manifest.is_file():
        raise BenchError(f"không thấy {manifest}")
    entries = {}
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if line.strip():
            digest, name = line.split(maxsplit=1)
            entries[name.lstrip("*")] = digest.lower()
    return entries


def load_datasets(data_dir, types):
    data_dir = Path(data_dir).resolve()
    checksums = read_checksums(data_dir)
    datasets = []
    for kind in types:
        matches = sorted(data_dir.glob(DATASET_PATTERNS[kind]))
        if len(matches) != 1:
            raise BenchError(
                f"cần đúng 1 file {DATASET_PATTERNS[kind]} trong {data_dir}, "
                f"thấy {len(matches)}"
            )
        file = matches[0]
        digest = sha256(file)
        if checksums.get(file.name) != digest:
            raise BenchError(f"{file.name} không khớp SHA256SUMS.txt")
        datasets.append(
            {"type": kind, "path": file, "bytes": file.stat().st_size, "sha256": digest}
        )
    return datasets


def prepare_out_dir(out):
    out = Path(out).resolve()
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        raise BenchError(f"{out} đã có dữ liệu; chọn thư mục kết quả mới")
    out.mkdir(parents=True, exist_ok=True)
    return out


def host_info():
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }


# --- Kết quả -----------------------------------------------------------------


def write_json(path, value):
    Path(path).write_text(
        json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n",
        encoding="utf-8",
    )


def _format(value):
    if value is None:
        return ""
    return f"{value:.6f}" if isinstance(value, float) else value


class CsvLog:
    """Ghi và flush từng dòng để ngắt giữa chừng vẫn giữ được các lượt đã xong."""

    def __init__(self, path, fields):
        self.stream = open(path, "x", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.stream, fields)
        self.writer.writeheader()

    def write(self, row):
        self.writer.writerow({key: _format(value) for key, value in row.items()})
        self.stream.flush()

    def close(self):
        self.stream.close()


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def summarize(rows):
    """Trung vị theo (profile, dataset). Lượt không ok bị loại khỏi thống kê."""
    groups = {}
    for row in rows:
        key = tuple(row[name] for name in ("suite", "profile", "dataset", "file", "bytes"))
        groups.setdefault(key, []).append(row)
    summary = []
    for key, group in groups.items():
        ok = [row for row in group if row["status"] == "ok"]

        def values(name):
            return [float(row[name]) for row in ok if row.get(name) not in (None, "")]

        def median(name):
            found = values(name)
            return statistics.median(found) if found else None

        speeds = values("mib_per_s")
        summary.append(
            dict(
                zip(("suite", "profile", "dataset", "file", "bytes"), key),
                trials_total=len(group),
                trials_ok=len(ok),
                median_seconds=median("seconds"),
                median_mib_per_s=median("mib_per_s"),
                min_mib_per_s=min(speeds, default=None),
                max_mib_per_s=max(speeds, default=None),
                median_sender_cpu_s=median("sender_cpu_s"),
                median_receiver_cpu_s=median("receiver_cpu_s"),
            )
        )
    return summary


def markdown_table(summary):
    def number(value, digits):
        return "" if value is None else f"{value:.{digits}f}"

    lines = [
        "| Profile | Dữ liệu | MiB | Lượt đạt | Trung vị (s) | Trung vị MiB/s "
        "| Min–Max MiB/s | CPU sender (s) | CPU receiver (s) |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in summary:
        spread = ""
        if row["min_mib_per_s"] is not None:
            spread = f"{row['min_mib_per_s']:.1f}–{row['max_mib_per_s']:.1f}"
        lines.append(
            f"| {row['profile']} | {row['dataset']} | {int(row['bytes']) / MIB:.0f} "
            f"| {row['trials_ok']}/{row['trials_total']} "
            f"| {number(row['median_seconds'], 3)} "
            f"| {number(row['median_mib_per_s'], 1)} | {spread} "
            f"| {number(row['median_sender_cpu_s'], 3)} "
            f"| {number(row['median_receiver_cpu_s'], 3)} |"
        )
    return "\n".join(lines) + "\n"

