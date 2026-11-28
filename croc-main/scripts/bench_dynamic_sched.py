#!/usr/bin/env python3
"""INT-03: lập lịch chunk động (SCH) khi một luồng dữ liệu bị bóp băng thông.

Mỗi lượt truyền một file qua relay riêng trên localhost, có 4 luồng dữ liệu, và
một luồng bị bóp xuống --rates. Có 3 profile:
  base     croc-base
  static   croc-upgraded --sched static  (giữ cách croc chia chunk, không lấy lại)
  dynamic  croc-upgraded --sched dynamic (lấy lại chunk đang kẹt ở luồng chậm)

Hai cách bóp một luồng (--throttle):
  flag  cờ --debug-throttle-worker 0:<rate> ở sender (mặc định). croc-base không
        có cờ này nên profile base chạy không bóp, chỉ để tham chiếu.
  tc    Linux, quyền root: tc trên lo bóp cổng dữ liệu đầu tiên của relay, áp
        cho cả 3 profile. CHƯA chạy thử trên máy thật.

Chỉ đo baseline:
    python bench_dynamic_sched.py --data-dir DIR --out DIR_MỚI

So sánh static với dynamic:
    python bench_dynamic_sched.py --new bin/croc-upgraded --data-dir DIR --out DIR_MỚI

Kết quả: trials.csv (mỗi lượt), streams.csv (mỗi mẫu stream_stat của sender),
summary.csv, summary.md, meta.json; log và JSONL gốc trong runs/.
"""

import argparse
import contextlib
import datetime
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys

import croc_bench as bench

SUITE = "dynamic-sched"
THROTTLED_WORKER = 0
TC_DEVICE = "lo"
UNTHROTTLED = "none"
MEASUREMENT = {
    "seconds": "từ khi khởi chạy receiver đến khi receiver thoát; GỒM cả bắt tay",
    "mib_per_s": "kích thước file gốc (MiB) / seconds",
    "rate": "tốc độ của luồng bị bóp, cú pháp --throttleUpload (k = 1024 byte/s); "
    "'none' là không bóp",
    "stolen_chunks": "tổng stolen_chunks lớn nhất của từng worker trong JSONL của "
    "sender; chỉ có ở croc-upgraded",
    "streams.csv": "mọi bản ghi stream_stat của sender; t_seconds tính từ mẫu đầu "
    "của lượt; throughput_bps là byte/s do croc tự ước lượng",
    "network": "relay riêng trên 127.0.0.1, 4 luồng dữ liệu",
}

TRIAL_FIELDS = (
    bench.TRIAL_FIELDS[:2]
    + ["sched", "throttle", "rate", "rate_bytes_per_s"]
    + bench.TRIAL_FIELDS[2:]
    + ["workers", "stolen_chunks", "stream_samples"]
)
STREAM_FIELDS = [
    "profile", "rate", "trial", "t_seconds", "worker_id", "local_port",
    "throughput_bps", "inflight", "stolen_chunks",
]  # fmt: skip
SUMMARY_FIELDS = ["rate"] + bench.SUMMARY_FIELDS + ["median_stolen_chunks", "vs_static_pct"]


# --- Tốc độ và telemetry -----------------------------------------------------


def parse_rate(text):
    """Byte/s theo cú pháp --throttleUpload của croc: 512k, 2M, 1g hoặc số byte."""
    multipliers = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
    number, unit = text, 1
    if text and text[-1].lower() in multipliers:
        number, unit = text[:-1], multipliers[text[-1].lower()]
    if not number.isdigit() or int(number) <= 0:
        raise ValueError(f"tốc độ không hợp lệ {text!r} (vd: 512k, 32k, 2M)")
    return int(number) * unit


def read_stream_stats(path):
    """Các bản ghi stream_stat trong một file JSONL; file không có thì trả về []."""
    path = Path(path)
    if not path.is_file():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError as error:
            raise bench.BenchError(f"dòng telemetry không hợp lệ: {line!r}") from error
        if record.get("event") == "stream_stat":
            events.append(record)
    return events


def _timestamp(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def stream_totals(events):
    # stolen_chunks là bộ đếm cộng dồn của từng worker nên lấy giá trị lớn nhất.
    stolen = {}
    for event in events:
        worker = event["worker_id"]
        stolen[worker] = max(stolen.get(worker, 0), event["stolen_chunks"])
    return {
        "workers": len(stolen) if events else None,
        "stolen_chunks": sum(stolen.values()) if events else None,
        "stream_samples": len(events),
    }


def stream_rows(events, **fields):
    if not events:
        return []
    start = min(_timestamp(event["ts"]) for event in events)
    return [
        {
            **fields,
            "t_seconds": (_timestamp(event["ts"]) - start).total_seconds(),
            **{name: event.get(name) for name in STREAM_FIELDS[4:]},
        }
        for event in events
    ]


# --- Bóp một cổng bằng tc ----------------------------------------------------


def tc_commands(port, rate_bytes, device=TC_DEVICE):
    """Lệnh tc: mọi gói tới cổng port đi qua tbf, các gói khác vào băng 0 không giới hạn."""
    setup = [
        ["tc", "qdisc", "add", "dev", device, "root", "handle", "1:", "prio",
         "bands", "4", "priomap", *["0"] * 16],
        ["tc", "qdisc", "add", "dev", device, "parent", "1:4", "handle", "40:", "tbf",
         "rate", f"{rate_bytes * 8}bit", "burst", "131072", "latency", "5s"],
        ["tc", "filter", "add", "dev", device, "parent", "1:", "protocol", "ip",
         "prio", "1", "u32", "match", "ip", "dport", str(port), "0xffff", "flowid", "1:4"],
    ]  # fmt: skip
    cleanup = ["tc", "qdisc", "del", "dev", device, "root"]
    return setup, cleanup


def check_tc():
    if not sys.platform.startswith("linux"):
        raise bench.BenchError("--throttle tc chỉ chạy trên Linux; dùng --throttle flag")
    if os.geteuid() != 0:
        raise bench.BenchError("--throttle tc cần quyền root (sudo)")
    if not shutil.which("tc"):
        raise bench.BenchError("không thấy lệnh tc (cài iproute2)")
    shown = subprocess.run(
        ["tc", "qdisc", "show", "dev", TC_DEVICE], capture_output=True, text=True, check=True
    ).stdout
    # Không đè cấu hình tc sẵn có của máy.
    if " root " in f" {shown} " and "noqueue" not in shown:
        raise bench.BenchError(f"{TC_DEVICE} đã có qdisc gốc, không ghi đè: {shown.strip()}")


@contextlib.contextmanager
def tc_throttle(port, rate_bytes):
    setup, cleanup = tc_commands(port, rate_bytes)
    try:
        for command in setup:
            subprocess.run(command, check=True, capture_output=True)
        yield
    except subprocess.CalledProcessError as error:
        raise bench.BenchError(
            f"{' '.join(error.cmd)}: {error.stderr.decode(errors='replace').strip()}"
        ) from error
    finally:
        subprocess.run(cleanup, capture_output=True)


# --- Kết quả -----------------------------------------------------------------


def summarize(rows):
    summary = []
    for rate in dict.fromkeys(str(row["rate"]) for row in rows):
        group = [row for row in rows if str(row["rate"]) == rate]
        for entry in bench.summarize(group):
            stolen = [
                float(row["stolen_chunks"])
                for row in group
                if row["profile"] == entry["profile"]
                and row["status"] == "ok"
                and row.get("stolen_chunks") not in (None, "")
            ]
            summary.append(
                {
                    "rate": rate,
                    **entry,
                    "median_stolen_chunks": statistics.median(stolen) if stolen else None,
                }
            )
    # So với static ở cùng tốc độ: dương là nhanh hơn static.
    static = {
        row["rate"]: row["median_seconds"]
        for row in summary
        if row["profile"] == "static" and row["median_seconds"]
    }
    for row in summary:
        reference = static.get(row["rate"])
        row["vs_static_pct"] = (
            (reference - row["median_seconds"]) / reference * 100
            if reference and row["median_seconds"] is not None and row["profile"] != "static"
            else None
        )
    return summary


def markdown_table(summary):
    def number(value, digits):
        return "" if value is None else f"{value:.{digits}f}"

    lines = [
        "| Luồng bị bóp | Profile | Lượt đạt | Trung vị (s) | Trung vị MiB/s "
        "| Min–Max MiB/s | Chunk bị lấy lại | Nhanh hơn static |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in summary:
        spread = ""
        if row["min_mib_per_s"] is not None:
            spread = f"{row['min_mib_per_s']:.1f}–{row['max_mib_per_s']:.1f}"
        faster = "" if row["vs_static_pct"] is None else f"{row['vs_static_pct']:+.1f}%"
        lines.append(
            f"| {'không bóp' if row['rate'] == UNTHROTTLED else row['rate'] + '/s'} "
            f"| {row['profile']} | {row['trials_ok']}/{row['trials_total']} "
            f"| {number(row['median_seconds'], 3)} | {number(row['median_mib_per_s'], 1)} "
            f"| {spread} | {number(row['median_stolen_chunks'], 0)} | {faster} |"
        )
    return "\n".join(lines) + "\n"


# --- Chạy --------------------------------------------------------------------


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
    parser.add_argument(
        "--types", nargs="+", choices=list(bench.DATASET_PATTERNS), default=["random"]
    )
    parser.add_argument(
        "--rates", nargs="+", default=["512k", "32k"],
        help="tốc độ của luồng bị bóp (mặc định: 512k 32k)",
    )  # fmt: skip
    parser.add_argument("--throttle", choices=["flag", "tc"], default="flag")
    parser.add_argument("--timeout", type=float, default=600, help="giây cho mỗi lượt")
    args = parser.parse_args(argv)
    if args.repeats < 1:
        parser.error("--repeats phải >= 1")
    if args.timeout <= 0:
        parser.error("--timeout phải > 0")
    try:
        args.rate_bytes = {rate: parse_rate(rate) for rate in args.rates}
    except ValueError as error:
        parser.error(str(error))
    return args


def plan_cases(profiles, rates, throttle):
    """Danh sách (profile, rate) của một vòng lặp."""
    cases = []
    for profile in profiles:
        if profile["name"] == "base" and throttle == "flag":
            cases.append((profile, UNTHROTTLED))
        else:
            cases += [(profile, rate) for rate in rates]
    return cases


def profile_args(profile, rate, throttle, metrics):
    """(global_args, sender_global_args) của một lượt.

    Cờ chỉ có tác dụng ở sender; croc hiểu rate theo cú pháp của --throttleUpload.
    """
    sender = []
    if profile["metrics"]:
        sender += ["--metrics-out", metrics]
    if throttle == "flag" and rate != UNTHROTTLED and profile["name"] != "base":
        sender += ["--debug-throttle-worker", f"{THROTTLED_WORKER}:{rate}"]
    return profile["global"], sender


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
    if args.throttle == "tc":
        check_tc()

    profiles, notes = [], []
    if args.base.is_file():
        profiles.append(
            {"name": "base", **bench.binary_info(args.base), "global": [], "metrics": False}
        )
        if args.throttle == "flag":
            notes.append("croc-base không có --debug-throttle-worker: profile base chạy không bóp")
    if args.new:
        new = bench.binary_info(args.new)
        if profiles and new["sha256"] == profiles[0]["sha256"]:
            raise bench.BenchError("--new giống hệt --base; không có gì để so sánh")
        needed = ["--sched", "--metrics-out"]
        if args.throttle == "flag":
            needed.append("--debug-throttle-worker")
        if absent := bench.missing_flags(args.new, needed):
            raise bench.BenchError(f"binary --new thiếu cờ {', '.join(absent)}")
        for mode in ("static", "dynamic"):
            profiles.append(
                {
                    "name": mode, **new, "sched": mode,
                    "global": ["--sched", mode, *args.new_global_arg], "metrics": True,
                }
            )  # fmt: skip
    else:
        notes.append("không có --new: chỉ đo croc-base")
    relay_binary = profiles[0]["path"]

    datasets = bench.load_datasets(args.data_dir, args.types)
    largest = max(item["bytes"] for item in datasets)
    if shutil.disk_usage(out).free < 2 * largest + 256 * bench.MIB:
        raise bench.BenchError("không đủ đĩa trống cho lượt đo")

    meta = {
        "suite": SUITE,
        "status": "running",
        "started_at": datetime.datetime.now().astimezone().isoformat(),
        "argv": sys.argv,
        "host": bench.host_info(),
        "relay_binary": relay_binary,
        "profiles": profiles,
        "data_dir": str(args.data_dir.resolve()),
        "datasets": datasets,
        "repeats": args.repeats,
        "throttle": args.throttle,
        "throttled_worker": THROTTLED_WORKER if args.throttle == "flag" else "cổng dữ liệu đầu tiên của relay",
        "rates": args.rate_bytes,
        "cpu_method": bench.CPU_METHOD,
        "measurement": MEASUREMENT,
        "notes": notes,
    }
    bench.write_json(out / "meta.json", meta)

    rows = []
    log = bench.CsvLog(out / "trials.csv", TRIAL_FIELDS)
    streams = bench.CsvLog(out / "streams.csv", STREAM_FIELDS)
    cases = plan_cases(profiles, args.rates, args.throttle)
    try:
        for trial in range(1, args.repeats + 1):
            for data in datasets:
                # Đảo thứ tự ở lượt chẵn để giảm sai lệch theo thời gian.
                for profile, rate in cases if trial % 2 else cases[::-1]:
                    case = f"{trial:02}-{data['type']}-{profile['name']}-{rate}"
                    run_dir = (out / "runs" / case).resolve()
                    metrics = run_dir / "sender-metrics.jsonl"
                    rate_bytes = args.rate_bytes.get(rate)
                    global_args, sender_args = profile_args(
                        profile, rate, args.throttle, metrics
                    )
                    ports = bench.free_ports(5)
                    throttled = (
                        tc_throttle(ports[1], rate_bytes)
                        if args.throttle == "tc" and rate != UNTHROTTLED
                        else contextlib.nullcontext()
                    )
                    started_at = datetime.datetime.now().astimezone().isoformat()
                    with throttled:
                        result = bench.run_transfer(
                            run_dir,
                            binary=profile["path"],
                            relay_binary=relay_binary,
                            source=data["path"],
                            expected_sha256=data["sha256"],
                            global_args=global_args,
                            sender_global_args=sender_args,
                            timeout=args.timeout,
                            ports=ports,
                        )
                    events = read_stream_stats(metrics)
                    for sample in stream_rows(events, profile=profile["name"], rate=rate, trial=trial):
                        streams.write(sample)
                    row = {
                        "suite": SUITE,
                        "profile": profile["name"],
                        "sched": profile.get("sched", ""),
                        "throttle": args.throttle if rate != UNTHROTTLED else UNTHROTTLED,
                        "rate": rate,
                        "rate_bytes_per_s": rate_bytes,
                        "dataset": data["type"],
                        "file": data["path"].name,
                        "bytes": data["bytes"],
                        "trial": trial,
                        "binary_sha256": profile["sha256"],
                        "extra_args": " ".join(map(str, global_args + sender_args)),
                        "started_at": started_at,
                        "run_dir": f"runs/{case}",
                        **result,
                        **stream_totals(events),
                    }
                    log.write(row)
                    rows.append(row)
                    if result["status"] == "ok":
                        stolen = row["stolen_chunks"]
                        print(
                            f"{case}: {result['seconds']:.3f}s, {result['mib_per_s']:.1f} MiB/s"
                            + ("" if stolen is None else f", lấy lại {stolen} chunk")
                            + ", SHA-256 khớp",
                            flush=True,
                        )
                    else:
                        print(f"{case}: LỖI - {result['error']}", file=sys.stderr, flush=True)
        meta["status"] = "complete"
    finally:
        log.close()
        streams.close()
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
    silent = [
        row for row in rows
        if row["status"] == "ok" and profile_by_name(profiles, row["profile"])["metrics"]
        and not row["stream_samples"]
    ]  # fmt: skip
    if silent:
        print(
            f"CẢNH BÁO: {len(silent)} lượt của croc-upgraded không ghi stream_stat",
            file=sys.stderr,
        )
    failed = sum(row["status"] != "ok" for row in rows)
    if failed:
        print(f"{failed}/{len(rows)} lượt lỗi, đã loại khỏi thống kê", file=sys.stderr)
    return 1 if failed else 0


def profile_by_name(profiles, name):
    return next(profile for profile in profiles if profile["name"] == name)


def main(argv=None):
    bench.setup_console()
    args = parse(argv)
    bench.install_signal_handlers()
    try:
        return run(args)
    except bench.BenchError as error:
        print(f"Lỗi: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Đã ngắt; relay, sender và receiver đã được dừng.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
