#!/usr/bin/env python3
"""INT-03: thông lượng và CPU khi truyền văn bản, ZIP và dữ liệu ngẫu nhiên.

Bản nâng cấp chỉ được đo khi một lượt chạy thử phát sự kiện chunk_compress;
nó luôn được gọi với --adaptive-compress. Lượt chạy thử không tính vào số đo.

Chỉ đo baseline:
    python bench_adaptive_compress.py --data-dir DIR --out DIR_MỚI

So sánh khi đã có bản nâng cấp:
    python bench_adaptive_compress.py --new bin/croc-upgraded \\
        --data-dir DIR --out DIR_MỚI
"""

import argparse
import datetime
from pathlib import Path
import shutil
import sys

import croc_bench as bench

SUITE = "adaptive-compress"
FEATURE_FLAG = "--adaptive-compress"
FEATURE_EVENT = "chunk_compress"
MEASUREMENT = {
    "seconds": "từ khi khởi chạy receiver đến khi receiver thoát; GỒM cả bắt tay "
    "PAKE và nối các cổng dữ liệu; KHÔNG gồm thời gian sender hash/chuẩn bị file",
    "mib_per_s": "kích thước file gốc (MiB) / seconds",
    "cpu": "user+kernel trên cả vòng đời của từng tiến trình sender và receiver, "
    "không gồm relay. Sender gồm cả hash và lấy mẫu nén trước khi receiver chạy",
    "receiver_cpu_pct": "receiver_cpu_s / seconds * 100 (100% = một lõi)",
    "network": "relay riêng trên 127.0.0.1, không giới hạn băng thông",
}


def parse(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base", type=Path, default=bench.DEFAULT_BASE)
    parser.add_argument("--new", type=Path, help="binary croc-upgraded")
    parser.add_argument("--new-global-arg", action="append", default=[])
    parser.add_argument("--new-send-arg", action="append", default=[])
    parser.add_argument("--new-receive-arg", action="append", default=[])
    parser.add_argument(
        "--data-dir", type=Path, default=bench.CROC_ROOT / "tmp" / "datasets"
    )
    parser.add_argument("--out", type=Path, help="thư mục kết quả mới hoặc rỗng")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--types",
        nargs="+",
        choices=list(bench.DATASET_PATTERNS),
        default=["text", "zip", "random"],
    )
    parser.add_argument("--timeout", type=float, default=600, help="giây cho mỗi lượt")
    args = parser.parse_args(argv)
    if args.repeats < 1:
        parser.error("--repeats phải >= 1")
    if args.timeout <= 0:
        parser.error("--timeout phải > 0")
    return args


def run(args):
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out = bench.prepare_out_dir(
        args.out or bench.CROC_ROOT / "tmp" / "results" / f"{SUITE}-{stamp}"
    )

    new, pending = None, None
    if args.new:
        info = bench.binary_info(args.new)
        new_global = [FEATURE_FLAG, *args.new_global_arg]
        if bench.missing_flags(args.new, [FEATURE_FLAG]):
            pending = f"binary --new chưa có cờ {FEATURE_FLAG}"
        elif FEATURE_EVENT not in bench.probe_events(
            args.new, args.base if args.base.is_file() else args.new, new_global
        ):
            pending = (
                f"binary --new có cờ {FEATURE_FLAG} nhưng chạy thử không phát "
                f"{FEATURE_EVENT} (tính năng chưa hoạt động)"
            )
        else:
            new = info
        if pending:
            pending += "; không đo profile 'upgraded'"
            print(f"CẢNH BÁO: {pending}", file=sys.stderr)

    if pending and not args.base.is_file():
        # Tình huống của CI hiện tại: chưa có tính năng và chưa có baseline.
        bench.write_json(
            out / "status.json",
            {"suite": SUITE, "status": "pending", "missing": [pending, f"không thấy {args.base}"]},
        )
        print(f"[{SUITE}] KHÔNG ĐO, không có số liệu.")
        return 0

    base = bench.binary_info(args.base)
    profiles = [{"name": "base", **base, "global": [], "send": [], "receive": []}]
    if new:
        if new["sha256"] == base["sha256"]:
            raise bench.BenchError("--new giống hệt --base; không có gì để so sánh")
        profiles.append(
            {
                "name": "upgraded",
                **new,
                "global": new_global,
                "send": args.new_send_arg,
                "receive": args.new_receive_arg,
            }
        )

    datasets = bench.load_datasets(args.data_dir, args.types)
    largest = max(item["bytes"] for item in datasets)
    # Cần chỗ cho bản staged (nếu không hardlink được) và file nhận.
    if shutil.disk_usage(out).free < 2 * largest + 256 * bench.MIB:
        raise bench.BenchError("không đủ đĩa trống cho lượt đo")

    meta = {
        "suite": SUITE,
        "status": "running",
        "started_at": datetime.datetime.now().astimezone().isoformat(),
        "argv": sys.argv,
        "host": bench.host_info(),
        "relay_binary": base["path"],
        "profiles": profiles,
        "data_dir": str(args.data_dir.resolve()),
        "datasets": datasets,
        "repeats": args.repeats,
        "cpu_method": bench.CPU_METHOD,
        "measurement": MEASUREMENT,
        "notes": [pending] if pending else [],
    }
    bench.write_json(out / "meta.json", meta)

    rows = []
    log = bench.CsvLog(out / "trials.csv", bench.TRIAL_FIELDS)
    try:
        # Xen kẽ loại dữ liệu và đảo thứ tự profile để giảm sai lệch theo thời gian.
        for trial in range(1, args.repeats + 1):
            for data in datasets:
                for profile in profiles if trial % 2 else profiles[::-1]:
                    case = f"{trial:02}-{data['type']}-{profile['name']}"
                    started_at = datetime.datetime.now().astimezone().isoformat()
                    result = bench.run_transfer(
                        out / "runs" / case,
                        binary=profile["path"],
                        relay_binary=base["path"],
                        source=data["path"],
                        expected_sha256=data["sha256"],
                        global_args=profile["global"],
                        send_args=profile["send"],
                        receive_args=profile["receive"],
                        timeout=args.timeout,
                    )
                    row = {
                        "suite": SUITE,
                        "profile": profile["name"],
                        "dataset": data["type"],
                        "file": data["path"].name,
                        "bytes": data["bytes"],
                        "trial": trial,
                        "binary_sha256": profile["sha256"],
                        "extra_args": " ".join(
                            profile["global"] + profile["send"] + profile["receive"]
                        ),
                        "started_at": started_at,
                        "run_dir": f"runs/{case}",
                        **result,
                    }
                    log.write(row)
                    rows.append(row)
                    if result["status"] == "ok":
                        print(
                            f"{case}: {result['seconds']:.3f}s, "
                            f"{result['mib_per_s']:.1f} MiB/s, SHA-256 khớp",
                            flush=True,
                        )
                    else:
                        print(f"{case}: LỖI - {result['error']}", file=sys.stderr, flush=True)
        meta["status"] = "complete"
    finally:
        log.close()
        if meta["status"] != "complete":
            meta["status"] = "interrupted"
        meta["finished_at"] = datetime.datetime.now().astimezone().isoformat()
        bench.write_json(out / "meta.json", meta)
        summary = bench.summarize(rows)
        summary_log = bench.CsvLog(out / "summary.csv", bench.SUMMARY_FIELDS)
        for row in summary:
            summary_log.write(row)
        summary_log.close()
        table = bench.markdown_table(summary)
        (out / "summary.md").write_text(table, encoding="utf-8")

    print("\n" + table)
    print(f"Kết quả: {out}")
    failed = sum(row["status"] != "ok" for row in rows)
    if failed:
        print(f"{failed}/{len(rows)} lượt lỗi, đã loại khỏi thống kê", file=sys.stderr)
    return 1 if failed else 0


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
