#!/usr/bin/env python3
"""INT-03: lập lịch chunk động (SCH). CHƯA ĐO: mới chỉ kiểm tra điều kiện.

Dùng lại được từ croc_bench: run_transfer, load_datasets, CsvLog, summarize,
ProcessGroup, wait_ready.

Còn phải viết sau khi nhóm có code:
  - lượt A/B `--sched static` và `--sched dynamic` của croc-upgraded;
  - đọc JSONL `stream_stat` theo schema trong src/telemetry;
  - bóp băng thông một cổng dữ liệu: Linux `ip netns` + `tc` (quyền root),
    hoặc cờ `--debug-throttle-worker` nếu nhóm triển khai.

Thoát 0 khi còn thiếu điều kiện (không có số liệu), 2 khi đủ điều kiện mà phần đo
chưa được viết.
"""

import argparse
import os
from pathlib import Path
import shutil
import sys

import croc_bench as bench


def main(argv=None):
    bench.setup_console()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base", type=Path, default=bench.DEFAULT_BASE)
    parser.add_argument("--new", type=Path)
    parser.add_argument("--repeats", type=int, default=3, help="chưa dùng")
    parser.add_argument("--out", type=Path, help="ghi status.json vào thư mục mới")
    args = parser.parse_args(argv)
    try:
        # Giới hạn tốc độ để lượt chạy thử đủ dài cho mẫu stream_stat (200 ms/mẫu).
        conditions = bench.feature_conditions(
            args.new,
            args.base,
            ["--sched", "--metrics-out"],
            "stream_stat",
            ["--sched", "dynamic", "--throttleUpload", "4M"],
            size=4 * bench.MIB,
        )
        netem = (
            sys.platform.startswith("linux")
            and os.geteuid() == 0
            and bool(shutil.which("ip") and shutil.which("tc"))
        )
        worker_flag = bool(args.new) and not bench.missing_flags(
            args.new, ["--debug-throttle-worker"]
        )
        conditions.append(
            (
                "cách bóp một luồng: Linux root có ip/tc, hoặc cờ --debug-throttle-worker (chỉ kiểm tra có cờ)",
                netem or worker_flag,
            )
        )
        return bench.report_pending("dynamic-sched", args.out, conditions)
    except bench.BenchError as error:
        print(f"Lỗi: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
