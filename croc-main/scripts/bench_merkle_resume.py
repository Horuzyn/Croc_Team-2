#!/usr/bin/env python3
"""INT-03: Merkle resume (MRK). CHƯA ĐO: có sẵn công cụ làm hỏng chunk.

Làm hỏng N chunk của một file dở dang trên đĩa (sửa tại chỗ, mỗi chunk lật 1 byte):
    python bench_merkle_resume.py --corrupt FILE --chunks 3 --seed 1

Dùng lại được từ croc_bench: ProcessGroup, wait_ready, free_ports, child_env,
load_datasets, CsvLog. run_transfer chỉ chạy một lượt trọn vẹn nên bài resume
cần luồng riêng: ngắt giữa chừng rồi chạy lại vào cùng thư mục nhận.

Còn phải viết sau khi nhóm có code:
  - ngắt đường truyền (Linux `ss -K`, hoặc dừng tiến trình);
  - chạy lại với `--merkle-resume`, đọc JSONL `merkle_verify` để lấy số byte gửi lại.

Không có --corrupt: chỉ kiểm tra điều kiện. Thoát 0 khi còn thiếu (không có số
liệu), 2 khi đủ điều kiện mà phần đo chưa được viết.
"""

import argparse
import json
from pathlib import Path
import random
import sys

import croc_bench as bench

# Kích thước lá theo docs/03_Long_MRK_Merkle_Resume.md; đối chiếu lại khi có src/merkle.
CHUNK_SIZE = 32 * 1024


def corrupt_chunks(path, count, *, chunk_size=CHUNK_SIZE, seed=None):
    """Lật một byte trong count chunk khác nhau. Trả về danh sách vị trí đã sửa."""
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
    if not 1 <= count <= total:
        raise bench.BenchError(f"số chunk phải trong khoảng 1..{total}")
    rng = random.Random(seed)
    changes = []
    with path.open("r+b") as stream:
        for index in sorted(rng.sample(range(total), count)):
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


def main(argv=None):
    bench.setup_console()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base", type=Path, default=bench.DEFAULT_BASE)
    parser.add_argument("--new", type=Path)
    parser.add_argument("--repeats", type=int, default=3, help="chưa dùng")
    parser.add_argument("--out", type=Path, help="ghi status.json vào thư mục mới")
    parser.add_argument("--corrupt", type=Path, metavar="FILE")
    parser.add_argument("--chunks", type=int, default=1)
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args(argv)
    try:
        if args.corrupt:
            changes = corrupt_chunks(
                args.corrupt, args.chunks, chunk_size=args.chunk_size, seed=args.seed
            )
            print(json.dumps({"file": str(args.corrupt), "changes": changes}, indent=2))
            return 0
        # Nếu MRK chỉ phát merkle_verify khi resume thì đổi cách chạy thử khi viết phần đo.
        conditions = bench.feature_conditions(
            args.new,
            args.base,
            ["--merkle-resume", "--metrics-out"],
            "merkle_verify",
            ["--merkle-resume"],
        )
        return bench.report_pending("merkle-resume", args.out, conditions)
    except bench.BenchError as error:
        print(f"Lỗi: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
