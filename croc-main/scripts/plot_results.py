#!/usr/bin/env python3
"""Vẽ biểu đồ PNG từ trials.csv của 3 bộ benchmark INT-03 (cần matplotlib).

    python plot_results.py --input THƯ_MỤC_KẾT_QUẢ --out THƯ_MỤC_BIỂU_ĐỒ

Tìm mọi trials.csv dưới --input; mỗi file ra một PNG, đặt tên theo đường dẫn
tương đối của thư mục chứa nó. Nhận các suite adaptive-compress, dynamic-sched
và merkle-resume.
"""

import argparse
from pathlib import Path
import statistics
import sys

import croc_bench as bench

SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e1e0d9"
# Màu cố định theo profile, theo thứ tự bảng phân loại; static (xanh ngọc) có
# tương phản 2.47:1 nên mọi cột đều mang nhãn số.
PROFILES = {
    "base": ("croc-base", "#2a78d6"),
    "static": ("croc-upgraded --sched static", "#00b894"),
    "upgraded": ("croc-upgraded", "#eb6834"),
    "dynamic": ("croc-upgraded --sched dynamic", "#eb6834"),
}
DATASET_LABELS = {"text": "Văn bản", "zip": "ZIP", "random": "Ngẫu nhiên", "mixed": "Hỗn hợp"}


def _size(rows):
    return f"{int(rows[0]['bytes']) / bench.MIB:.0f} MiB"


def _compress_group(value, rows):
    return f"{DATASET_LABELS.get(value, value)}\n{_size(rows)}"


def _sched_group(value, rows):
    return "không bóp" if value == "none" else f"luồng chậm\n{value}/s"


def _merkle_group(value, rows):
    return f"{value} chunk hỏng"


def _sched_note(rows):
    note = "Cột: trung vị các lượt đạt; chấm: từng lượt. Relay localhost, 4 luồng."
    if any(row["profile"] == "base" and row.get("rate") == "none" for row in rows):
        note += " croc-base không có --debug-throttle-worker nên chạy không bóp."
    return note


SUITES = {
    "adaptive-compress": {
        "title": "Nén theo dữ liệu",
        "group": "dataset",
        "group_label": _compress_group,
        "done": ("ok",),
        "panels": [
            ("mib_per_s", 1, "Thông lượng (MiB/s)", "{:.1f}"),
            ("receiver_cpu_s", 1, "CPU tiến trình receiver (giây)", "{:.3f}"),
        ],
        "note": lambda rows: "Cột: trung vị các lượt đạt; chấm: từng lượt. "
        "Relay localhost; thời gian gồm bắt tay.",
    },
    "dynamic-sched": {
        "title": "Lập lịch chunk khi một luồng bị bóp",
        "group": "rate",
        "group_label": _sched_group,
        "done": ("ok",),
        "panels": [
            ("seconds", 1, "Thời gian truyền (giây, thấp hơn là tốt)", "{:.2f}"),
            ("stolen_chunks", 1, "Chunk lấy lại từ luồng chậm", "{:.0f}"),
        ],
        "note": _sched_note,
    },
    "merkle-resume": {
        "title": "Resume sau khi cắt kết nối",
        "group": "corrupt_chunks",
        "group_label": _merkle_group,
        # corrupt: truyền xong nhưng file sai; vẫn đo được lượng byte đã gửi lại.
        "done": ("ok", "corrupt"),
        "panels": [
            ("full_resend_ratio", 100, "Byte gửi lại khi resume (% kích thước file)", "{:.0f}%"),
            ("resume_seconds", 1, "Thời gian resume (giây)", "{:.2f}"),
        ],
        "note": lambda rows: "Cột: trung vị các lượt truyền xong; chấm: từng lượt. "
        "Chunk hỏng: lật byte trong file đã nhận trước khi resume.",
    },
}


def _points(rows, field, scale, done):
    return [
        float(row[field]) * scale
        for row in rows
        if row["status"] in done and row.get(field) not in (None, "")
    ]


def draw(rows, spec, title, target):
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    groups = list(dict.fromkeys(str(row[spec["group"]]) for row in rows))
    by_group = {
        group: [row for row in rows if str(row[spec["group"]]) == group] for group in groups
    }
    panels = [
        panel
        for panel in spec["panels"]
        if _points(rows, panel[0], panel[1], spec["done"])
    ] or spec["panels"][:1]

    figure, axes = plt.subplots(
        1, len(panels), figsize=(5.4 * len(panels), 4.8), squeeze=False, facecolor=SURFACE
    )
    def measured(group, field):
        # Profile có cột này ở nhóm này (croc-base không ghi telemetry thì bỏ qua).
        return [
            name
            for name in PROFILES
            if any(
                r["profile"] == name and r.get(field) not in (None, "")
                for r in by_group[group]
            )
            or any(
                r["profile"] == name and r["status"] not in spec["done"]
                for r in by_group[group]
            )
        ]

    most = max(len(measured(g, panel[0])) for g in groups for panel in panels) or 1
    width, legend_done = min(0.5, 0.8 / most), set()
    for axis, (field, scale, label, text) in zip(axes[0], panels):
        axis.set_facecolor(SURFACE)
        top, labels = 0.0, []
        shown = [group for group in groups if measured(group, field)]
        for position, group in enumerate(shown):
            present = measured(group, field)
            for number, profile in enumerate(present):
                name, color = PROFILES[profile]
                x = position + (number - (len(present) - 1) / 2) * (width + 0.03)
                points = _points(
                    [r for r in by_group[group] if r["profile"] == profile],
                    field, scale, spec["done"],
                )  # fmt: skip
                if not points:
                    axis.text(x, 0, "không có\nlượt đạt", ha="center", va="bottom",
                              color=MUTED, fontsize=8)  # fmt: skip
                    continue
                value = statistics.median(points)
                axis.bar(
                    x, value, width, color=color,
                    label=None if profile in legend_done else name,
                )  # fmt: skip
                legend_done.add(profile)
                # Từng lượt đo, rải ngang để không đè nhau.
                spread = [x + (i - (len(points) - 1) / 2) * 0.04 for i in range(len(points))]
                axis.scatter(spread, points, s=22, color=INK, edgecolors=SURFACE, zorder=3)
                top = max([top, value, *points])
                labels.append((x, value))
        # Nhãn trung vị ở chân cột để không bị đọc nhầm là nhãn của một chấm.
        for x, value in labels:
            inside = value > top * 0.15
            axis.annotate(
                text.format(value), (x, 0 if inside else value), xytext=(0, 5),
                textcoords="offset points", ha="center", fontsize=9,
                color="white" if inside else INK,
            )  # fmt: skip
        axis.set_title(label, color=INK, fontsize=11, loc="left")
        axis.set_xticks(
            range(len(shown)),
            [spec["group_label"](group, by_group[group]) for group in shown],
        )
        axis.set_xlim(-0.6, len(shown) - 0.4)
        axis.set_ylim(0, top * 1.18 or 1)
        axis.yaxis.grid(True, color=GRID, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.tick_params(colors=MUTED, length=0)
        for side in ("top", "right", "left"):
            axis.spines[side].set_visible(False)
        axis.spines["bottom"].set_color("#c3c2b7")

    figure.suptitle(title, color=INK, fontsize=12, x=0.01, ha="left")
    handles, names = [], []
    for axis in axes[0]:
        for handle, name in zip(*axis.get_legend_handles_labels()):
            if name not in names:
                handles.append(handle)
                names.append(name)
    if len(names) > 1:
        figure.legend(handles, names, loc="upper right", frameon=False, fontsize=9)
    figure.text(0.01, 0.01, spec["note"](rows), color=MUTED, fontsize=8.5)
    figure.tight_layout(rect=(0, 0.04, 1, 0.9 if len(names) > 2 else 0.95))
    figure.savefig(target, dpi=150, facecolor=SURFACE)
    plt.close(figure)


def find_jobs(source, out):
    source = source.resolve()
    if source.is_file():
        files, root = [source], source.parent
    else:
        files, root = sorted(source.rglob("trials.csv")), source
    jobs = []
    for file in files:
        rows = bench.read_csv(file)
        suites = {row.get("suite") for row in rows} & set(SUITES)
        if len(suites) != 1:
            continue
        (suite,) = suites
        parts = file.parent.relative_to(root).parts or (file.parent.name,)
        name = "-".join(parts)
        jobs.append((suite, rows, name, out.resolve() / f"{name}.png"))
    return jobs


def main(argv=None):
    bench.setup_console()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", type=Path, required=True, help="thư mục hoặc trials.csv")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    jobs = find_jobs(args.input, args.out)
    if not jobs:
        print(f"Lỗi: không có trials.csv của {', '.join(SUITES)} trong {args.input}", file=sys.stderr)
        return 1
    existing = [target for *_, target in jobs if target.exists()]
    if existing:
        print(f"Lỗi: đã tồn tại {existing[0]}; chọn --out khác", file=sys.stderr)
        return 1
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        print("Lỗi: thiếu matplotlib (python -m pip install matplotlib)", file=sys.stderr)
        return 3
    args.out.mkdir(parents=True, exist_ok=True)
    for suite, rows, name, target in jobs:
        spec = SUITES[suite]
        draw(rows, spec, f"{spec['title']}: {name}", target)
        print(target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
