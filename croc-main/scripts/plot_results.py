#!/usr/bin/env python3
"""Vẽ biểu đồ PNG từ trials.csv của bench_adaptive_compress.py (cần matplotlib).

python plot_results.py --input THƯ_MỤC_KẾT_QUẢ --out THƯ_MỤC_BIỂU_ĐỒ
"""

import argparse
from pathlib import Path
import sys

import croc_bench as bench

SUITE = "adaptive-compress"
DATASET_LABELS = {"text": "Văn bản", "zip": "ZIP", "random": "Ngẫu nhiên", "mixed": "Hỗn hợp"}
# Hai màu đầu của bảng phân loại tham chiếu, cố định theo profile.
PROFILES = {"base": ("croc-base", "#2a78d6"), "upgraded": ("croc-upgraded", "#eb6834")}
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e1e0d9"


def draw(rows, title, target):
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    summary = {(row["profile"], row["dataset"]): row for row in bench.summarize(rows)}
    datasets = list(dict.fromkeys(row["dataset"] for row in rows))
    profiles = [name for name in PROFILES if any(row["profile"] == name for row in rows)]
    sizes = {row["dataset"]: int(row["bytes"]) / bench.MIB for row in rows}
    panels = [("mib_per_s", "median_mib_per_s", "Thông lượng (MiB/s)", "{:.1f}")]
    if any(row.get("receiver_cpu_s") for row in rows):
        panels.append(
            ("receiver_cpu_s", "median_receiver_cpu_s", "CPU tiến trình receiver (giây)", "{:.3f}")
        )

    figure, axes = plt.subplots(
        1, len(panels), figsize=(5.2 * len(panels), 4.8), squeeze=False, facecolor=SURFACE
    )
    width = 0.34
    for axis, (field, median_field, label, text) in zip(axes[0], panels):
        axis.set_facecolor(SURFACE)
        top, labels = 0.0, []
        for number, profile in enumerate(profiles):
            name, color = PROFILES[profile]
            legend_name = name
            shift = (number - (len(profiles) - 1) / 2) * (width + 0.02)
            for position, dataset in enumerate(datasets):
                entry = summary.get((profile, dataset))
                points = [
                    float(row[field])
                    for row in rows
                    if row["profile"] == profile
                    and row["dataset"] == dataset
                    and row["status"] == "ok"
                    and row.get(field)
                ]
                x = position + shift
                if not entry or entry[median_field] is None:
                    axis.text(x, 0, "không có\nlượt đạt", ha="center", va="bottom", color=MUTED)
                    continue
                value = entry[median_field]
                axis.bar(x, value, width, color=color, label=legend_name)
                legend_name = None
                # Từng lượt đo, rải ngang để không đè nhau.
                spread = [x + (i - (len(points) - 1) / 2) * 0.05 for i in range(len(points))]
                axis.scatter(spread, points, s=22, color=INK, edgecolors=SURFACE, zorder=3)
                top = max([top, value, *points])
                labels.append((x, value))
        # Nhãn trung vị nằm ở chân cột để không bị đọc nhầm là nhãn của một chấm.
        for x, value in labels:
            inside = value > top * 0.15
            axis.annotate(
                text.format(value), (x, 0 if inside else value), xytext=(0, 5),
                textcoords="offset points", ha="center", fontsize=9,
                color="white" if inside else INK,
            )  # fmt: skip
        axis.set_title(label, color=INK, fontsize=11, loc="left")
        axis.set_xticks(
            range(len(datasets)),
            [f"{DATASET_LABELS.get(d, d)}\n{sizes[d]:.0f} MiB" for d in datasets],
        )
        axis.set_xlim(-0.6, len(datasets) - 0.4)
        axis.set_ylim(0, top * 1.18 or 1)
        axis.yaxis.grid(True, color=GRID, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.tick_params(colors=MUTED, length=0)
        for side in ("top", "right", "left"):
            axis.spines[side].set_visible(False)
        axis.spines["bottom"].set_color("#c3c2b7")

    figure.suptitle(title, color=INK, fontsize=12, x=0.01, ha="left")
    if len(profiles) > 1:
        handles, labels = axes[0][0].get_legend_handles_labels()
        figure.legend(handles, labels, loc="upper right", frameon=False)
    figure.text(
        0.01, 0.01,
        "Cột: trung vị các lượt đạt; chấm: từng lượt. Relay localhost; thời gian gồm bắt tay.",
        color=MUTED, fontsize=8.5,
    )  # fmt: skip
    figure.tight_layout(rect=(0, 0.04, 1, 0.95))
    figure.savefig(target, dpi=150, facecolor=SURFACE)
    plt.close(figure)


def main(argv=None):
    bench.setup_console()
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", type=Path, required=True, help="thư mục hoặc trials.csv")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    source = args.input.resolve()
    if source.is_file():
        files, root = [source], source.parent
    else:
        files, root = sorted(source.rglob("trials.csv")), source
    jobs = []
    for file in files:
        rows = [row for row in bench.read_csv(file) if row.get("suite") == SUITE]
        if rows:
            parts = file.parent.relative_to(root).parts or (file.parent.name,)
            jobs.append((rows, "-".join(parts), args.out.resolve() / f"{'-'.join(parts)}.png"))
    if not jobs:
        print(f"Lỗi: không có dữ liệu {SUITE} trong {source}", file=sys.stderr)
        return 1
    existing = [target for _, _, target in jobs if target.exists()]
    if existing:
        print(f"Lỗi: đã tồn tại {existing[0]}; chọn --out khác", file=sys.stderr)
        return 1
    try:
        import matplotlib  # noqa: F401
    except ImportError:
        print("Lỗi: thiếu matplotlib (python -m pip install matplotlib)", file=sys.stderr)
        return 3
    args.out.mkdir(parents=True, exist_ok=True)
    for rows, name, target in jobs:
        draw(rows, f"Nén theo dữ liệu: {name}", target)
        print(target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
