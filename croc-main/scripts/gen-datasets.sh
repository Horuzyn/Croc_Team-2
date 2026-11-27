#!/usr/bin/env bash
# Sinh bộ dữ liệu kiểm thử INT-02 và SHA256SUMS.txt. Không ghi đè file đã có.
set -euo pipefail
export LC_ALL=C

usage() {
  cat <<'EOF'
Cách dùng: gen-datasets.sh [tùy chọn]

  --out-dir DIR        Thư mục đầu ra (mặc định: croc-main/tmp/datasets,
                       hoặc croc-main/tmp/datasets-small khi --small)
  --small              Bộ nhỏ để thử script (8 MiB + 3 x 2 MiB)
  --reuse-800 FILE     Chép file 800 MiB đã có thay vì sinh mới
  --confirm-random     Bắt buộc đi kèm --reuse-800: xác nhận file là dữ liệu ngẫu nhiên
  -h, --help           In trợ giúp
EOF
}

die() { printf 'Lỗi: %s\n' "$*" >&2; exit 1; }

MIB=1048576
SMALL=0
OUT_DIR=""
REUSE=""
CONFIRM=0

while [ $# -gt 0 ]; do
  case "$1" in
    --out-dir) [ $# -ge 2 ] || die "--out-dir thiếu giá trị"; OUT_DIR=$2; shift 2 ;;
    --small) SMALL=1; shift ;;
    --reuse-800) [ $# -ge 2 ] || die "--reuse-800 thiếu giá trị"; REUSE=$2; shift 2 ;;
    --confirm-random) CONFIRM=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "tùy chọn không hợp lệ: $1" ;;
  esac
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [ "$SMALL" -eq 1 ]; then
  [ -z "$REUSE" ] || die "--reuse-800 không dùng cùng --small"
  : "${OUT_DIR:=$SCRIPT_DIR/../tmp/datasets-small}"
  BIN_MIB=8; SET_MIB=2; REGION_KIB=256
  BIN_NAME=testfile_small.bin
  TEXT_NAME=dataset_text_small.log
  ZIP_NAME=dataset_compressed_small.zip
  MIX_NAME=dataset_mixed_small.bin
else
  : "${OUT_DIR:=$SCRIPT_DIR/../tmp/datasets}"
  BIN_MIB=800; SET_MIB=200; REGION_KIB=4096
  BIN_NAME=testfile_800MB.bin
  TEXT_NAME=dataset_text_200MB.log
  ZIP_NAME=dataset_compressed_200MB.zip
  MIX_NAME=dataset_mixed_200MB.bin
fi

for tool in awk dd head sha256sum df wc; do
  command -v "$tool" >/dev/null 2>&1 || die "thiếu công cụ: $tool"
done

# Trên Windows, "python3" có thể chỉ là stub của Microsoft Store nên phải chạy thử.
ZIP_TOOL=""
if command -v zip >/dev/null 2>&1; then
  ZIP_TOOL=zip
else
  for py in python3 python; do
    if "$py" -c 'import zipfile' >/dev/null 2>&1; then ZIP_TOOL=$py; break; fi
  done
fi
[ -n "$ZIP_TOOL" ] || die "cần 'zip' hoặc Python 3 (Ubuntu/WSL: sudo apt install zip)"

if [ -n "$REUSE" ]; then
  [ "$CONFIRM" -eq 1 ] || die "--reuse-800 yêu cầu thêm --confirm-random"
  [ -f "$REUSE" ] || die "không tìm thấy file: $REUSE"
  size=$(wc -c < "$REUSE" | tr -d ' ')
  [ "$size" -eq $((800 * MIB)) ] || die "$REUSE có $size byte, cần đúng $((800 * MIB))"
  # Bắt lỗi file toàn số 0 (ví dụ tạo bằng fsutil); không thay cho xác nhận của người dùng.
  command -v gzip >/dev/null 2>&1 || die "thiếu công cụ: gzip"
  for skip in 0 400 799; do
    packed=$(dd if="$REUSE" bs=$MIB skip=$skip count=1 2>/dev/null | gzip -1 | wc -c)
    [ "$packed" -gt $((MIB * 99 / 100)) ] || die "$REUSE nén được tại MiB $skip, không phải dữ liệu ngẫu nhiên"
  done
fi

mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)

for name in "$BIN_NAME" "$TEXT_NAME" "$ZIP_NAME" "$MIX_NAME" SHA256SUMS.txt; do
  [ ! -e "$OUT_DIR/$name" ] || die "đã tồn tại $OUT_DIR/$name (không ghi đè; xóa thủ công hoặc chọn --out-dir khác)"
done

if command -v git >/dev/null 2>&1 && git -C "$OUT_DIR" rev-parse --is-inside-work-tree 2>/dev/null | grep -q true; then
  for name in "$BIN_NAME" "$TEXT_NAME" "$ZIP_NAME" "$MIX_NAME" SHA256SUMS.txt; do
    git -C "$OUT_DIR" check-ignore -q "$name" ||
      die "$OUT_DIR/$name nằm trong repo nhưng không được .gitignore loại trừ"
  done
fi

# Bộ dữ liệu + payload tạm của zip + 512 MiB dự phòng.
need_kib=$(( (BIN_MIB + 4 * SET_MIB + 512) * 1024 ))
free_kib=$(df -Pk "$OUT_DIR" | awk 'NR==2 {print $4}')
[ "$free_kib" -ge "$need_kib" ] ||
  die "không đủ đĩa: cần $((need_kib / 1024)) MiB, còn $((free_kib / 1024)) MiB"

WORK=$(mktemp -d "$OUT_DIR/.gen.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# gen_text BYTES SEED: in đúng BYTES byte log giả lập ra stdout.
gen_text() {
  awk -v target="$1" -v seed="$2" 'BEGIN {
    srand(seed)
    split("INFO INFO INFO DEBUG WARN ERROR", level, " ")
    split("relay sender receiver discovery pake transfer", comp, " ")
    split("chunk sent;chunk acknowledged;connection established;retrying after timeout;manifest verified;buffer flushed", msg, ";")
    n = 0
    while (n < target) {
      line = sprintf("2026-%02d-%02dT%02d:%02d:%02d.%03dZ %s [%s] session=%08x peer=10.%d.%d.%d port=%d chunk=%d bytes=%d msg=\"%s\"\n",
        1 + int(rand() * 12), 1 + int(rand() * 28), int(rand() * 24), int(rand() * 60), int(rand() * 60), int(rand() * 1000),
        level[1 + int(rand() * 6)], comp[1 + int(rand() * 6)], int(rand() * 2147483647),
        int(rand() * 256), int(rand() * 256), int(rand() * 256), 9009 + int(rand() * 5),
        int(rand() * 100000), int(rand() * 65536), msg[1 + int(rand() * 6)])
      if (n + length(line) > target) line = substr(line, 1, target - n)
      printf "%s", line
      n += length(line)
    }
  }'
}

# gen_random BYTES: BYTES phải là bội của 64 KiB.
gen_random() {
  dd if=/dev/urandom bs=65536 count=$(($1 / 65536)) iflag=fullblock 2>/dev/null
}

echo "Đầu ra: $OUT_DIR"

if [ -n "$REUSE" ]; then
  echo "[1/4] Chép $REUSE -> $BIN_NAME"
  cp -- "$REUSE" "$WORK/$BIN_NAME"
else
  echo "[1/4] $BIN_NAME: $BIN_MIB MiB từ /dev/urandom"
  gen_random $((BIN_MIB * MIB)) > "$WORK/$BIN_NAME"
fi

echo "[2/4] $TEXT_NAME: $SET_MIB MiB văn bản"
gen_text $((SET_MIB * MIB)) 1 > "$WORK/$TEXT_NAME"

# Payload ngẫu nhiên lưu kiểu "store": nén lại gần như không giảm, đại diện cho dữ liệu đã nén.
echo "[3/4] $ZIP_NAME: zip chứa $SET_MIB MiB dữ liệu không nén được ($ZIP_TOOL)"
gen_random $((SET_MIB * MIB)) > "$WORK/payload.bin"
if [ "$ZIP_TOOL" = zip ]; then
  (cd "$WORK" && zip -q -0 -X "$ZIP_NAME" payload.bin)
else
  (cd "$WORK" && "$ZIP_TOOL" -c 'import sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_STORED) as z:
    z.write("payload.bin")' "$ZIP_NAME")
fi
rm -f "$WORK/payload.bin"

echo "[4/4] $MIX_NAME: $SET_MIB MiB, xen kẽ vùng văn bản/ngẫu nhiên $REGION_KIB KiB"
region=$((REGION_KIB * 1024))
regions=$((SET_MIB * MIB / region))
for ((i = 0; i < regions; i++)); do
  if ((i % 2 == 0)); then
    gen_text "$region" $((1000 + i))
  else
    gen_random "$region"
  fi
done > "$WORK/$MIX_NAME"

check_size() {
  local got
  got=$(wc -c < "$WORK/$1" | tr -d ' ')
  [ "$got" -eq "$2" ] || die "$1 có $got byte, cần $2"
}
check_size "$BIN_NAME" $((BIN_MIB * MIB))
check_size "$TEXT_NAME" $((SET_MIB * MIB))
check_size "$MIX_NAME" $((SET_MIB * MIB))

(cd "$WORK" && sha256sum "$BIN_NAME" "$TEXT_NAME" "$ZIP_NAME" "$MIX_NAME" > SHA256SUMS.txt)

for name in "$BIN_NAME" "$TEXT_NAME" "$ZIP_NAME" "$MIX_NAME" SHA256SUMS.txt; do
  mv -n -- "$WORK/$name" "$OUT_DIR/$name"
  [ ! -e "$WORK/$name" ] || die "$OUT_DIR/$name xuất hiện trong lúc chạy, không ghi đè"
done

echo
for name in "$BIN_NAME" "$TEXT_NAME" "$ZIP_NAME" "$MIX_NAME"; do
  printf '%12d byte  %s\n' "$(wc -c < "$OUT_DIR/$name")" "$name"
done
echo
cat "$OUT_DIR/SHA256SUMS.txt"
