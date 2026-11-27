#!/usr/bin/env bash
# INT-02: build binary đối chứng bin/croc-base từ commit baseline của nhóm.
#
# 9e435c2 là bản croc-main chưa có nâng cấp nào (cây giống upstream
# schollz/croc d77d59a, "croc version 11.6.6"). Build từ git, không từ thư mục
# làm việc, nên kết quả không phụ thuộc nhánh đang checkout.
set -euo pipefail

usage() {
  cat <<'EOF'
Cách dùng: build-croc-base.sh [--force]

  --force   Ghi đè bin/croc-base đã có
Biến môi trường:
  GO=...           Lệnh go (mặc định: go)
  BASE_COMMIT=...  Commit baseline (mặc định: 9e435c2)
EOF
}

die() { printf 'Lỗi: %s\n' "$*" >&2; exit 1; }

FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --force) FORCE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "tùy chọn không hợp lệ: $1" ;;
  esac
done

GO=${GO:-go}
BASE_COMMIT=${BASE_COMMIT:-9e435c2}
command -v "$GO" >/dev/null 2>&1 || die "không thấy '$GO' (đặt GO=/đường/dẫn/go)"
command -v git >/dev/null 2>&1 || die "thiếu git"
command -v tar >/dev/null 2>&1 || die "thiếu tar"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
git -C "$REPO" cat-file -e "$BASE_COMMIT^{commit}" 2>/dev/null ||
  die "không có commit $BASE_COMMIT (chạy git fetch origin)"

EXE=$("$GO" env GOEXE)
OUT="$REPO/croc-main/bin/croc-base$EXE"
[ "$FORCE" -eq 1 ] || [ ! -e "$OUT" ] || die "đã có $OUT (thêm --force để build lại)"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "Lấy croc-main tại $BASE_COMMIT"
git -C "$REPO" archive "$BASE_COMMIT" croc-main | tar -x -C "$WORK"

# Cùng cờ build với bin/croc-upgraded trong .github/workflows/benchmark.yml.
echo "Build $OUT"
mkdir -p "$(dirname "$OUT")"
(
  cd "$WORK/croc-main"
  CGO_ENABLED=0 "$GO" build -buildvcs=false -trimpath -tags netgo,osusergo \
    -ldflags="-s -w -buildid=" -o "$WORK/croc-base$EXE" .
)
mv -f "$WORK/croc-base$EXE" "$OUT"

"$OUT" --version
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$OUT"
fi
