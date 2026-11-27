# CMP-01: Thiết kế Entropy Sampler & Multi-level `flate` Pool

> **Tác giả:** Nam (`CMP`) · **Trạng thái:** Implemented (`feat/cmp-adaptive`)  
> **Phạm vi:** `croc-main/src/compress/entropy.go`, `level_pool.go`

---

## 1. Động lực Thiết kế

Trong `croc-main` gốc, quyết định nén được thực hiện **1 lần cho cả file** (`shouldCompressFile`) và chỉ dùng duy nhất `flate.HuffmanOnly` (`-2`). Điều này có hai hạn chế:
1. File hỗn hợp (ví dụ archive/tar chứa cả text và ảnh/video đã nén) bị nén toàn bộ hoặc bỏ nén toàn bộ.
2. Không thể nâng mức nén lên `BestSpeed` / `DefaultCompression` khi băng thông mạng là nút thắt cổ chai.

`CMP-01` xây dựng hai thành phần nền tảng trong `src/compress`:
- `EntropyEstimate(data []byte) float64`: Ước lượng nhanh Shannon entropy cho từng chunk 32 KiB.
- `LevelPool`: Quản lý 3 `sync.Pool` tách biệt cho `flate.HuffmanOnly`, `flate.BestSpeed`, `flate.DefaultCompression`.

---

## 2. Entropy Sampler (`entropy.go`)

### Thuật toán & Tối ưu
- **Công thức:** Shannon entropy trên phân bố byte:
  $$H = -\sum_{i=0}^{255} p_i \log_2(p_i) \in [0, 8]\ \text{bits/byte}$$
- **Stride Sampling (`stride = 4`):** Trên chunk 32 KiB (`models.TCP_BUFFER_SIZE / 2`), hàm lấy mẫu mỗi byte thứ 4 (8.192 mẫu) thay vì quét toàn bộ 32.768 byte.
- **Zero Heap Allocation:** Bảng tần suất `[256]uint32` nằm hoàn toàn trên stack, không cấp phát heap (`0 allocs/op`), an toàn đồng thời (stateless).

### Ngưỡng sử dụng cho `CMP-02`
| Khoảng Entropy (bits/byte) | Loại dữ liệu điển hình | Hướng xử lý ở `CMP-02` |
| :--- | :--- | :--- |
| `< 6.0` | Text thuần, JSON, log, mã nguồn | Nén trực tiếp (`CodecFlate`) |
| `6.0 – 7.5` | Dữ liệu hỗn hợp | Thử nén; gửi `CodecRaw` nếu tỉ lệ nén `>= 0.96` |
| `> 7.5` | `.zip`, `.mp4`, `/dev/urandom` | Bỏ qua nén ngay (`CodecRaw 0x00`) |

---

## 3. Multi-level `flate` Pool (`level_pool.go`)

`LevelPool` giữ mảng `[3]sync.Pool` tương ứng:
- Index `0`: `flate.HuffmanOnly` (`-2`) — nhanh nhất, dùng khi CPU là nút thắt.
- Index `1`: `flate.BestSpeed` (`1`) — cân bằng CPU và tỉ lệ nén.
- Index `2`: `flate.DefaultCompression` (`-1`, tương đương level `6`) — tỉ lệ nén cao nhất khi mạng chậm.

Mỗi lần gọi `LevelPool.CompressTo(dst, src, level)` tái sử dụng backing array của `dst` (`dst[:0]`), thực hiện `Get -> Reset -> Write -> Close -> Reset(io.Discard) -> Put` tương tự `huffmanWriterPool` gốc. Phía Receiver giữ nguyên `DecompressTo` và `readerPool` vì `flate.Reader` tự giải mã mọi mức nén DEFLATE.

---

## 4. Kiểm thử & Xác minh

- **Unit tests:** `entropy_test.go` (pure text, JSON/log, random, all-zeros, single-byte, empty, compressed stream) và `level_pool_test.go` (round-trip 3 levels, buffer reuse, concurrent access).
- **Benchmarks:** `BenchmarkEntropyEstimate` và `BenchmarkLevelPoolCompress` (`-benchmem`).
- **Lệnh chạy chuẩn:**
  ```bash
  cd croc-main
  go test -v -race ./src/compress
  go test -bench="Benchmark(EntropyEstimate|LevelPoolCompress|TransferChunkCompression)" -benchmem ./src/compress
  ```
