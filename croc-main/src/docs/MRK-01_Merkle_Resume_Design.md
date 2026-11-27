# `MRK-01`: Thiết kế Cây Merkle 32 KiB & Giao thức Kiểm chứng Resume

> **Phụ trách:** Long (`MRK`) · **Liên kết contract:** Đức (`INT-01`), Sơn (`SCH-01`), Nam (`CMP-04`)  
> **Mã nguồn tham chiếu:** [merkle.go](../merkle/merkle.go) · [resume.go](../merkle/resume.go) · [merkle_resume.go](../croc/merkle_resume.go) · [message.go](../message/message.go)

---

## 1. Cấu trúc Cây Merkle mức Chunk

- **Kích thước lá (`DefaultChunkSize`):** `32 KiB` (`32 * 1024` byte), khớp 1-1 với wire chunk của `croc` (`models.TCP_BUFFER_SIZE / 2`).
- **Ước lượng bộ nhớ:** File `800 MB` có `25,600` lá × `32 byte` = `819,200 byte` (`~800 KiB` RAM) để lưu mảng hash lá.
- **Thuật toán băm & Domain Separation (RFC 6962 §2.1):**
  - **Nút lá:** `LeafHash(chunk) = SHA256(0x00 || chunk)`
  - **Nút cha:** `NodeHash(left, right) = SHA256(0x01 || left || right)`
  - Ngăn chặn second-preimage / leaf-vs-node collision (ví dụ không thể ghép `left || right` thành một lá 64B để giả mạo nút cha).

### Xử lý các trường hợp biên (Edge Cases)

| Trường hợp biên | Số lá (`LeafCount`) | Cách tính `Root()` và kiểm chứng |
| :--- | :---: | :--- |
| **File `0 byte`** | `0` | `Root() = EmptyRoot() = SHA256(0x00)`. Vì mọi lá của file khác rỗng đều chứa ít nhất 1 byte dữ liệu sau `0x00`, `EmptyRoot()` không bao giờ đụng độ với bất kỳ lá hợp lệ hay nút nội bộ (`0x01`) nào. |
| **File `< 32 KiB`** (1 byte .. 32,767 byte) | `1` | `Root() = LeafHash(data)` (không đệm zero). |
| **File không chia hết cho `32 KiB`** | `ceil(fileSize / 32 KiB)` | Lá cuối chỉ băm đúng `fileSize % 32 KiB` byte thực tế (không zero-padding). `VerifyChunk(idx, data)` kiểm tra cả độ dài chính xác của chunk cuối. |
| **Số nút ở một tầng bị lẻ** | `N` lẻ | Nút lẻ cuối cùng ở tầng đó được đôn nguyên trạng lên tầng cha (RFC 6962 promotion rule). |

---

## 2. Giao thức Control Plane & Giới hạn Kích thước Bản tin

PAKE Feature Flag: `merkle-chunk-resume-v1` ([message.go](../message/message.go)). Nếu peer không quảng bá flag này, client tự động fallback về `utils.MissingChunksFile` và cơ chế hash cuối file truyền thống.

### 2.1. `TypeMerkleManifest` (`"merkle-manifest"`)

- **Vấn đề kích thước:** Một bản tin control plane có giới hạn giải nén `maxDecompressedMessageSize = 64 MiB` (`message.go`) và cần giữ kích thước hợp lý khi đi qua Relay. Với file rất lớn (ví dụ 800 MB → 800 KiB hash lá, hoặc 10 GB → ~10 MB hash lá), gửi một bản tin duy nhất có thể gây nghẽn hoặc vượt giới hạn frame.
- **Giải pháp chia lô (Batching):** Chia mảng lá theo lô `merkleManifestLeavesPerBatch = 4096` lá/bản tin (`4096 × 32B = 128 KiB` nhị phân ≈ `170 KiB` base64 JSON). File `800 MB` chỉ cần `7` bản tin `TypeMerkleManifest` tuần tự trên kết nối control `conn[0]`.
- **Payload (`merkleManifestPayload`):**
  - `fi` (`FileIndex`), `fs` (`FileSize`), `cs` (`ChunkSize`), `lc` (`LeafCount`), `r` (`Root` 32B), `sl` (`StartLeaf`), `lh` (`LeafHashes` packed), `fn` (`Final`).
- **Kiểm tra an toàn phía Receiver:** Từ chối nếu `StartLeaf` không liên tiếp, tổng số lá vượt `LeafCount`, `ExpectedLeafCount(FileSize, ChunkSize) != LeafCount`, hoặc `ComputeRoot(leaves) != Root`.

### 2.2. State Machine `TypeChunkRepairRequest` / `TypeChunkRepairAck`

1. **Resume Scan (`recipientInitializeFile`):** Receiver chạy `merkle.ScanResumeFile` song song (`runtime.NumCPU()` worker dùng `ReadAt`) để tìm `missing ∪ corrupted` dưới dạng `[]int64` (`[chunkSize, startOffset0, count0, ...]`) khớp `SCH.ReenqueueRanges`.
2. **Inline Verify (`receiveData`):** Sau khi giải mã (và giải nén `CMP-04`), Receiver gọi `VerifyChunk(idx, plaintext)` trước khi ghi đĩa; nếu sai lệch, ghi nhận `chunkIdx` vào `corruptChunks`.
3. **Selective Repair Loop (tối đa `maxMerkleRepairRounds = 3` vòng):**
   - Kết thúc lượt nhận nếu `len(corruptChunks) > 0` và `repairRound < 3`: Receiver giữ file mở, reset bộ đếm lượt nhận, xuất telemetry `merkle_verify`, và gửi `TypeChunkRepairRequest` chứa `ranges` lỗi.
   - Sender nhận `TypeChunkRepairRequest` → kiểm tra biên `validateRepairRanges` → gửi `TypeChunkRepairAck` → đẩy lại đúng các range lỗi vào hàng đợi gửi (hoặc `SCH.ReenqueueRanges`).
   - Nếu sau `3` vòng vẫn còn chunk lỗi (ví dụ hỏng ổ đĩa vật lý), Receiver dừng và báo lỗi thay vì lặp vô hạn.
