# Kế hoạch Chi tiết `SCH-05` — Xử lý Idempotent Receive & Kiểm thử Bóp Băng thông 1 Luồng (`512 KB/s`)

> **Người thực hiện:** **Sơn** (`SCH` — Transport & Scheduling Engineer)  
> **Thuộc lộ trình:** Ngày 4 · [02_Son_SCH_Dynamic_Scheduling.md](../02_Son_SCH_Dynamic_Scheduling.md)  
> **Kế thừa từ:** [SCH-01_Dynamic_Scheduler_Plan.md](./SCH-01_Dynamic_Scheduler_Plan.md) · [SCH-02_Stream_Tracker_Plan.md](./SCH-02_Stream_Tracker_Plan.md) · [SCH-03_WorkStealing_Queue_Plan.md](./SCH-03_WorkStealing_Queue_Plan.md) · [SCH-04_Sender_Integration_Plan.md](./SCH-04_Sender_Integration_Plan.md)  
> **Trạng thái:** Đã thiết kế & kiểm thử TDD `-race` ([croc.go](../../croc-main/src/croc/croc.go), [chunk_queue.go](../../croc-main/src/croc/chunk_queue.go), [upgrade_options.go](../../croc-main/src/croc/upgrade_options.go), [dynamic_sched_test.go](../../croc-main/src/croc/dynamic_sched_test.go))

---

## 1. Mục tiêu & Bài toán Concurrency ở Phía Receiver

Khi cơ chế **Work-Stealing** (`dynamic-chunk-sched-v1`) hoạt động, một chunk đang kẹt trên kết nối TCP chậm có thể được luồng nhanh cướp và gửi song song mà không đóng kết nối cũ. Điều này dẫn đến 4 tình huống tranh chấp phía Receiver cần xử lý triệt để:

1. **Cộng đúp tiến độ (`TotalSent` / `TotalChunksTransferred` / thanh progress):** Hai bản sao của cùng `offset` đến trên 2 kết nối khác nhau không được làm Receiver tưởng đã nhận đủ số chunk khi vẫn còn chunk khác chưa tới.
2. **Đua lệnh `WriteAt` với `receiveFile.Close()` ở cuối file (`os.ErrClosed`):** Nếu bản sao thứ hai vừa vượt qua bước kiểm tra mở file thì bản sao thứ nhất hoàn tất file và gọi `receiveFile.Close()`, lệnh `WriteAt` muộn sẽ báo lỗi đóng file.
3. **Bản sao trễ xuyên qua ranh giới nhiều file (Multi-file stale packet bleed):** Trong phiên truyền nhiều file liên tiếp trên cùng tập kết nối TCP, gói tin trùng bị trễ trên luồng nghẽn của File `k` không được rơi sang lúc Receiver đã mở File `k+1`.
4. **Phối hợp sửa lỗi chọn lọc với `MRK-04`/`MRK-05` (Long):** Khi Receiver yêu cầu gửi lại dải chunk hỏng (`repairRanges`), chỉ các offset trong `repairRanges` được gỡ đánh dấu để nhận lại, còn các chunk hợp lệ từ lượt trước vẫn giữ bit chống trùng.

---

## 2. Thiết kế Kỹ thuật

### 2.1. Idempotent Receive Hai Pha (`inflightReceiveChunks` + `receivedChunks`)
- Trước khi gọi `receiveFile.WriteAt(payload, positionInt64)`:
  - Dưới `c.receiveMutex`, kiểm tra `c.inflightReceiveChunks.Set(positionInt64)`.
  - Nếu trả về `false` (chunk này đang được một goroutine khác ghi hoặc đã ghi xong): bỏ qua ngay lập tức **không gọi `WriteAt`**, loại bỏ hoàn toàn ghi đĩa thừa lẫn nguy cơ đua `WriteAt` sau `receiveFile.Close()`.
  - Nếu `WriteAt` gặp lỗi I/O thực sự: gỡ bit `c.inflightReceiveChunks.Clear(positionInt64)` trước khi báo lỗi.
- Sau khi `WriteAt` thành công:
  - Dưới `c.receiveMutex`, đánh dấu `firstArrival = c.receivedChunks.Set(positionInt64)` và chỉ tăng `c.TotalSent`, `c.TotalChunksTransferred`, `c.addProgress(...)` khi `firstArrival == true`.
  - Điều kiện kết thúc lượt nhận:
    `finished := firstArrival && (c.TotalChunksTransferred == c.CurrentFileChunkCount || (len(c.CurrentFileChunkRanges) == 0 && c.TotalSent == fileSize))`
    đảm bảo gói trùng không bao giờ kích hoạt đóng file sớm trong chế độ Resume hoặc Merkle Selective Repair.

### 2.2. Hàng rào Xả Gói Trễ Giữa Nhiều File (Zero Chunk-Wire-Format Change)
- Giữ nguyên 100% khung dữ liệu `[8B offset][payload]` trên kênh data để tương thích tuyệt đối với `CMP-04` (Nam) và `MRK-04` (Long).
- Sender đếm tổng số gói data đã phát trong lượt hiện tại (`sentPackets`) và đính kèm số đếm này trong `TypeCloseRecipient.Bytes` khi `peerDynamicChunkSched` bật.
- Nếu phiên truyền **chỉ có 1 file** (hoặc đang ở file cuối cùng): Receiver kết thúc ngay khi đủ chunk duy nhất, **không chờ** gói trùng đang kẹt trên luồng bị bóp (triệt tiêu 100% tail latency).
- Nếu phiên truyền **còn file kế tiếp** (`FilesToTransferCurrentNum + 1 < len(FilesToTransfer)`): tại `TypeCloseRecipient`, Receiver chờ `recvDataPackets >= sentPackets` (có ràng buộc context) trước khi mở file tiếp theo để xả sạch mọi bản sao trễ của file cũ.

### 2.3. API Đồng bộ Bitmap cho `MRK-04`/`MRK-05`
- Cung cấp `unmarkReceivedChunk(offset int64)` và `unmarkReceivedChunkRanges(ranges []int64, fileSize int64)` trong [upgrade_options.go](../../croc-main/src/croc/upgrade_options.go) để gỡ đồng thời cả `inflightReceiveChunks` lẫn `receivedChunks` cho đúng các chunk cần truyền lại.

### 2.4. Thống kê Tỷ trọng Chunk Từng Worker (`LastSenderWorkerChunks`)
- Ghi nhận số chunk hoàn tất đầu tiên (`first == true`) theo từng `workerID` trong `requestedChunkQueue` và lưu snapshot tại `Client.LastSenderWorkerChunks()` để kiểm chứng định lượng tiêu chí DoD `> 85%`.

---

## 3. Kế hoạch Kiểm thử TDD (`dynamic_sched_test.go`)

1. **`TestSCH05_SingleStreamThrottled512KBpsFastStreamsCarryOver85Percent`:**
   - Truyền file `16 MiB + 4,099 B` qua 4 luồng TCP với `--debug-throttle-worker 0:512k`.
   - Kiểm chứng các luồng nhanh (`1..3`) gánh **`> 85%`** tổng lượng chunk, `stolen_chunks > 0`, thời gian truyền `--sched dynamic` nhanh hơn rõ rệt `--sched static` (loại bỏ chờ luồng chậm cuối file), và SHA-256 khớp tuyệt đối.
2. **`TestSCH05_IdempotentReceiveNoDoubleCountUnderDuplicateDelivery`:**
   - Bơm trực tiếp các gói chunk trùng lặp song song vào kênh nhận trong phiên truyền thực tế; xác nhận `TotalSent == fileSize`, `TotalChunksTransferred == expectedChunks` (không cộng đúp) và không phát sinh lỗi `os.ErrClosed`.
3. **`TestSCH05_UnmarkReceivedChunkRangesForSelectiveRepair`:**
   - Kiểm chứng gỡ đánh dấu theo dải `ranges` chỉ mở lại đúng các offset cần sửa lỗi trong khi vẫn chặn bản sao trễ của các chunk lành.
4. **`TestSCH05_MultiFileDrainPreventsCrossFileStaleDuplicates`:**
   - Truyền liên tiếp 3 file khác nhau khi 1 luồng bị bóp tốc độ mạnh, kiểm chứng không có gói trễ nào của file trước ghi đè sang file sau.
