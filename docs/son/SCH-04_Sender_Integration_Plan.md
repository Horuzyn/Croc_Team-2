# Kế hoạch Chi tiết `SCH-04` — Tích hợp Scheduler vào `sendData` & Mở API `ReenqueueRanges`

> **Người thực hiện:** **Sơn** (`SCH` — Transport & Scheduling Engineer)  
> **Thuộc lộ trình:** Ngày 3 · [02_Son_SCH_Dynamic_Scheduling.md](../02_Son_SCH_Dynamic_Scheduling.md)  
> **Kế thừa từ:** [SCH-01_Dynamic_Scheduler_Plan.md](./SCH-01_Dynamic_Scheduler_Plan.md) · [SCH-02_Stream_Tracker_Plan.md](./SCH-02_Stream_Tracker_Plan.md) · [SCH-03_WorkStealing_Queue_Plan.md](./SCH-03_WorkStealing_Queue_Plan.md)  
> **Trạng thái:** Đã cài đặt & kiểm thử E2E `-race` ([chunk_queue.go](../../croc-main/src/croc/chunk_queue.go), [croc.go](../../croc-main/src/croc/croc.go), [tailcat_negotiation.go](../../croc-main/src/croc/tailcat_negotiation.go), [upgrade_options.go](../../croc-main/src/croc/upgrade_options.go), `dynamic_sched_test.go`)

---

## 1. Đánh giá Điều kiện Thực hiện `SCH-04`

`SCH-04` **đã đủ 100% điều kiện hoàn thành ngay** mà không cần chờ thành viên khác:
1. Toàn bộ cờ CLI (`--sched`, `--metrics-out`, `--debug-throttle-worker`), `Client.metrics` và `message.FeatureDynamicChunkSchedV1` từ **Đức (`INT-01`)** đã có sẵn.
2. Điểm nối cho **Nam (`CMP-03`/`CMP-04`)**: Đo chính xác `netSendDuration` quanh `dataConn.Send()` trong `sendData` và cung cấp hook `SetNetSendObserver` / `LastNetSendDuration()` trên `*Client`, đồng thời giữ nguyên khối nén hiện tại để khi ghép `CMP-04` không xảy ra xung đột logic.
3. Điểm nối cho **Long (`MRK-05`)**: Mở phương thức an toàn đồng thời `(c *Client) ReenqueueRanges(ranges []int64) int` và hàm `unmarkReceivedChunk(offset int64)` phía Receiver.

---

## 2. Thiết kế Tích hợp An toàn & Tương thích Ngược

### 2.1. Thương lượng PAKE Feature (`dynamic-chunk-sched-v1`)
- Trong `pakeFeatures()` ([tailcat_negotiation.go](../../croc-main/src/croc/tailcat_negotiation.go)): quảng bá `message.FeatureDynamicChunkSchedV1` khi `SchedMode != "static"`.
- Trong `processMessagePake` ([croc.go](../../croc-main/src/croc/croc.go)): ghi nhận `c.peerDynamicChunkSched`.
- **Quy tắc tương thích ngược:** Sender chỉ bật chế độ Work-Stealing (`SchedModeDynamic`) khi `c.Options.SchedMode != "static" && c.peerDynamicChunkSched`. Nếu peer là bản `croc-base` cũ hoặc người dùng truyền `--sched static`, Sender tự động chạy `SchedModeStatic` (không gửi lặp chunk sang peer cũ), trong khi `StreamTracker` vẫn ghi `stream_stat` JSONL cho cả 2 chế độ để phục vụ Benchmark A/B.

### 2.2. Quản lý Vòng đời File & Worker trong `chunk_queue.go`
- Giữ nguyên cấu trúc `requestedChunkQueue` cho luồng `bundle` (`progress != nil`) để bảo toàn 100% tương thích với [bundle.go](../../croc-main/src/croc/bundle.go).
- Với phiên truyền file thường (`progress == nil`):
  - Khởi tạo `StreamTracker(c.metrics)` + `newChunkSchedulerByMode(...)` cùng `unitCtx, unitCancel`.
  - Khi tất cả chunk hoàn tất (`onDone`), hủy `unitCancel()` để đánh thức tức thì các luồng chậm đang chờ trong `Acquire` hoặc `--debug-throttle-worker`, dừng `tracker.Stop()` để flush `stream_stat`, và chỉ thực hiện `file.Close()` khi `activeWorkers == 0` (tránh lỗi `os.ErrClosed` khi worker bị steal vừa bước vào `fread.ReadAt`).
  - Gắn `c.joinSenderUnit()` vào `TypeCloseSender` và `defer` trong `Send()` **trước** `defer c.closeMetrics()` để đảm bảo bản ghi `stream_stat` cuối cùng luôn được ghi xuống file JSONL trước khi đóng file.

### 2.3. Chống Đếm trùng ở Receiver & Hỗ trợ `--debug-throttle-worker`
- Tại `receiveData`: dùng `c.receivedChunks` (`pagedChunkBitset`) dưới `c.receiveMutex` để khi nhận bản sao chunk từ Work-Stealing thì chỉ cộng `c.TotalSent`, `c.TotalChunksTransferred` và `c.addProgress` đúng 1 lần đầu tiên.
- Tại `sendData`: nếu `c.debugWorkerLimiter != nil && workerID == c.debugThrottleWorkerID`, áp dụng giới hạn tốc độ riêng cho luồng `workerID` bên trong khoảng đo `netSendDuration` để cả bài test Go lẫn `bench_dynamic_sched.py` của Đức quan sát đúng tốc độ suy giảm của luồng bị bóp.

---

## 3. Kế hoạch Kiểm thử `SCH-04` (`sch_integration_test.go`)

1. **Truyền file lớn đa luồng (100+ MB qua 4–8 kết nối TCP):** Kiểm chứng toàn vẹn dữ liệu 100% (xxhash / SHA-256) cho cả `--sched dynamic` và `--sched static`, xác nhận `stream_stat` JSONL được xuất đầy đủ.
2. **Kiểm thử bóp 1 luồng (`--debug-throttle-worker 0:512k`):** Truyền qua local relay đa kết nối, xác nhận Work-Stealing kích hoạt (`stolen_chunks > 0`), không đếm trùng tiến độ ở Receiver, file đích khớp bit-for-bit.
3. **Kiểm thử tương thích ngược (`New ↔ Legacy`):** Giả lập peer không quảng bá `dynamic-chunk-sched-v1` → Sender tự động lùi về `static` an toàn.
4. **Kiểm thử API `Client.ReenqueueRanges` & `SetNetSendObserver`:** Xác nhận API mở cho Nam (`CMP`) và Long (`MRK`) hoạt động chính xác dưới `-race`.
