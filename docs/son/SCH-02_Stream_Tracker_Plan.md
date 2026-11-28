# Kế hoạch Chi tiết `SCH-02` — Bộ đo Thông lượng từng luồng TCP (`StreamTracker`)

> **Người thực hiện:** **Sơn** (`SCH` — Transport & Scheduling Engineer)  
> **Thuộc lộ trình:** Ngày 2 · [02_Son_SCH_Dynamic_Scheduling.md](../02_Son_SCH_Dynamic_Scheduling.md)  
> **Phụ thuộc đã mở khóa:** `INT-01` ([metrics.go](../../croc-main/src/telemetry/metrics.go), [message.go](../../croc-main/src/message/message.go))  
> **Trạng thái:** Đã cài đặt & pass `go test -race` (`croc-main/src/croc/stream_metrics.go`, `stream_metrics_test.go`)

---

## 1. Mục tiêu & Tiêu chí Hoàn thành (`DoD`)

Theo đặc tả tại [02_Son_SCH_Dynamic_Scheduling.md](../02_Son_SCH_Dynamic_Scheduling.md#L88-L91):
1. Theo dõi thông lượng **EWMA (`bytes/sec`)** và độ trễ trung bình mỗi chunk (`ewmaDur`) cho từng `workerID` trong `croc-main/src/croc/stream_metrics.go`.
2. Phân loại từng luồng thành `WorkerFast`, `WorkerNormal`, `WorkerSlow` so với trung vị toàn pool (và so với đỉnh lịch sử của chính luồng đó), cài đặt trọn vẹn interface `StreamSpeedInspector` của `SCH-01`/`SCH-03`.
3. Định kỳ mỗi **`200 ms`** xuất bản ghi JSONL `stream_stat` qua `telemetry.Sink` (`telemetry.StreamStat`: `worker_id`, `local_port`, `throughput_bps`, `inflight`, `stolen_chunks`), đồng thời flush mẫu cuối khi đóng phiên.
4. **DoD kiểm chứng:** Unit test `stream_metrics_test.go` phát hiện chính xác một luồng suy giảm tốc độ `> 50%` trong vòng `< 1 giây` (cả khi luồng gửi chậm dần lẫn khi luồng bị treo giữa chừng trong `Send()`), pass `go test -race`.

---

## 2. Thiết kế Thuật toán & Cấu trúc Dữ liệu

### 2.1. Công thức EWMA & Xử lý Treo Giữa Chừng (In-Flight Stall Cap)
- **Hệ số làm mượt EWMA:** $\alpha = 0.35$.
  - Khi hoàn tất 1 chunk (`RecordSendSample(workerID, bytes, sendDur)`):
    - $\text{instBps} = \frac{\text{bytes}}{\text{sendDur.Seconds()}}$
    - Mẫu đầu tiên (`samples == 0`): khởi tạo trực tiếp `ewmaBps = instBps`, `ewmaDur = sendDur`.
    - Từ mẫu thứ 2:
      $$\text{ewmaBps}_{t} = 0.35 \cdot \text{instBps} + 0.65 \cdot \text{ewmaBps}_{t-1}$$
      $$\text{ewmaDur}_{t} = 0.35 \cdot \text{sendDur} + 0.65 \cdot \text{ewmaDur}_{t-1}$$
    - Với một luồng giảm tốc độ `> 50%` (ví dụ còn `30%–45%` tốc độ ban đầu), chỉ sau **2–4 chunks** (`< 150 ms`), `ewmaBps` đã hội tụ xuống dưới ngưỡng `50%`.
- **Phát hiện tức thời khi socket treo cứng giữa lệnh `Send()` (`MarkSendStart` / `MarkSendFinish`):**
  - Nếu một luồng đang giữ chunk `InFlight` với thời gian bay `elapsed = now - inflightStart` vượt quá `max(2 * ewmaDur, 10 ms)`, thông lượng hiệu dụng tức thời bị chặn trên bởi:
    $$\text{effectiveBps} = \min\left(\text{ewmaBps},\ \frac{\text{inflightBytes}}{\text{elapsed.Seconds()}}\right)$$
  - Nhờ đó, kể cả khi một luồng bị bóp xuống rất thấp hoặc đứt mạng chưa kịp trả về từ `dataConn.Send()`, `StreamTracker` vẫn phát hiện suy giảm `> 50%` trong `< 100 ms`.

### 2.2. Ngưỡng Phân loại `WorkerTier`
Sử dụng trung vị nửa trên (`upperMedian` — phần tử `sorted[n/2]` của mảng tốc độ tăng dần) làm chuẩn tham chiếu `refBps` của pool (giúp phát hiện chính xác ngay cả khi pool chỉ có 2 luồng hoặc 2/4 luồng cùng nghẽn):
- **`WorkerSlow`:** `effectiveBps < 0.50 * refBps` (suy giảm `> 50%` so với trung vị pool) hoặc tự suy giảm `effectiveBps < 0.50 * peakBps` sau giai đoạn warm-up.
- **`WorkerFast`:** `effectiveBps >= 0.85 * refBps`.
- **`WorkerNormal`:** `0.50 * refBps <= effectiveBps < 0.85 * refBps` (hoặc chưa có mẫu đo).

### 2.3. Chu kỳ Xuất Telemetry JSONL (`200 ms`) & Quy tắc Khóa
- `StreamTracker` nhận `telemetry.Sink` từ `INT-01`.
- Hàm `Start(ctx context.Context)` khởi chạy goroutine nền với `time.NewTicker(200 * time.Millisecond)` (chỉ chạy khi `sink != nil && sink != telemetry.Nop` để tiết kiệm tài nguyên khi người dùng không bật `--metrics-out`).
- Tại mỗi nhịp `200 ms` và khi gọi `Stop()`:
  1. Dưới `mu.RLock()`: chụp nhanh danh sách `telemetry.StreamStat` của các worker theo thứ tự `workerID` tăng dần.
  2. Mở `mu.RUnlock()`.
  3. **Bên ngoài lock**: gọi `sink.Emit(stat)` cho từng worker → không bao giờ giữ `mu` khi ghi đĩa JSONL.
  4. Không bao giờ gọi ngược vào `ChunkScheduler` → bảo toàn thứ tự khóa một chiều `ChunkScheduler -> StreamTracker`.

---

## 3. Kế hoạch Kiểm thử (`stream_metrics_test.go`)

1. **`TestStreamTrackerDetectsOver50PctDropUnder1Second` (DoD chính):**
   - Khởi tạo 4 luồng ở tốc độ `10 MB/s`, sau đó giảm tốc luồng 2 xuống `4.0 MB/s` (giảm 60% > 50%) → xác nhận `ClassifyWorker(2) == WorkerSlow` trong `< 200 ms` (`<< 1s`).
2. **`TestStreamTrackerDetectsMidSendSocketStallUnder1Second`:**
   - Luồng 1 đang ở `20 MB/s` thì bị treo cứng ngay trong `MarkSendStart` (chưa gọi `RecordSendSample`) → sau `80 ms`, `ClassifyWorker(1)` tự động chuyển sang `WorkerSlow` và `EffectiveThroughputBps` phản ánh đúng mức sụt giảm.
3. **`TestStreamTrackerEmitsPeriodicJSONLTelemetry`:**
   - Mở `telemetry.Open(tempFile)` thật từ package `src/telemetry` của Đức, đăng ký `LocalPort` (9009, 9010), chạy ticker `200 ms`, đóng tracker và đọc lại file `.jsonl` kiểm chứng đầy đủ các trường `ts`, `event=="stream_stat"`, `worker_id`, `local_port`, `throughput_bps`, `inflight`, `stolen_chunks`.
4. **`TestStreamTrackerIntegratedWithAdaptiveChunkScheduler`:**
   - Cắm trực tiếp `StreamTracker` vào `newAdaptiveChunkScheduler` (`SCH-01` + `SCH-02` + `SCH-03`), chạy 4 workers (1 worker chậm 10×) xuất file JSONL thật, kiểm chứng cả `StolenChunks > 0` trên scheduler lẫn `stolen_chunks > 0` trong log JSONL dưới `-race`.
