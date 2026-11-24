## Thông tin Pull Request

- **Thành viên thực hiện:** <!-- Nam / Sơn / Long / Đức -->
- **Mã đầu việc & Task:** <!-- Ví dụ: CMP-02, SCH-04, MRK-05, INT-01 -->
- **Nhánh nguồn -> đích:** <!-- Ví dụ: feat/cmp-adaptive -> dev -->

## Tóm tắt thay đổi

<!-- Mô tả ngắn gọn nội dung code/tài liệu đã thay đổi và ảnh hưởng tới các đầu việc khác -->

## Điểm liên kết với các thành viên khác

- [ ] Không sửa chéo file ngoài phạm vi đầu việc (hoặc đã thống nhất với **Đức / `INT`** trước khi sửa `croc.go`).
- [ ] Tuân thủ đúng Shared Contracts (PAKE feature flags, 1B `CodecHeader`, Control Plane `message.Type`, Telemetry JSONL).

## Checklist Kiểm thử Bắt buộc

- [ ] `cd croc-main && go vet ./...` pass
- [ ] `cd croc-main && go test -count=1 ./...` pass
- [ ] `cd croc-main && go test -race -count=1 ./src/compress ./src/message ./src/receivefs ./src/croc` (kèm `./src/merkle`, `./src/telemetry` nếu có) pass không có Data Race
- [ ] Đảm bảo tương thích ngược với client `croc-base` khi peer không quảng bá feature mới
- [ ] Đã cập nhật trạng thái checkbox trong file đầu việc tương ứng ở `docs/` (nếu hoàn thành task)
