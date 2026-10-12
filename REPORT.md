# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Lê Trung Kiên |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602748 |
| GitHub | keybeand |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-03365 |

## Kết quả cuối

- `dataset_version` của bạn: fedabb0286ef
- Số khung hình train / val / test của bạn: train: 692, val: 160, test: 167 (tổng 1019 khung, 26 trip: 18 train, 4 val, 4 test)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt rõ nhất giữa hai nguồn telemetry:
1. Tên cột khác nhau: A dùng `speed_mps`, `heading_deg`, `gas`, `brake`, `steer`; B dùng `spd_kmh`, `hdg`, `throttle_pct`, `brake_pct`, `steering`.
2. Đơn vị đo khác nhau: A lưu vận tốc m/s, bàn đạp từ 0 đến 1; B lưu km/h, bàn đạp theo phần trăm 0 đến 100%.
3. Định dạng thời gian: A dùng chuỗi ISO 8601 UTC (`2025-10-07T08:00:00.014Z`), B dùng số nguyên epoch mili-giây (`1759933801509`).
4. Lệch đồng hồ: A khớp mốc video, còn thiết bị B bị lệch 1500 ms so với video gốc.

Pipeline giữ riêng mỗi nguồn một bảng và để kiểu chuỗi vì tầng Bronze có nhiệm vụ lưu đúng dữ liệu thô nhận về. Việc ép kiểu hay gộp bảng ngay từ đầu dễ làm rơi rụng dữ liệu hoặc sập pipeline khi nhà cung cấp đổi schema đột ngột.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Mô hình staging của dbt chuẩn hóa tên cột về một chuẩn chung, đổi `spd_kmh / 3.6` sang m/s và chuyển epoch ms sang timestamp UTC. Đồng thời, công thức trừ đi 1500 ms đã khai báo trong `sources.json` để đồng bộ giờ thiết bị B. Bằng chứng là mốc thời gian bản ghi đầu của B sau xử lý đạt `2025-10-08 14:30:00.009000+00:00`, khớp mốc mở video trong ngưỡng sai số 0.5 giây.

Bảng quarantine:
- drive_a: duplicate = 20, speed_missing = 30, speed_out_of_range = 3
- drive_b: duplicate = 20, speed_missing = 30, speed_out_of_range = 3

Dòng lỗi được đẩy vào bảng rejects để kỹ sư truy vết nguyên nhân cảm biến hỏng hóc hoặc tính toán tỷ lệ hao hụt dữ liệu khi kiểm toán chất lượng.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` chỉ đặt cảnh báo `warn` vì việc xe đi vào hầm hay khuất sóng làm mất tín hiệu cảm biến là thực tế vận hành, không làm vỡ cấu trúc dữ liệu. Test `in_range` (0 đến 70 m/s) phải là `error` vì vận tốc phi lý sẽ làm hỏng nhãn huấn luyện của mô hình xe tự lái.

Ở thí nghiệm `break-gate`, khi nâng trần vận tốc lên 1000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` đã phát hiện 6 dòng vi phạm và trả về `ERROR=1`. Cổng chất lượng đóng vai trò chốt chặn tự động, ngăn dữ liệu hỏng lọt xuống các tầng dưới.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Số liệu từ `idempotency.json`:
- Trước khi chạy lại: 1246 dòng, mã băm 6c7480dfde7e.
- Sau hai lần chạy lại partition `trip-00003`: vẫn đúng 1246 dòng, mã băm giữ nguyên 6c7480dfde7e.
- Nếu ghi dạng `append`: số dòng của trip bị nhân đôi từ 60 lên 120 dòng sau hai lượt.

Cơ chế overwrite theo partition của Delta Lake đảm bảo tính nguyên tử qua commit log. Nếu tiến trình trích xuất bị ngắt quãng giữa chừng, giao dịch bị hủy và partition cũ vẫn nguyên vẹn. Xóa thủ công rồi ghi mới dễ để lại thư mục trống nếu gặp sự cố.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung đen là lỗi mất tín hiệu hoàn toàn từ camera nên bị loại bỏ (drive A mất 6 khung, drive B mất 4 khung). Khung tối xảy ra khi xe đi qua bóng râm hoặc ban đêm, hình ảnh vẫn có chi tiết môi trường nên pipeline gắn cờ cảnh báo nhưng vẫn giữ lại (drive A giữ 4 khung, drive B giữ 61 khung). Cách xử lý này hợp lý vì mô hình cần học cách nhận diện vật thể trong điều kiện thiếu sáng.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng 2 vì loại bỏ tốt các khung đứng hình (như đoạn camera đơ 6 giây ở drive A) mà không làm mất các khung cảnh xe đang di chuyển chậm. Khi xe chạy nhanh, cảnh vật thay đổi liên tục giữa các khung nên có thể nới ngưỡng; khi xe dừng chờ đèn đỏ, ngưỡng thấp giúp tránh giữ lại các khung hình trùng lặp vô ích.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Số liệu từ `leakage.json`:
- Chia theo khung hình: toàn bộ 26/26 trip bị xé lẻ giữa các tập, và cả 26 trip đều xuất hiện đồng thời ở cả train lẫn test.
- Chia theo trip: 0 trip nào bị chia cắt giữa các tập.

Các khung hình liên tiếp cách nhau chỉ 0.5 giây có độ tương đồng hình ảnh rất cao. Nếu chia ngẫu nhiên theo khung, mô hình học khung trước ở tập train và gặp lại khung liền kề ở tập test. Điều này dẫn đến hiện tượng rò rỉ dữ liệu, điểm đánh giá trên tập test cao nhưng mô hình mất khả năng xử lý khi gặp cung đường mới ngoài đời thực.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Phiên bản dataset được tính bằng băm SHA-256 từ danh sách nối các cặp `frame_id|split`. Cách này giúp việc định danh hoàn toàn phụ thuộc vào nội dung thực tế, hai lần chạy trên cùng dữ liệu luôn cho ra đúng một mã băm `fedabb0286ef`.

Mã phiên bản của tôi khác các bạn khác vì MSSV được dùng làm salt để xáo trộn thứ tự gán trip vào các tập train, val, test. Mỗi sinh viên có một danh sách phân chia khung hình khác nhau, tạo ra mã băm phiên bản riêng biệt.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Từ phần `lineage` trong `manifest.json`, dataset bắt nguồn từ:
- Hai video gốc có mã băm sha256: drive_a (`636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`) và drive_b (`bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`).
- Bảng thô `bronze.video_files` phiên bản 1, các bảng `bronze.raw_telemetry_a`, `raw_telemetry_b`, `trips_raw` phiên bản 0.
- Các bảng xử lý `silver.frames` (phiên bản 131) cùng `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` đều ở phiên bản 4.

Sau 3 tháng, người muốn tái tạo lại dataset chỉ cần mã nguồn Git tại commit tương ứng, hai file video gốc đúng sha256 và dùng tính năng time travel của Delta Lake để truy xuất đúng số phiên bản bảng đã ghi trong lineage.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Khi chạy với biến `LAB_FAIL_ONCE=trip-03365`, đúng 1 task con `silver_frames [] / 8` bị lỗi đỏ với thông báo `RuntimeError: simulated crash while extracting trip-03365`. Tất cả 7 task phía sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) chuyển sang màu cam trạng thái `upstream_failed`.

Tôi thực hiện clear duy nhất task con số 8 bị đỏ. Airflow chạy lại task này thành công và các bước tiếp theo tự động chạy tiếp đến khi toàn bộ DAG xanh. Dữ liệu không bị nhân đôi nhờ cơ chế overwrite partition, `dataset_version` vẫn giữ nguyên giá trị `fedabb0286ef`. Minh chứng nằm ở ảnh `airflow_task_failed.png` và `airflow_graph.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba thành phần sẽ quá tải đầu tiên khi mở rộng lên 500 xe mỗi ngày:
1. Giải mã video bằng FFmpeg tuần tự trên container đơn lẻ: việc giải nén hàng nghìn giờ video sẽ nghẽn CPU. Cần tách việc trích xuất khung hình ra cụm xử lý phân tán như Ray hoặc Celery worker với GPU tăng tốc (Slide 24).
2. DuckDB chạy in-process trên một máy: dung lượng hàng chục terabyte vượt quá bộ nhớ và ổ cứng cục bộ. Cần chuyển sang công cụ xử lý phân tán như Spark, Trino hoặc hồ dữ liệu đám mây (Slide 22).
3. Airflow LocalExecutor dùng chung một máy chủ: không đủ tài nguyên điều phối hàng chục nghìn task song song. Cần chuyển sang CeleryExecutor hoặc KubernetesExecutor để tự động co giãn tài nguyên theo tải (Slide 26).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    trip_id,
    drive_id,
    frames AS tong_khung,
    (frames - frames_kept) AS khung_bi_loai,
    ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 1) AS ty_le_loai_pct,
    lighting AS anh_sang,
    conditions AS thoi_tiet
FROM marts.mart_trip_summary
ORDER BY ty_le_loai_pct DESC
LIMIT 5;
```

Truy vấn này tìm ra 5 chuyến xe có tỷ lệ khung hình bị loại bỏ cao nhất trong pipeline. Kết quả cho thấy `trip-04622` dẫn đầu với 56.7% khung bị loại (34/60 khung), theo sau là `trip-03904` với 50.0%. Nguyên nhân phần lớn do xe dừng đỗ hoặc di chuyển quá chậm dẫn đến camera tạo nhiều khung hình trùng lặp bị bộ lọc dHash loại bớt.

## Nhìn lại

Điều bất ngờ nhất là tính tiện lợi của cơ chế event time join và Delta time travel, giúp giải quyết triệt để vấn đề lệch đồng hồ và chạy lại an toàn mà không cần viết mã xử lý bù trừ phức tạp. Một điểm pipeline hiện tại chưa bắt được là trường hợp kính chắn gió bị bẩn cục bộ hoặc giọt nước mưa che khuất một góc camera nhưng chưa đủ mờ để kích hoạt ngưỡng blur score của toàn khung hình.

