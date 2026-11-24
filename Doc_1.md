**TÀI LIỆU HƯỚNG DẪN THỰC HÀNH LAB**  
**PHÂN TÍCH KIẾN TRÚC MẠNG PHÂN TẦNG VÀ**  
**GIAO THỨC TRUYỀN DỮ LIỆU VỚI CROC**

| Môn học: Lập trình mạng (Network Programming) | Thời lượng thực hành: 04 tiết (1 buổi Lab) |
| :---- | :---- |
| **Mã học phần:** INT | **Hình thức thực hiện:** Nhóm 02 sinh viên (Sender \- Receiver) |
| **Đối tượng:** SV Ngành CNTT, KTPM, Mạng máy tính | **Môi trường Lab:** Linux / macOS / Windows 10-11 |

Nhóm :

Thành viên: 

**I. MỤC TIÊU BÀI THỰC HÀNH (LAB OBJECTIVES)**

Bài thực hành được thiết kế theo phương pháp đảo ngược kỹ thuật (Reverse Engineering & Top-Down Networking Analysis), nhằm giúp sinh viên chuyển hóa các kiến thức lý thuyết trừu tượng trong mô hình TCP/IP thành trực quan thực tế thông qua việc phân tích hành vi của công cụ truyền dữ liệu mã hóa đa luồng croc.

**1\. Về Kiến thức (Knowledge Outcomes)**

* **Phân định ranh giới chức năng các tầng mạng:** Hiểu rõ sự phân tách giữa Tầng Ứng dụng (Application), Tầng Giao vận (Transport) và Tầng Mạng (Network/Internet) trong một kiến trúc P2P hiện đại.  
* **Tách bạch Control Plane và Data Plane:** Phân biệt luồng thông tin báo hiệu (Signaling \- đăng ký, tìm peer, bắt tay mật mã) và luồng truyền tải dữ liệu dung lượng lớn (Data Stream).  
* **Cơ chế bắt tay mật mã an toàn:** Hiểu nguyên lý trao đổi khóa xác thực bằng mật mã yếu (PAKE / SPAKE2) qua máy chủ Relay mà không để lộ bí mật nội dung cho máy chủ trung gian.  
* **Kỹ thuật đục lỗ NAT (NAT Traversal) và Chuyển tiếp (Relay):** Nắm vững cách các peer vượt qua thiết bị tường lửa (Firewall) và router NAT để truyền P2P hoặc giáng cấp qua Relay server.

**2\. Về Kỹ năng thực hành (Practical Skills)**

* **Giám sát trạng thái Socket tầng giao vận:** Sử dụng thành thạo các công cụ hệ điều hành (ss, netstat, lsof) để theo dõi các kết nối TCP (ESTABLISHED, TIME\_WAIT) và socket UDP.  
* **Phân tích bắt gói tin nâng cao với Wireshark:** Bắt, lọc, bóc tách header TCP/UDP, trích xuất luồng dữ liệu (TCP Stream) và đo lường băng thông thực tế.  
* **Thực nghiệm kiểm thử tính chịu lỗi (Fault-Tolerance):** Giả lập sự cố rớt mạng, ngắt tiến trình đột ngột và đánh giá cơ chế truyền tiếp (Resuming) tầng ứng dụng.

**II. TỔNG QUAN KIẾN TRÚC CROC VÀ MÔ HÌNH PHÂN TẦNG**

croc là một công cụ mã nguồn mở viết bằng ngôn ngữ Go, cho phép truyền file và thư mục an toàn, tốc độ cao giữa hai máy tính bất kỳ. Khác với các mô hình Client-Server truyền thống (như FTP hay HTTP Upload/Download), croc kết hợp giữa kiến trúc Client-Server (dùng cho pha Signaling qua Relay) và kiến trúc P2P (dùng cho pha Data Transfer).

| Tầng Mạng (Layer) | Cơ chế kỹ thuật trong croc | Hành vi quan sát trong bài Lab |
| ----- | ----- | ----- |
| **Tầng Ứng dụng(Application)** | • PAKE (SPAKE2): Dẫn xuất khóa phiên từ passphrase ngắn.• Mã hóa AES-256-GCM.• Chunking: Cắt file thành các khối cố định (1 MB) & nén.• Quản lý manifest để Resume. | • Chuỗi ký tự code phrase sinh ra bí mật phiên.• Server trung gian (Relay) không đọc được nội dung payload.• Trạng thái offset khi truyền tiếp. |
| **Tầng Giao vận(Transport)** | • UDP Broadcast/Multicast (Port 9009\) để phát hiện láng giềng.• TCP Multi-streaming (4 \- 8 kết nối TCP song song) để tối ưu băng thông đường truyền. | • Socket UDP lắng nghe 0.0.0.0:9009.• Bảng socket hiển thị đồng thời nhiều kết nối ESTABLISHED giữa 2 IP cùng lúc. |
| **Tầng Mạng & Liên kết(Network & Link)** | • Phân giải địa chỉ IP Public/Private qua STUN.• Đục lỗ NAT (Hole punching).• Chuyển tiếp lưu lượng (Relay Fallback) khi gặp Symmetric NAT. | • So sánh IP đích trong gói tin Wireshark: Khi nào là IP LAN nội bộ (192.168.x.x), khi nào là IP Public của Relay ngoài Internet. |

**III. CHUẨN BỊ MÔI TRƯỜNG & YÊU CẦU KỸ THUẬT**

**1\. Cấu hình phần mềm và công cụ**

Sinh viên phải cài đặt đầy đủ các phần mềm sau trên cả 2 máy tính trước khi vào buổi thực hành:

| Phần mềm / Tiện ích | Mục đích sử dụng | Lệnh cài đặt mẫu |
| ----- | ----- | ----- |
| **croc (v9.x trở lên)** | Ứng dụng truyền file đối tượng nghiên cứu | Ubuntu/Debian: curl https\://getcroc.schollz.com \| bashmacOS: brew install crocWindows: choco install croc hoặc scoop install croc |
| **Wireshark / tshark** | Bắt gói tin và phân tích luồng TCP/UDP | Ubuntu: sudo apt install wireshark tsharkmacOS: brew install \--cask wiresharkWindows: Tải bộ cài chính thức wireshark.org |
| **Công cụ Socket(ss, netstat, lsof)** | Quan sát trạng thái kết nối và bảng cổng mở | Linux/macOS: Tích hợp sẵn trong iproute2, net-toolsWindows: netstat tích hợp sẵn trong Command Prompt |

**2\. Sinh dữ liệu kiểm thử (Test Data Generation)**

Để bảo đảm việc truyền file kéo dài đủ thời gian (khoảng 30 đến 60 giây) cho các thao tác bắt gói tin và ngắt mạng, sinh viên tạo một file nhị phân ngẫu nhiên dung lượng 800 MB bằng lệnh tương ứng trên hệ điều hành:

| \# Trên Linux:dd if=/dev/urandom of=testfile\_800MB.bin bs=1M count=800 status=progress\# Trên macOS:mkfile \-n 800m testfile\_800MB.bin\# Trên Windows (PowerShell với quyền Admin):fsutil file createnew testfile\_800MB.bin 838860800 |
| :---- |

| 📌 QUY ĐỊNH VỀ DỮ LIỆU THỰC NGHIỆMKhông sử dụng các file nén sẵn có (.zip, .mp4, .iso) chứa metadata nhạy cảm của cá nhân. File ngẫu nhiên tạo từ /dev/urandom có entropy cao, giúp quan sát chính xác hiệu năng nén (compression ratio) của croc ở tầng ứng dụng. |
| :---- |

**IV. NỘI DUNG VÀ CÁC BƯỚC THỰC HÀNH CHI TIẾT**

**BÀI 1: Khảo sát Chu trình Bắt tay (Handshake), Báo hiệu (Signaling) & SPAKE2**

Mục tiêu: Bóc tách tiến trình thiết lập phiên, phân tích địa chỉ máy chủ báo hiệu (Signaling Relay), và hiểu cơ chế bảo mật PAKE (Password-Authenticated Key Exchange).

**Các bước thực hiện:**

Bước 1.1: Trên Máy A (Sender), mở Terminal và chạy lệnh gửi file ở chế độ gỡ lỗi chi tiết (--debug):

| croc \--debug send testfile\_800MB.bin |
| :---- |

Hệ thống sẽ in ra một chuỗi passphrase (ví dụ: '7892-alpha-charlie-bravo'). Hãy ghi lại chuỗi này.

Bước 1.2: Trên Máy B (Receiver), mở Terminal và chạy lệnh nhận tương ứng:

| croc \--debug \<passphrase\_nhan\_duoc\> |
| :---- |

Bước 1.3: Quan sát toàn bộ các thông báo dạng log xuất hiện trên cả 2 màn hình từ thời điểm nhấn Enter cho tới khi tiến trình truyền dữ liệu bắt đầu.

**Nhiệm vụ và Câu hỏi Đánh giá Bài 1 (2.0 Điểm):**

**C1.1 (0.5 điểm):** Trích xuất log hiển thị trên màn hình và chỉ rõ: Tên miền / Địa chỉ IP và Port của máy chủ Relay mặc định mà bên gửi và bên nhận kết nối đến ở giây đầu tiên? Kênh kết nối báo hiệu này sử dụng giao thức nào (TCP thuần, HTTP hay WebSocket/TLS)?

**C1.2 (0.75 điểm):** Phân tích tiến trình log và chia thành 4 giai đoạn logic rõ ràng: (1) Kết nối máy chủ báo hiệu; (2) Trao đổi khóa mật mã (PAKE); (3) Thăm dò cổng kết nối & địa chỉ IP (Discovery); (4) Khởi tạo kênh dữ liệu. Hãy ghi chú dòng log tương ứng cho từng giai đoạn.

**C1.3 (0.75 điểm):** Giải thích cơ chế bảo mật: Chuỗi passphrase (mật khẩu ngắn) được dùng để làm gì? Máy chủ Relay trung gian có thể giải mã được file truyền qua nó hay không? Hãy viện dẫn nguyên lý hoạt động của thuật toán SPAKE2 để bảo vệ dữ liệu.

**BÀI 2: Giám sát Socket Tầng Giao vận (Transport Layer Monitoring)**

Mục tiêu: Khảo sát trực quan kiến trúc kết nối đa luồng (Multi-streaming TCP), phát hiện socket lắng nghe UDP, và phân tích lý do kiến trúc.

**Các bước thực hiện:**

Bước 2.1: Bắt đầu lại tiến trình truyền file 800 MB giữa Máy A và Máy B.

Bước 2.2: Trong lúc dữ liệu đang truyền và tốc độ đã ổn định, mở một cửa sổ Terminal thứ 2 trên Máy A (Sender) hoặc Máy B (Receiver) và thực hiện lệnh kiểm tra socket hệ thống:

| \# Trên Linux:ss \-tupn \| grep croc\# Trên macOS:lsof \-i \-P \| grep croc\# Trên Windows (PowerShell):netstat \-ano \| findstr \-i croc |
| :---- |

Bước 2.3: Chụp ảnh màn hình toàn bộ đầu ra lệnh trên, lưu lại để đính kèm vào báo cáo.

**Nhiệm vụ và Câu hỏi Đánh giá Bài 2 (2.5 Điểm):**

**C2.1 (0.75 điểm):** Từ kết quả lệnh giám sát, đếm và liệt kê chính xác có bao nhiêu socket TCP đang ở trạng thái ESTABLISHED? Ghi rõ cặp địa chỉ (IP nguồn : Port nguồn \<---\> IP đích : Port đích) của từng socket này.

**C2.2 (1.0 điểm):** Phân tích kiến trúc giao vận: Tại sao croc lại mở đồng thời nhiều kết nối TCP song song (Multi-streaming / Parallel Sockets) để truyền 1 file thay vì chỉ dùng 1 kết nối TCP duy nhất? Phân tích ưu nhược điểm dưới góc độ: (a) Băng thông / Tắc nghẽn mạng; (b) Hiện tượng nghẽn đầu hàng (Head-of-Line Blocking) của TCP; (c) Tận dụng tích số băng thông \- trễ (Bandwidth-Delay Product \- BDP).

**C2.3 (0.75 điểm):** Xác định socket UDP đang mở trong bảng kết quả: Cổng (port) lắng nghe là bao nhiêu? Socket UDP này được sử dụng vào mục đích gì? Tại sao tác giả không sử dụng giao thức TCP cho tác vụ này?

**BÀI 3: Phân tích Lưu lượng Gói tin với Wireshark (Direct LAN vs. Relay WAN)**

Mục tiêu: Sử dụng Wireshark để phân tích đường đi của gói tin, phân biệt giữa chế độ truyền trực tiếp ngang hàng (Direct P2P) và chuyển tiếp qua Relay (Relay Fallback), đồng thời thẩm tra tính bảo mật payload trên đường truyền.

**Các bước thực hiện:**

Bước 3.1: Khởi động Wireshark, chọn card mạng đang kết nối (Wi-Fi hoặc Ethernet). Thiết lập bộ lọc hiển thị (Display Filter):

| tcp.port \== 9009 \|\| tcp.port \== 9010 \|\| tcp.port \== 9011 \|\| tcp.port \== 9012 \|\| udp.port \== 9009 |
| :---- |

Bước 3.2 (Kịch bản A \- Truyền nội mạng trực tiếp): Bắt đầu bắt gói tin trên Wireshark. Chạy lệnh ép buộc truyền qua LAN:

| croc \--local \--debug send testfile\_800MB.bin |
| :---- |

Sau khi truyền xong, dừng bắt gói tin và lưu lại thành file: lab\_croc\_lan.pcapng.

Bước 3.3 (Kịch bản B \- Truyền qua máy chủ Relay công cộng): Bắt đầu phiên bắt gói tin mới. Chạy lệnh cấm kết nối nội bộ để ép dùng Relay:

| croc \--no-local \--debug send testfile\_800MB.bin |
| :---- |

Sau khi hoàn tất, dừng bắt gói tin và lưu lại thành file: lab\_croc\_relay.pcapng.

**Nhiệm vụ và Câu hỏi Đánh giá Bài 3 (3.0 Điểm):**

**C3.1 (1.0 điểm):** So sánh địa chỉ IP nguồn (Source IP) và IP đích (Destination IP) của các gói tin mang dữ liệu lớn (TCP Len \~ 1460 bytes) trong 2 file pcap:  
\- Trong Kịch bản A: Gói tin truyền trực tiếp giữa 2 địa chỉ IP riêng (Private IP 192.168.x.x) hay có qua Internet?  
\- Trong Kịch bản B: Gói tin dữ liệu được gửi đến IP nào? Tra cứu xem địa chỉ IP này thuộc về tổ chức / quốc gia nào?

**C3.2 (1.0 điểm):** Thẩm tra tính bảo mật của Payload:  
\- Mở file lab\_croc\_relay.pcapng trong Wireshark. Chuột phải vào một gói tin TCP mang dữ liệu \-\> Chọn Follow \-\> TCP Stream.  
\- Quan sát dữ liệu hiển thị dạng ASCII / Hex. Có đọc được tên file, định dạng dữ liệu gốc hay bất kỳ thông tin nhạy cảm nào không? Chụp ảnh màn hình minh chứng và giải thích tại sao Relay không thể nhìn trộm dữ liệu này.

**C3.3 (1.0 điểm):** Vẽ lại Sơ đồ Tuần tự (Sequence Diagram) mô tả dòng trao đổi gói tin giữa 3 thực thể: Sender, Receiver, và Relay Server cho cả 2 kịch bản (LAN vs WAN Relay).

**BÀI 4: Khảo sát Cơ chế Chịu lỗi, Phục hồi (Resume) & Toàn vẹn Dữ liệu**

Mục tiêu: Đánh giá cơ chế xử lý sự cố mạng đứt quãng, phân biệt sự khác nhau căn bản giữa tính năng phát lại của TCP (TCP Retransmission) và tính năng phục hồi truyền dở dang (Application Chunk Resuming).

**Các bước thực hiện:**

Bước 4.1: Khởi tạo truyền file 800 MB. Khi thanh tiến trình đạt khoảng 50%, thực hiện ngắt kết nối đột ngột bằng cách nhấn Ctrl \+ C trên terminal máy nhận (hoặc ngắt Wi-Fi / rút cáp mạng của máy nhận trong 10 giây).

Bước 4.2: Kết nối lại mạng (nếu rút cáp), mở lại Terminal trên máy nhận và chạy lại đúng lệnh nhận với passphrase ban đầu:

| croc \--debug \<passphrase\_cu\> |
| :---- |

Bước 4.3: Quan sát các thông báo in ra và thanh tiến trình phần trăm khi quá trình truyền tiếp tục.

**Nhiệm vụ và Câu hỏi Đánh giá Bài 4 (2.5 Điểm):**

**C4.1 (0.75 điểm):** Chương trình có phải truyền lại từ byte 0 không? Trích xuất chính xác dòng log cho thấy croc nhận biết được file đã tải dở dang và bắt đầu gửi từ offset/chunk nào.

**C4.2 (1.0 điểm):** Phân tích so sánh bản chất kỹ thuật:  
\- Cơ chế truyền lại của TCP (TCP Retransmission dựa trên Sequence/ACK numbers) hoạt động trong tình huống nào?  
\- Khi tiến trình croc bị tắt (kill/Ctrl+C), bảng trạng thái TCP Socket trong OS Kernel còn tồn tại không? Tại sao TCP không thể tự thực hiện việc truyền tiếp trong trường hợp này mà phải do Tầng Ứng dụng tự quản lý?

**C4.3 (0.75 điểm):** Cơ chế kiểm tra toàn vẹn: Làm thế nào bên nhận (Receiver) biết chắc chắn rằng file sau khi nối các chunk lại hoàn toàn chính xác, không bị sai sót dù chỉ 1 bit? Trích dẫn thuật toán băm (Hashing) mà croc sử dụng.

**V. TIÊU CHÍ ĐÁNH GIÁ VÀ BAREM ĐIỂM CHI TIẾT**

Điểm tổng kết bài Lab được tính theo thang điểm 10.0, phân bổ theo mức độ hoàn thành nhiệm vụ thực nghiệm và độ sâu sắc của báo cáo phân tích lý thuyết mạng:

| Hạng mục | Nội dung đánh giá | Minh chứng bắt buộc | Điểm |
| ----- | ----- | ----- | ----- |
| **Bài 1:Bắt tay & Báo hiệu** | Xác định đúng IP Relay, phân tách 4 giai đoạn logic trong log, giải thích đúng cơ chế PAKE/SPAKE2. | Ảnh chụp màn hình log có cờ \--debug, trích dẫn dòng log. | 2.0 |
| **Bài 2:Giám sát Socket** | Đếm đúng số socket TCP ESTABLISHED, giải thích thấu đáo ưu điểm Multi-streaming vs Single TCP, giải thích đúng vai trò UDP. | Ảnh chụp màn hình lệnh ss / netstat / lsof trong lúc đang truyền file. | 2.5 |
| **Bài 3:Wireshark Packet Analysis** | So sánh chuẩn xác IP LAN vs WAN Relay, kiểm tra payload mã hóa (Follow TCP Stream), vẽ sơ đồ Sequence Diagram chuẩn xác. | 02 file pcapng đính kèm, ảnh chụp màn hình Follow TCP Stream, sơ đồ luồng. | 3.0 |
| **Bài 4:Chịu lỗi & Resume** | Thực hiện thành công kịch bản resume, phân biệt tường minh TCP Retransmission vs Application Chunk Resuming, cơ chế băm kiểm tra toàn vẹn. | Ảnh chụp log thể hiện resumed from chunk/offset, mã hash kiểm tra. | 2.5 |
| **TỔNG CỘNG** | **Đánh giá toàn diện trên báo cáo PDF và các tệp dữ liệu pcapng.** | **Đầy đủ thành phần nộp bài theo quy cách.** | **10.0** |

**Quy định trừ điểm (Penalty Rules):**

* **Nộp muộn:** Trừ 1.0 điểm/ngày trễ hạn; bài nộp muộn quá 3 ngày nhận 0 điểm.  
* **Thiếu file bắt gói tin (.pcapng):** Trừ 2.0 điểm nếu thiếu một hoặc cả hai file pcap.  
* **Thiếu ảnh chụp màn hình minh chứng:** Trừ 0.5 điểm cho mỗi câu hỏi không có bằng chứng thực nghiệm cụ thể.  
* **Gian lận / Trùng lặp mã nguồn & log:** Nếu phát hiện sao chép file pcap hoặc sao chép log từ nhóm khác mà không tự thực hiện: Nhận điểm 0 toàn bộ nhóm.

**VI. QUY CÁCH NỘP BÀI VÀ YÊU CẦU BÁO CÁO**

Sinh viên hoàn thiện báo cáo và đóng gói toàn bộ sản phẩm thực hành theo đúng cấu trúc sau:

| LabNetwork\_Nhom\<SoNhom\>\_\<HoTenSV1\>\_\<HoTenSV2\>.zip│├── BaoCao\_ThuHoach.pdf       \<- Báo cáo trả lời toàn bộ câu hỏi từ C1.1 đến C4.3 kèm ảnh minh chứng└── Captures/    ├── lab\_croc\_lan.pcapng   \<- File Wireshark bắt gói kịch bản LAN trực tiếp    └── lab\_croc\_relay.pcapng \<- File Wireshark bắt gói kịch bản qua Relay công cộng |
| :---- |

| 📌 YÊU CẦU ĐỊNH DẠNG NỘPFile báo cáo thu hoạch BẮT BUỘC phải xuất sang định dạng PDF trước khi nộp, không nộp file Word thô. Ảnh chụp màn hình trong báo cáo phải rõ nét, thấy rõ dấu nhắc lệnh Prompt (có username/hostname) để chứng minh tính độc lập của nhóm. |
| :---- |

**VII. PHỤ LỤC: BẢNG TRA CỨU CỜ LỆNH VÀ BỘ LỌC WIRESHARK**

**Phụ lục A: Bảng tra cứu các cờ lệnh quan trọng của croc**

| Tham số / Cờ lệnh (Flag) | Ý nghĩa & Tác động đến tầng mạng |
| ----- | ----- |
| **\--debug** | Bật chế độ in nhật ký chi tiết (debug log) ở mọi tầng giao thức. |
| **\--local** | Bắt buộc chỉ quét và kết nối qua mạng cục bộ LAN, cấm kết nối ra Relay ngoài Internet. |
| **\--no-local** | Vô hiệu hóa tính năng tìm kiếm láng giềng LAN, ép buộc truyền qua Relay công cộng. |
| **\--relay \<ip:port\>** | Chỉ định địa chỉ IP hoặc Domain máy chủ Relay tùy biến thay vì máy chủ mặc định. |
| **\--ports \<p1,p2...\>** | Chỉ định dải cổng TCP cụ thể phục vụ truyền dữ liệu đa luồng. |
| **\--curve \<ten\_curve\>** | Lựa chọn đường cong elliptic trong thuật toán SPAKE2 (vd: p256, p384, p521). |
| **\--hash \<ten\_hash\>** | Thuật toán băm kiểm tra toàn vẹn file sau khi ghép (xxhash, sha256, md5). |

**Phụ lục B: Một số bộ lọc hiển thị Wireshark (Display Filters) hữu ích**

* **tcp.port in {9009..9013}**: Lọc toàn bộ các gói tin TCP thuộc các cổng truyền dữ liệu mặc định của croc.  
* **udp.port \== 9009**: Lọc các gói tin UDP broadcast/multicast dùng cho phát hiện peer nội mạng.  
* **tcp.flags.syn \== 1 && tcp.flags.ack \== 0**: Lọc các gói bắt tay TCP SYN đầu tiên để phát hiện thời điểm khởi tạo từng luồng socket.  
* **ip.addr \== \<IP\_CUA\_RELAY\>**: Lọc toàn bộ lưu lượng trao đổi qua máy chủ Relay để đo lường băng thông WAN.