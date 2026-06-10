# Hệ Thống Triển Khai Kafka Cluster Với Ansible

Tài liệu này hướng dẫn chi tiết cách triển khai, cấu hình, quản lý sao lưu và phục hồi cho cụm **Apache Kafka Cluster (KRaft Mode)** tích hợp với **Kafka UI** và **Keycloak** để xác thực người dùng (OIDC) và phân quyền (RBAC) sử dụng **Ansible**.

---

## Kiến Trúc Hệ Thống

Hệ thống được thiết kế chạy trên môi trường AWS EC2 hoặc máy ảo Linux (Ubuntu) với mô hình phân tán gồm các cụm máy chủ sau:

1. **Kafka Cluster (3 Nodes)**:
   - `kafka-01` (Broker & Controller) - IP: `172.31.37.252`
   - `kafka-02` (Broker & Controller) - IP: `172.31.42.127`
   - `kafka-03` (Broker & Controller) - IP: `172.31.33.230`
   - Chế độ hoạt động: **KRaft** (Kafka Raft Metadata Mode) - Không sử dụng ZooKeeper.
   - Phiên bản: Kafka `4.1.1` (Scala `2.13`), chạy trên **Java 17**.

2. **Management & Auth Stack (2 Nodes)**:
   - `kafka-ui-01` - IP: `172.31.34.4`
   - `kafka-ui-02` - IP: `172.31.19.172`
   - Các dịch vụ chạy dạng container qua Docker Compose:
     - **Kafka UI**: Giao diện trực quan quản lý Kafka cluster, cấu hình RBAC kết nối Keycloak.
     - **Keycloak**: Hệ thống quản lý danh tính (Identity Provider) phục vụ xác thực người dùng qua giao thức OIDC (OpenID Connect).
     - **PostgreSQL 16**: Cơ sở dữ liệu lưu trữ cấu hình cho Keycloak.

3. **Cân bằng tải & Định tuyến**:
   - Tích hợp với AWS Application Load Balancer (ALB) tại địa chỉ: `kafka-alb-505898566.ap-southeast-1.elb.amazonaws.com`.

---

## Cấu Trúc Thư Mục Dự Án

```text
Kafka/
├── ansible.cfg              # Cấu hình Ansible chung
├── inventory/               # Thư mục chứa cấu hình máy chủ & biến số
│   ├── hosts.ini            # Định nghĩa các nhóm máy chủ (kafka, kafka_ui)
│   └── group_vars/          # Biến cấu hình theo nhóm máy chủ
│       ├── all.yml          # Biến dùng chung (phiên bản Kafka, đường dẫn lưu trữ...)
│       ├── kafka.yml        # Biến cấu hình riêng cho Kafka cluster
│       └── kafka_ui.yml     # Biến cấu hình Kafka UI, Keycloak và các secret mã hóa
├── playbooks/               # Các kịch bản chạy chính của Ansible
│   ├── install.yml          # Kịch bản cài đặt toàn bộ hệ thống
│   ├── backup.yml           # Kịch bản thiết lập sao lưu tự động
│   └── restore.yml          # Kịch bản phục hồi dữ liệu từ bản backup
└── roles/                   # Các module thực thi nhiệm vụ chi tiết
    ├── common/              # Cấu hình OS cơ bản (hostname, hosts, timezone, sysctl, file limits)
    ├── java/                # Cài đặt OpenJDK 17 và các package bổ trợ (curl, wget, vim...)
    ├── installation/        # Cài đặt Kafka, tạo User/Group, khởi tạo KRaft UUID và Systemd Service
    ├── verify/              # Kiểm tra trạng thái Cluster & ISR (In-Sync Replicas) sau cài đặt
    ├── kafka_ui/            # Cài đặt Docker, Docker Compose, cấu hình Kafka UI & Keycloak (import realm)
    ├── backup/              # Cấu hình script backup (/usr/local/bin/kafka-backup.sh) và Cronjob
    └── restore/             # Dừng dịch vụ, phục hồi dữ liệu metadata KRaft và khởi động lại Kafka
```

---

## Các Tham Số Cấu Hình Chính

### 1. Cấu hình chung (`inventory/group_vars/all.yml`)
- `kafka_version`: `"4.1.1"`
- `scala_version`: `"2.13"`
- `kafka_base_dir`: `/opt/kafka` (Thư mục cài đặt)
- `kafka_data_dir`: `/data/kafka` (Thư mục chứa dữ liệu Kafka)
- `timezone`: `"Asia/Ho_Chi_Minh"`

### 2. Cấu hình Kafka (`inventory/group_vars/kafka.yml`)
- `kafka_heap_opts`: `"-Xms512m -Xmx512m"` (Tối ưu cho tài nguyên máy chủ nhỏ như `t2.micro`).
- `kafka_num_partitions`: `3` (Số lượng partition mặc định).
- `kafka_replication_factor`: `3` (Độ nhân bản dữ liệu).
- `kafka_min_insync_replicas`: `2` (Số replica tối thiểu ghi nhận thành công).
- `kafka_controller_quorum_voters`: Thiết lập quorum cử tri KRaft (`1@kafka-01:9093,2@kafka-02:9093,3@kafka-03:9093`).

### 3. Cấu hình Giao diện & Xác thực (`inventory/group_vars/kafka_ui.yml`)
- Cấu hình phiên bản: `kafka_ui_version: "v1.3.0"`, `keycloak_version: "26.6.1"`.
- Các mật khẩu quản trị và mã bí mật (`keycloak_admin_password`, `keycloak_db_password`, `keycloak_client_secret`) được mã hóa an toàn bằng **Ansible Vault** (`!vault | ...`).
- Đường dẫn tích hợp ALB: `kafka-alb-505898566.ap-southeast-1.elb.amazonaws.com`.

---

## Hướng Dẫn Sử Dụng

### Điều kiện tiên quyết (Prerequisites)
1. Máy điều khiển (Control Node) đã cài đặt **Ansible**.
2. Đã tạo file chứa mật khẩu giải mã Ansible Vault tại `/root/.vault_pass` (được khai báo trong `ansible.cfg`).
3. Private key SSH `kafka-lab-key.pem` được đặt đúng vị trí để truy cập các server.

### 1. Cài đặt Toàn bộ Hệ thống
Chạy lệnh sau để tiến hành nâng cấp OS, cài đặt Java 17, thiết lập Kafka KRaft cluster, cài đặt Docker và khởi chạy Kafka UI + Keycloak:
```bash
ansible-playbook playbooks/install.yml
```
*(Lưu ý: Mật khẩu Vault sẽ tự động được đọc từ `/root/.vault_pass` nhờ thiết lập trong `ansible.cfg`)*

### 2. Thiết lập Sao lưu Tự động (Backup)
Kịch bản này sẽ tạo thư mục lưu trữ sao lưu tại `/opt/kafka/backups`, phân quyền cho user `kafka`, đẩy script `/usr/local/bin/kafka-backup.sh` lên các máy chủ Kafka và cấu hình Cronjob chạy hàng ngày vào lúc **02:00 AM**.
```bash
ansible-playbook playbooks/backup.yml
```
**Nội dung bản sao lưu bao gồm:**
- Danh sách và thông tin chi tiết các Topic (`kafka-topics-backup.txt`).
- File cấu hình broker (`server.properties`).
- Nén thư mục metadata KRaft (`kraft-metadata.tar.gz`).
*Mặc định hệ thống chỉ lưu giữ các bản sao lưu trong vòng **7 ngày gần nhất**.*

### 3. Phục hồi dữ liệu (Restore)
Khi cần khôi phục lại metadata và cấu hình Kafka về trạng thái trước đó, bạn có thể chạy kịch bản phục hồi. Mặc định kịch bản sẽ quét và khôi phục từ **bản sao lưu mới nhất**.
```bash
ansible-playbook playbooks/restore.yml
```
Nếu bạn muốn phục hồi một phiên bản sao lưu cụ thể, hãy truyền biến `backup_version`:
```bash
ansible-playbook playbooks/restore.yml -e "backup_version=2026-06-10_02-00"
```
**Quy trình khôi phục tự động:**
1. Kiểm tra sự tồn tại của bản backup.
2. Tạm dừng dịch vụ `kafka`.
3. Ghi đè file `server.properties` từ backup.
4. Giải nén đè dữ liệu KRaft (`kraft-metadata.tar.gz`).
5. Phân quyền lại thư mục cho user `kafka`.
6. Khởi động lại dịch vụ `kafka` và kiểm tra cổng kết nối.

---

## Cơ Chế Phân Quyền (RBAC) Trên Kafka UI
Hệ thống sử dụng Keycloak làm Identity Provider để thực hiện xác thực Single Sign-On (SSO) cho Kafka UI qua OAuth2.
Quy trình thiết lập tự động import file cấu hình Realm (`prod-realm.json`) vào Keycloak, định nghĩa sẵn hai nhóm quyền chính (đọc từ thuộc tính `groups` của user Keycloak):

1. **Role: admins (Nhóm Admin)**
   - Subject map: User thuộc nhóm `Admin` trong Keycloak.
   - Quyền hạn: Toàn quyền cấu hình cluster, quản lý topic, consumer group, schema registry, connect và KSQL.

2. **Role: readonly (Nhóm Viewer)**
   - Subject map: User thuộc nhóm `Viewer` trong Keycloak.
   - Quyền hạn: Chỉ có quyền xem cấu hình cluster, xem danh sách topic, đọc tin nhắn (`MESSAGES_READ`), phân tích dữ liệu, xem consumer group. Không có quyền sửa đổi hay xóa.

---

## Lưu Ý Quan Trọng & Sửa Lỗi

### Giới Hạn Tài Nguyên (Heap Size)

Cấu hình JVM heap trong `inventory/group_vars/kafka.yml` đang được để ở mức thấp (`-Xms512m -Xmx512m`) để chạy trên các instance lab nhỏ (như `t2.micro` với 1GB RAM). 

Khi triển khai trên môi trường Production thực tế, hãy nâng cấu hình này lên (thông thường là `4GB` đến `6GB` hoặc hơn tùy thuộc dung lượng RAM của máy chủ).
