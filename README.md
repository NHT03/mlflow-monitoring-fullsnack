# 🎯 Production ML Model Monitoring & Orchestration System

Hệ thống giám sát (monitoring), phát hiện trôi dữ liệu (data drift), tự động hóa pipeline (orchestration) và cảnh báo thời gian thực (real-time alerts) cho mô hình Machine Learning trong môi trường Production.

---

## 📋 Mục lục (Table of Contents)

1. [Tổng quan hệ thống (System Overview)](#-tổng-quan-hệ-thống)
2. [Kiến trúc hệ thống (Architecture)](#-kiến-trúc-hệ-thống)
3. [Danh sách dịch vụ & Cổng truy cập (Services & Ports)](#-danh-sách-dịch-vụ--cổng-truy-cập)
4. [Yêu cầu tiên quyết (Prerequisites)](#-yêu-cầu-tiên-quyết)
5. [Hướng dẫn cài đặt & chạy từ A-Z (Step-by-Step Guide)](#-hướng-dẫn-cài-đặt--chạy-từ-a-z)
   - [Bước 1: Clone Repository & Cấu hình `.env`](#bước-1-clone-repository--cấu-hình-env)
   - [Bước 2: Khởi động Hạ tầng nền tảng (Storage, DB, MLflow, Prometheus, Grafana)](#bước-2-khởi-động-hạ-tầng-nền-tảng)
   - [Bước 3: Huấn luyện và Đăng ký Model vào MLflow Registry](#bước-3-huấn-luyện-và-đăng-ký-model-vào-mlflow-registry)
   - [Bước 4: Khởi tạo Database cho Airflow & Khởi động toàn bộ Service](#bước-4-khởi-tạo-database-cho-airflow--khởi-động-toàn-bộ-service)
   - [Bước 5: Kiểm tra Model Serving API](#bước-5-kiểm-tra-model-serving-api)
   - [Bước 6: Vận hành Airflow Pipeline & Cảnh báo Telegram](#bước-6-vận-hành-airflow-pipeline--cảnh-báo-telegram)
   - [Bước 7: Theo dõi Dashboards trên Grafana & Evidently](#bước-7-theo-dõi-dashboards-trên-grafana--evidently)
   - [Bước 8: Chạy giả lập dữ liệu & Kiểm thử Data Drift (Simulations)](#bước-8-chạy-giả-lập-dữ-liệu--kiểm-thử-data-drift)
6. [Tài liệu API (API Documentation)](#-tài-liệu-api)
7. [Xử lý sự cố thường gặp (Troubleshooting & FAQ)](#-xử-lý-sự-cố-thường-gặp-troubleshooting)
8. [Cấu trúc thư mục (Project Structure)](#-cấu-trúc-thư-mục)

---

## 🎯 Tổng quan hệ thống

Dự án cung cấp một hệ thống MLOps hoàn chỉnh đáp ứng tiêu chuẩn sản xuất (production-ready):
- **Model Registry & Tracking**: Quản lý phiên bản mô hình, siêu tham số và artifacts với **MLflow** và **MinIO (S3-compatible)**.
- **Model Serving**: API tốc độ cao bằng **FastAPI**, tự động nạp mô hình từ MLflow Model Registry (Production stage) và xuất Prometheus metrics.
- **Data Drift & Quality Monitoring**: **Evidently AI** giám sát phân phối dữ liệu đầu vào, phát hiện sai lệch (drift score, p-value), tạo báo cáo HTML trực quan.
- **Metrics Collection & Dashboards**: **Prometheus** thu thập số liệu thời gian thực và **Grafana** hiển thị biểu đồ chi tiết (Request rate, Latency p95/p99, Error rate, Drift metrics).
- **Workflow Orchestration**: **Apache Airflow** lập lịch định kỳ kiểm tra sức khỏe hệ thống, kích hoạt phân tích drift và tự động gửi thông báo.
- **Real-time Alerting**: Tích hợp cảnh báo tự động về **Telegram Bot** khi xảy ra Data Drift hoặc API gặp sự cố (cả từ Airflow DAG lẫn Grafana Alerting).

---

## 🏗 Kiến trúc hệ thống

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             PRODUCTION ML MONITORING STACK                       │
│                                                                                  │
│   ┌───────────────┐        ┌──────────────┐         ┌────────────────────────┐   │
│   │    Grafana    │───────▶│  Prometheus  │◀────────│   FastAPI Model API    │   │
│   │    (:3000)    │        │   (:9090)    │         │        (:8000)         │   │
│   └───────┬───────┘        └──────▲───────┘         └───────────┬────────────┘   │
│           │                       │                             │                │
│           │ (Telegram Alerts)     │ (Metrics)                   │                │
│           ▼                       │                             ▼ (Predictions)  │
│   ┌───────────────┐        ┌──────┴────────┐        ┌────────────────────────┐   │
│   │ Telegram Bot  │◀───────│ Evidently AI  │◀───────│   Data Simulation      │   │
│   │ (Chat Alerts) │        │    (:8001)    │        │  (Traffic/Drift Test)  │   │
│   └───────▲───────┘        └───────────────┘        └────────────────────────┘   │
│           │                                                                      │
│           │ (Alerts on drift / failures)                                         │
│   ┌───────┴───────────────┐        ┌──────────────┐         ┌────────────────┐   │
│   │    Apache Airflow     │───────▶│    MLflow    │────────▶│ MinIO Storage  │   │
│   │   Webserver/Scheduler │        │   (:5000)    │         │  (:9000/:9001) │   │
│   │        (:8080)        │        └──────┬───────┘         └────────────────┘   │
│   └───────────────────────┘               │                                      │
│                                           ▼                                      │
│                                    ┌──────────────┐                              │
│                                    │  PostgreSQL  │                              │
│                                    │   (:5432)    │                              │
│                                    └──────────────┘                              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔌 Danh sách dịch vụ & Cổng truy cập

Sau khi khởi chạy hoàn tất, các dịch vụ sẵn sàng tại các địa chỉ sau:

| Dịch vụ | URL | Thông tin đăng nhập mặc định | Mục đích |
|---------|-----|------------------------------|----------|
| **FastAPI Serving** | [http://localhost:8000](http://localhost:8000) | Không cần login (Docs: `/docs`) | Dự đoán (Inference), Health check, Prometheus metrics |
| **Evidently AI** | [http://localhost:8001](http://localhost:8001) | Không cần login (Docs: `/docs`) | Báo cáo Data Drift (`/reports`), Drift API |
| **Grafana Dashboards** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | Biểu đồ trực quan hóa API, Latency, Data Drift |
| **MLflow Registry** | [http://localhost:5000](http://localhost:5000) | Không cần login | Quản lý thí nghiệm và Model Registry |
| **Apache Airflow** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` | Điều phối pipeline, giám sát định kỳ & gửi Telegram |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | Không cần login | Thu thập và truy vấn Time-series metrics |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minio` / `minio123` | Giao diện quản lý S3 Bucket lưu trữ Model Artifacts |
| **MinIO S3 API** | [http://localhost:9000](http://localhost:9000) | S3 Access Key / Secret Key | S3 Storage Backend |
| **PostgreSQL** | `localhost:5432` | `mlflow` / `mlflow` | Database cho MLflow Backend & Airflow Metadata |

---

## 💻 Yêu cầu tiên quyết (Prerequisites)

Để chạy trơn tru toàn bộ hệ thống, máy tính cần cài đặt:

1. **Docker Desktop** (trên Windows / macOS) hoặc **Docker Engine & Docker Compose** (trên Linux):
   - Phiên bản Docker: `≥ 20.10.0`
   - Phiên bản Docker Compose: `≥ 2.0.0`
   - *Lưu ý*: Hãy chắc chắn Docker daemon đang hoạt động (`docker info`).
2. **Git**: Để clone mã nguồn.
3. *(Tùy chọn)* Python 3.10+: Nếu muốn chạy script training và simulation trực tiếp trên máy host. **Nếu không cài Python trên host, bạn hoàn toàn có thể chạy training trực tiếp thông qua Docker container theo hướng dẫn dưới đây.**

---

## 🚀 Hướng dẫn cài đặt & chạy từ A-Z

> [!IMPORTANT]
> **QUY TẮC BẮT BUỘC:** Không chạy `docker compose up -d` tất cả dịch vụ ngay từ lần đầu tiên. Container `api` cần một model đã được đăng ký ở trạng thái `Production` trong MLflow để nạp vào bộ nhớ. Nếu khởi động `api` khi chưa train model, `api` sẽ báo lỗi và liên tục khởi động lại (unhealthy).
> Hãy làm theo đúng 5 bước tuần tự bên dưới.

### Bước 1: Clone Repository & Cấu hình `.env`

Mở Terminal (hoặc PowerShell trên Windows) và thực hiện:

```bash
# 1. Clone repository về máy
git clone <URL_CUA_REPOSITORY>
cd ml-monitoring

# 2. Tạo file cấu hình môi trường từ mẫu có sẵn
# Trên Linux/macOS:
cp .env.example .env

# Trên Windows PowerShell:
Copy-Item .env.example .env
```

Mở file `.env` bằng trình soạn thảo (VS Code, Notepad, Nano...) để kiểm tra các thông số:
- Biến `USER`: Tên định danh cho container (mặc định: `USER=dongnd`).
- Cấu hình Telegram (Nếu muốn nhận tin nhắn cảnh báo):
  - `TELEGRAM_BOT_TOKEN`: Token bot lấy từ [@BotFather](https://t.me/BotFather).
  - `TELEGRAM_CHAT_ID`: ID người dùng hoặc nhóm chat (lấy nhanh bằng cách nhắn tin cho bot [@userinfobot](https://t.me/userinfobot)).
  > *Lưu ý*: Nếu chưa có Telegram Bot, bạn có thể để nguyên giá trị mặc định để chạy thử hệ thống trước.

---

### Bước 2: Khởi động Hạ tầng nền tảng

Khởi động các dịch vụ lưu trữ dữ liệu, cơ sở dữ liệu, MLflow, Prometheus và Grafana trước:

```bash
docker compose up -d postgres minio minio-init mlflow prometheus grafana
```

Kiểm tra trạng thái các container sau khoảng 10-15 giây:

```bash
docker compose ps
```

Đảm bảo các container sau đang ở trạng thái `Up` hoặc `healthy`:
- `..._monitoring-postgres`
- `..._monitoring-minio`
- `..._monitoring-mlflow`
- `..._monitoring-prometheus`
- `..._monitoring-grafana`

---

### Bước 3: Huấn luyện và Đăng ký Model vào MLflow Registry

Bây giờ chúng ta cần huấn luyện mô hình phân loại chất lượng rượu vang (`wine_quality_model`), lưu trữ artifact vào MinIO và đưa phiên bản này vào trạng thái `Production` trên MLflow.

Bạn có thể chọn **1 trong 2 cách** sau:

#### Cách 1: Chạy trực tiếp bên trong Docker MLflow (Khuyên Dùng ⭐⭐⭐)
*Ưu điểm: Không cần cài đặt Python, không lo xung đột thư viện hay lỗi biên dịch trên máy host.*

- **Trên Windows PowerShell:**
  ```powershell
  # Đọc biến USER từ .env (hoặc gõ trực tiếp tên container, ví dụ: dongnd_monitoring-mlflow)
  $USER_NAME = (Get-Content .env | Select-String "^USER=").Line.Split("=")[1].Trim()

  # Copy file training.py vào container mlflow
  docker cp scripts/training.py "${USER_NAME}_monitoring-mlflow:/mlflow/training.py"

  # Chạy huấn luyện và đăng ký model
  docker exec -e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 "${USER_NAME}_monitoring-mlflow" python /mlflow/training.py
  ```

- **Trên Linux / macOS / Git Bash:**
  ```bash
  USER_NAME=$(grep -E '^USER=' .env | cut -d '=' -f 2 || echo "dongnd")

  # Copy file training.py vào container mlflow
  docker cp scripts/training.py "${USER_NAME}_monitoring-mlflow:/mlflow/training.py"

  # Chạy huấn luyện và đăng ký model
  docker exec -e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 "${USER_NAME}_monitoring-mlflow" python /mlflow/training.py
  ```

#### Cách 2: Chạy trực tiếp trên máy Host (Yêu cầu Python 3.10 - 3.11)
```bash
pip install -r scripts/requirements.txt
python scripts/training.py
```

**Kết quả kỳ vọng xuất hiện trên màn hình:**
```text
✓ MLFlow Tracking URI: http://mlflow:5000
✓ Loading wine dataset...
✓ Training samples: 142 | Test samples: 36
✓ Training RandomForest model...
✓ Model Performance: Accuracy: 0.9722, F1 Score: 0.9722
✓ Model registered successfully: wine_quality_model v1
✓ Model version 1 transitioned to Production!
```

> [!TIP]
> Bạn có thể mở trình duyệt vào [http://localhost:5000](http://localhost:5000) (MLflow UI), chọn mục **Models** sẽ thấy model `wine_quality_model` đang ở stage **Production**.

---

### Bước 4: Khởi tạo Database cho Airflow & Khởi động toàn bộ Service

Apache Airflow cần một database riêng trong PostgreSQL và bảng metadata được migrate trước khi khởi chạy webserver/scheduler.

#### 4.1. Tạo database `airflow` trong PostgreSQL:
- **Trên Windows PowerShell:**
  ```powershell
  $USER_NAME = (Get-Content .env | Select-String "^USER=").Line.Split("=")[1].Trim()
  docker exec "${USER_NAME}_monitoring-postgres" psql -U mlflow -d mlflow -c "CREATE DATABASE airflow;"
  ```
- **Trên Linux / macOS:**
  ```bash
  USER_NAME=$(grep -E '^USER=' .env | cut -d '=' -f 2 || echo "dongnd")
  docker exec "${USER_NAME}_monitoring-postgres" psql -U mlflow -d mlflow -c "CREATE DATABASE airflow;"
  ```
*(Nếu database `airflow` đã tồn tại từ lần chạy trước, bạn có thể bỏ qua thông báo "already exists").*

#### 4.2. Migrate database và tạo tài khoản quản trị Airflow:
```bash
# Chạy migration bảng cho Airflow
docker compose run --rm airflow-webserver airflow db migrate

# Tạo tài khoản đăng nhập admin / admin
docker compose run --rm airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

#### 4.3. Khởi động API, Evidently và Airflow:
```bash
docker compose up -d api evidently airflow-webserver airflow-scheduler
```

Kiểm tra lại toàn bộ trạng thái hệ thống:
```bash
docker compose ps
```
Tất cả các container bây giờ phải hiển thị trạng thái `Up` và `healthy`!

---

### Bước 5: Kiểm tra Model Serving API

Xác nhận API đã tải thành công mô hình từ MLflow Model Registry:

#### 1. Kiểm tra trạng thái sức khỏe (Health Check):
```bash
curl http://localhost:8000/health
```
**Kết quả trả về:**
```json
{
  "status": "healthy",
  "model_loaded": true,
  "model_name": "wine_quality_model",
  "model_version": "1",
  "uptime_seconds": 15.42
}
```

#### 2. Gửi một request dự đoán (Inference):
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [13.2, 2.77, 2.51, 18.5, 96.6, 1.04, 2.55, 0.57, 1.47, 6.2, 1.05, 3.33, 820.0]}'
```
**Kết quả trả về:**
```json
{
  "prediction": 0.0,
  "model_name": "wine_quality_model",
  "model_version": "1",
  "timestamp": "2026-09-27T...",
  "latency_ms": 3.85
}
```

---

### Bước 6: Vận hành Airflow Pipeline & Cảnh báo Telegram

1. Truy cập giao diện Airflow Web UI: [http://localhost:8080](http://localhost:8080)
2. Đăng nhập bằng tài khoản:
   - **Username**: `admin`
   - **Password**: `admin`
3. Tại giao diện chính, bạn sẽ thấy 2 DAGs được lập trình sẵn:
   - **`test_telegram_notification`**: Dùng để kiểm tra nhanh kết nối gửi thông báo về Telegram. Bấm nút **Trigger DAG** (biểu tượng Play ▶) để kiểm tra tin nhắn về bot.
   - **`ml_model_monitoring_pipeline`**: Pipeline tự động thực hiện định kỳ:
     - `check_api_health`: Kiểm tra FastAPI có đang sống không.
     - `trigger_drift_analysis`: Gọi Evidently `/analyze` đối chiếu dữ liệu inference với baseline.
     - `query_prometheus_drift_metrics`: Kiểm tra chỉ số `evidently_data_drift_detected` và `evidently_drift_score`.
     - `alert_if_drift_or_failure`: Bắn tin nhắn cảnh báo đỏ đến Telegram nếu phát hiện dữ liệu bị trôi (Drift) hoặc hệ thống gặp sự cố.

---

### Bước 7: Theo dõi Dashboards trên Grafana & Evidently

1. Truy cập Grafana: [http://localhost:3000](http://localhost:3000)
2. Đăng nhập với tài khoản: `admin` / `admin`
3. Vào menu **Dashboards** để xem 2 dashboard có sẵn:
   - **ML Monitoring**:
     - Số lượng request/giây (RPS)
     - Thời gian phản hồi (Latency P50, P95, P99)
     - Tỷ lệ lỗi (Error Rate)
     - Phân phối kết quả dự đoán của model
   - **Evidently - Data Drift Monitoring**:
     - Điểm số trôi dữ liệu (Drift Score)
     - Số lượng đặc trưng (features) bị drift
     - Tỷ lệ giá trị null/missing values theo từng feature
4. Truy cập Evidently Reports: [http://localhost:8001/reports](http://localhost:8001/reports) để xem các báo cáo trực quan dưới dạng biểu đồ tương tác HTML.

---

### Bước 8: Chạy giả lập dữ liệu & Kiểm thử Data Drift

Thư mục `simulations/` chứa bộ công cụ tạo lưu lượng truy cập thực tế và giả lập các tình huống phân phối dữ liệu bị lệch (drift):

```bash
cd simulations
pip install -r requirements.txt
```

#### 1. Gửi 100 requests bình thường (Normal traffic):
```bash
python run_simulation.py -n 100 -s normal
```

#### 2. Giả lập dữ liệu bị Drift & tự động kích hoạt phân tích Evidently:
```bash
python run_simulation.py -n 200 -s moderate_drift --analyze
```

#### 3. Chạy các kịch bản định sẵn:
```bash
python scenarios.py 1  # Kịch bản 1: Lưu lượng hoạt động bình thường
python scenarios.py 2  # Kịch bản 2: Dữ liệu trôi dần theo thời gian (Gradual drift)
python scenarios.py 3  # Kịch bản 3: Phân phối thay đổi đột ngột (Sudden shift)
python scenarios.py 6  # Kịch bản 6: Kiểm thử chịu tải (Stress test)
```

Khi chạy các kịch bản drift, bạn hãy quan sát:
1. Grafana Dashboard cập nhật chỉ số drift tức thì.
2. Airflow DAG phát hiện drift trong lần quét tiếp theo và gửi thông báo cảnh báo về Telegram của bạn.

---

## 📖 Tài liệu API

FastAPI cung cấp giao diện Swagger UI tương tác trực tiếp tại: [http://localhost:8000/docs](http://localhost:8000/docs)

### Các Endpoint chính:
- `GET /health`: Kiểm tra trạng thái API, uptime và model đã load.
- `POST /predict`: Dự đoán với đầu vào là mảng 13 features của rượu vang:
  ```json
  {
    "features": [13.2, 2.77, 2.51, 18.5, 96.6, 1.04, 2.55, 0.57, 1.47, 6.2, 1.05, 3.33, 820.0]
  }
  ```
- `GET /model/info`: Lấy thông tin chi tiết về model đang hoạt động (tên model, phiên bản, thời gian load).
- `POST /model/reload`: Tải lại mô hình mới nhất từ MLflow mà không cần khởi động lại container.
- `GET /metrics`: Endpoint cho Prometheus scrape các metrics.

---

## 🛠 Xử lý sự cố thường gặp (Troubleshooting)

### 1. Container `api` liên tục bị khởi động lại hoặc báo `unhealthy`
- **Nguyên nhân**: Model chưa được huấn luyện hoặc chưa được gắn stage `Production` trong MLflow Registry trước khi API bật lên.
- **Khắc phục**:
  1. Kiểm tra MLflow xem model đã có chưa: `curl http://localhost:5000/api/2.0/mlflow/registered-models/get?name=wine_quality_model`
  2. Nếu chưa có hoặc lỗi, chạy lại bước huấn luyện (Bước 3).
  3. Sau khi model đã ở Production, khởi động lại API:
     ```bash
     docker compose restart api
     ```

### 2. Airflow báo lỗi `FATAL: database "airflow" does not exist`
- **Nguyên nhân**: PostgreSQL chỉ khởi tạo database `mlflow` theo biến môi trường mặc định lúc tạo volume lần đầu.
- **Khắc phục**: Chạy lệnh tạo database bằng tay vào container postgres:
  ```bash
  docker exec -it <TEN_CONTAINER_POSTGRES> psql -U mlflow -d mlflow -c "CREATE DATABASE airflow;"
  docker compose run --rm airflow-webserver airflow db migrate
  docker compose restart airflow-webserver airflow-scheduler
  ```

### 3. Không nhận được tin nhắn Telegram cảnh báo
- **Nguyên nhân**: Chưa cấu hình hoặc cấu hình sai `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` trong `.env`, hoặc chưa bấm `/start` tương tác với bot.
- **Khắc phục**:
  1. Mở app Telegram, tìm đến con bot vừa tạo và bấm `/start`.
  2. Điền chính xác `TELEGRAM_BOT_TOKEN` và `TELEGRAM_CHAT_ID` vào file `.env`.
  3. Khởi động lại service Airflow:
     ```bash
     docker compose restart airflow-webserver airflow-scheduler
     ```
  4. Vào Airflow UI [http://localhost:8080](http://localhost:8080) và trigger DAG `test_telegram_notification` để kiểm tra.

### 4. Dừng hệ thống & Reset sạch dữ liệu (Clean Reset)
Nếu muốn tắt hoàn toàn hệ thống hoặc xóa toàn bộ volume dữ liệu để làm lại từ đầu:

```bash
# Dừng tất cả container (giữ nguyên dữ liệu)
docker compose down

# Dừng và XÓA TOÀN BỘ volumes dữ liệu (lưu ý: sẽ xóa database, artifacts)
docker compose down -v
```

---

## 📁 Cấu trúc thư mục

```
ml-monitoring/
├── .env.example              # File cấu hình môi trường mẫu
├── .env                      # File cấu hình môi trường đang áp dụng (gitignored)
├── docker-compose.yml        # Orchestration định nghĩa toàn bộ 10 containers
├── README.md                 # Hướng dẫn chi tiết sử dụng dự án
├── airflow_dags/             # Thư mục chứa các DAGs của Apache Airflow
│   ├── ml_monitoring_dag.py  # Pipeline tự động kiểm tra sức khỏe, drift & cảnh báo
│   ├── telegram_alert.py     # Module phụ trợ kết nối Telegram API
│   └── test_telegram_dag.py  # DAG kiểm thử kết nối Telegram
├── api/                      # Mã nguồn FastAPI Model Serving
│   ├── Dockerfile            # Container build cho API
│   ├── main.py               # Ứng dụng FastAPI & định nghĩa Prometheus metrics
│   └── requirements.txt      # Thư viện Python cho API (numpy, sklearn, fastapi)
├── config/                   # Cấu hình cho Prometheus và Grafana
│   ├── prometheus/           # Cấu hình scrape targets & Alert rules
│   │   ├── prometheus.yml
│   │   └── evidently_alerts.yml
│   └── grafana/              # Provisioning Datasources, Dashboards & Telegram
│       ├── provisioning/
│       │   ├── alerting/telegram.yaml
│       │   ├── datasources/prometheus.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/       # JSON mẫu các Dashboard
├── evidently/                # Dịch vụ phát hiện Data Drift & xuất báo cáo
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
├── mlflow/                   # Cấu hình khởi chạy MLflow Tracking Server
│   └── Dockerfile
├── scripts/                  # Scripts huấn luyện mô hình ban đầu
│   ├── training.py           # Huấn luyện RandomForest & đăng ký MLflow Production
│   └── requirements.txt
└── simulations/              # Bộ công cụ giả lập traffic & drift để kiểm thử
    ├── run_simulation.py     # Chạy request mô phỏng với các tùy chọn
    ├── scenarios.py          # 6 kịch bản thử nghiệm tải & drift nâng cao
    ├── data_generator.py     # Sinh dữ liệu phân phối bình thường & drift
    └── quick_test.sh         # Script kiểm tra nhanh end-to-end
```

---

## 📄 License & Giấy phép

Dự án được xây dựng và chia sẻ phục vụ mục đích học tập, nghiên cứu và triển khai MLOps trong thực tế. Mọi đóng góp (Contributions) đều được hoan nghênh!