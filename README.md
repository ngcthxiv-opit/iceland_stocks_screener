# Iceland Stocks Screener

## Cấu trúc Project

```text
screener/
├─ app.py
├─ strategies.py
├─ requirements.txt
├─ README.md
├─ Database/
│  ├─ data.py
│  └─ import.py
├─ Output/
│  └─ Financial_Data.sqlite   (sinh ra sau khi chạy ETL)
├─ Resource/
│  ├─ iceland YEAR.xlsx       (input BCTC năm)
│  ├─ iceland QUA.xlsx        (input BCTC quý)
│  └─ PRICE.xlsx              (input dữ liệu giá OHLCV)
├─ static/
│  ├─ css/
│  │  └─ style.css
│  └─ js/
│     └─ app.js
└─ templates/
	└─ index.html
```

### Vai trò từng nhóm file

- `app.py`: Flask backend, expose API (`/api/meta`, `/api/screener`, `/api/strategy`, export Excel...), tính toán kỹ thuật (MACD, RSI), biến động giá và xếp hạng cơ bản.
- `strategies.py`: định nghĩa 7 chiến lược đầu tư và `DBDataLoader` để đọc dữ liệu từ SQLite.
- `Database/data.py`: pipeline chuẩn hóa dữ liệu đầu vào từ Excel (dịch chỉ số, parse sheet, làm sạch dữ liệu).
- `Database/import.py`: tạo schema SQLite (Star Schema), upsert dimension/fact, import dữ liệu BCTC + OHLCV vào `Output/Financial_Data.sqlite`.
- `templates/index.html`: giao diện trang chính.
- `static/js/app.js`: state UI, gọi API, render bảng, lọc, tìm kiếm, export.
- `static/css/style.css`: style giao diện.

## Cấu trúc Database

Database dùng SQLite theo mô hình Star Schema, gồm 4 bảng dimension và 2 bảng fact.

### 1) Dimension Tables

#### `dim_gics`
- `gics_industry_id` (PK)
- `gics_industry` (UNIQUE)
- `gics_sector`

#### `dim_company`
- `company_id` (PK)
- `ticker` (UNIQUE)
- `company_name`
- `listing_date`
- `exchange`
- `gics_industry_id` (FK -> `dim_gics.gics_industry_id`)

#### `dim_report_group`
- `report_group_id` (PK)
- `report_group_code` (UNIQUE)
- `report_group_name`

#### `dim_indicator`
- `indicator_id` (PK)
- `indicator_name` (UNIQUE)
- `report_group_id` (FK -> `dim_report_group.report_group_id`)

### 2) Fact Tables

#### `fact_financial`
- `fact_id` (PK)
- `company_id` (FK -> `dim_company.company_id`)
- `report_date`
- `period_type` (`A` hoặc `Q`)
- `fiscal_quarter` (1..4, nullable với dữ liệu năm)
- `indicator_id` (FK -> `dim_indicator.indicator_id`)
- `value_numeric`
- `value_text`
- Unique key nghiệp vụ: (`company_id`, `report_date`, `period_type`, `indicator_id`)

#### `fact_market_price`
- `price_id` (PK)
- `company_id` (FK -> `dim_company.company_id`)
- `trading_date`
- `price_close`, `price_open`, `price_high`, `price_low`
- `volume`
- Unique key nghiệp vụ: (`company_id`, `trading_date`)

### 3) Quan hệ dữ liệu (tóm tắt)

```text
dim_gics (1) ──< dim_company (N)
dim_report_group (1) ──< dim_indicator (N)
dim_company (1) ──< fact_financial (N) >── (1) dim_indicator
dim_company (1) ──< fact_market_price (N)
```

### 4) Index chính phục vụ truy vấn

- `idx_company_ticker` trên `dim_company(ticker)`
- `idx_indicator_name` trên `dim_indicator(indicator_name)`
- `idx_fact_company_period_date` trên `fact_financial(company_id, period_type, report_date)`
- `idx_fact_indicator_period_date` trên `fact_financial(indicator_id, period_type, report_date)`
- `idx_fact_quarter` trên `fact_financial(fiscal_quarter)`
- `idx_fact_price_date` trên `fact_market_price(company_id, trading_date)`

## Luồng hoạt động

### A. Luồng ETL (nạp dữ liệu vào database)

1. Chuẩn bị file nguồn trong `Resource/`:
	- `iceland YEAR.xlsx`
	- `iceland QUA.xlsx`
	- `PRICE.xlsx`
2. Chạy script import:

```bash
python Database/import.py
```

3. `Database/data.py` đọc và chuẩn hóa dữ liệu:
	- Dịch tên chỉ số EN -> VI.
	- Parse cấu trúc sheet Excel.
	- Làm sạch ticker/ngày/kiểu dữ liệu.
4. `Database/import.py` tạo schema + index nếu chưa có.
5. Nạp dimension (`dim_gics`, `dim_company`, `dim_report_group`, `dim_indicator`).
6. Nạp fact:
	- `fact_financial` từ dữ liệu BCTC năm/quý.
	- `fact_market_price` từ dữ liệu OHLCV.
7. Kết quả: file `Output/Financial_Data.sqlite` sẵn sàng cho backend.

### B. Luồng runtime (người dùng -> web -> backend -> database)

1. Chạy backend:

```bash
python app.py
```

2. Mở `http://localhost:5000`.
3. Frontend (`static/js/app.js`) gọi `/api/meta` để lấy:
	- Danh sách năm/quý
	- Danh sách chỉ số
	- Danh sách công ty
4. Khi user đổi bộ lọc/tìm kiếm/tab chiến lược:
	- Tab `TẤT CẢ` -> gọi `/api/screener`
	- Tab chiến lược -> gọi `/api/strategy`
5. Backend (`app.py`) xử lý:
	- Đọc dữ liệu financial và market price từ SQLite.
	- Pivot chỉ số tài chính theo công ty.
	- Tính thêm `EPS_4Q` alias, biến động giá (1D..YTD), MACD/RSI, tín hiệu Mua/Bán.
	- Tính `Điểm cơ bản`, `rank_pct`, `rating`.
	- Áp dụng search + filter số từ UI.
6. Trả JSON về frontend để render bảng.
7. Khi export:
	- `/api/export` cho chế độ tổng quát.
	- `/api/export_strategy` cho chế độ chiến lược.
	- Backend xuất file Excel theo đúng cột đang hiển thị.

### C. Luồng chiến lược đầu tư

1. UI chọn tab chiến lược (`quality`, `garp`, `value`, `dividend`, `health`, `efficiency`, `cashflow`).
2. Backend tạo `DBDataLoader` (đọc dữ liệu năm 2024, period `A`).
3. Chạy class chiến lược tương ứng trong `strategies.py` để lấy mã đạt tiêu chí.
4. Merge lại với dữ liệu thị trường + chỉ số tài chính để hiển thị đầy đủ.
5. Trả thêm `badges` (tiêu chí/đếm số mã) và `preset_cols` cho UI.
