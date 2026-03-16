import sqlite3
import pandas as pd
import numpy as np
import warnings
import os
import time

try:
    import data 
except ImportError:
    pass 

warnings.filterwarnings('ignore', category=UserWarning)

# ==============================================================
# 1. SQL SCHEMA CONSTANTS (DDL)
# ==============================================================
CREATE_TABLES_SQL = {
    "dim_gics": """
        CREATE TABLE IF NOT EXISTS dim_gics (
            gics_industry_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            gics_industry     VARCHAR(200) NOT NULL UNIQUE,
            gics_sector       VARCHAR(100) NOT NULL
        );
    """,
    "dim_company": """
        CREATE TABLE IF NOT EXISTS dim_company (
            company_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker            VARCHAR(20)  NOT NULL UNIQUE,
            company_name      VARCHAR(255),
            listing_date      DATE,
            exchange          VARCHAR(100),
            gics_industry_id  INTEGER REFERENCES dim_gics(gics_industry_id)
        );
    """,
    "dim_report_group": """
        CREATE TABLE IF NOT EXISTS dim_report_group (
            report_group_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            report_group_code VARCHAR(50)  NOT NULL UNIQUE,
            report_group_name VARCHAR(200) NOT NULL
        );
    """,
    "dim_indicator": """
        CREATE TABLE IF NOT EXISTS dim_indicator (
            indicator_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_name    VARCHAR(500) NOT NULL UNIQUE,
            report_group_id   INTEGER NOT NULL REFERENCES dim_report_group(report_group_id)
        );
    """,
    "fact_financial": """
        CREATE TABLE IF NOT EXISTS fact_financial (
            fact_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id        INTEGER NOT NULL REFERENCES dim_company(company_id),
            report_date       DATE,
            period_type       CHAR(1)  NOT NULL DEFAULT 'A' CHECK (period_type IN ('A', 'Q')),
            fiscal_quarter    INTEGER           CHECK (fiscal_quarter BETWEEN 1 AND 4),
            indicator_id      INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
            value_numeric     REAL,
            value_text        TEXT,
            UNIQUE (company_id, report_date, period_type, indicator_id)
        );
    """,

    "fact_market_price": """
        CREATE TABLE IF NOT EXISTS fact_market_price (
            price_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id        INTEGER NOT NULL REFERENCES dim_company(company_id),
            trading_date      DATE NOT NULL,
            price_close       REAL,
            price_open        REAL,
            price_high        REAL,
            price_low         REAL,
            volume            BIGINT,
            UNIQUE (company_id, trading_date)
        );
    """
}

SQL_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_company_ticker ON dim_company (ticker);",
    "CREATE INDEX IF NOT EXISTS idx_indicator_name ON dim_indicator (indicator_name);",
    "CREATE INDEX IF NOT EXISTS idx_indicator_report_group ON dim_indicator (report_group_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_company_period_date ON fact_financial (company_id, period_type, report_date);",
    "CREATE INDEX IF NOT EXISTS idx_fact_indicator_period_date ON fact_financial (indicator_id, period_type, report_date);",
    "CREATE INDEX IF NOT EXISTS idx_fact_quarter ON fact_financial (fiscal_quarter) WHERE fiscal_quarter IS NOT NULL;",
    "CREATE INDEX IF NOT EXISTS idx_fact_price_date ON fact_market_price (company_id, trading_date);",
]

# ==============================================================
# 2. DATA ACCESS OBJECT (DAO) CLASS
# ==============================================================
class VNStockDatabaseManager:
    """
    Class quản lý Cơ sở dữ liệu Cổ phiếu.
    Bao đóng các nghiệp vụ DDL, DML, và tối ưu I/O.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")  
        self.conn.execute("PRAGMA synchronous = NORMAL;")

    def initialize_schema(self):
        print(f"[DB] Khởi tạo schema (Star Schema) tại: {self.db_path}...")
        cursor = self.conn.cursor()
        for table, ddl in CREATE_TABLES_SQL.items():
            cursor.execute(ddl)
        for idx in SQL_CREATE_INDEXES:
            cursor.execute(idx)
        self.conn.commit()

    def upsert_dimensions(self, df_comp: pd.DataFrame, df_fact: pd.DataFrame):
        print("[DB] Đang nạp Dimension Tables (GICS, Company, Report, Indicator)...")
        cursor = self.conn.cursor()

        if not df_comp.empty:
            comp_wide = df_comp.pivot_table(index='Mã CK', columns='Chỉ số', values='Giá trị', aggfunc='first').reset_index()
            
            for col in ['Ngành GICS', 'Lĩnh vực GICS', 'Tên công ty', 'Ngày niêm yết', 'Sàn giao dịch']:
                if col not in comp_wide.columns: comp_wide[col] = None

            gics_data = comp_wide[['Ngành GICS', 'Lĩnh vực GICS']].dropna(subset=['Ngành GICS']).drop_duplicates()
            cursor.executemany("INSERT OR IGNORE INTO dim_gics (gics_industry, gics_sector) VALUES (?, ?)", gics_data.values.tolist())
            self.conn.commit()

            gics_map = dict(cursor.execute("SELECT gics_industry, gics_industry_id FROM dim_gics").fetchall())

            comp_records = []
            for _, row in comp_wide.iterrows():
                ticker = str(row['Mã CK']).replace("'", "").strip().upper()
                list_date = pd.to_datetime(row['Ngày niêm yết'], errors='coerce').strftime('%Y-%m-%d') if pd.notna(row['Ngày niêm yết']) else None
                gics_id = gics_map.get(row['Ngành GICS'])
                comp_records.append((ticker, row['Tên công ty'], list_date, row['Sàn giao dịch'], gics_id))

            cursor.executemany("INSERT OR IGNORE INTO dim_company (ticker, company_name, listing_date, exchange, gics_industry_id) VALUES (?, ?, ?, ?, ?)", comp_records)
            self.conn.commit()

        if not df_fact.empty:
            groups = df_fact[['Báo cáo']].dropna().drop_duplicates()
            groups['report_group_code'] = groups['Báo cáo'].apply(lambda x: "RG_" + str(hash(x))[:8].replace("-", "0"))
            
            cursor.executemany("INSERT OR IGNORE INTO dim_report_group (report_group_code, report_group_name) VALUES (?, ?)", groups[['report_group_code', 'Báo cáo']].values.tolist())
            self.conn.commit()

            report_map = dict(cursor.execute("SELECT report_group_name, report_group_id FROM dim_report_group").fetchall())

            inds = df_fact[['Chỉ số', 'Báo cáo']].dropna().drop_duplicates()
            inds['report_group_id'] = inds['Báo cáo'].map(report_map)
            
            cursor.executemany("INSERT OR IGNORE INTO dim_indicator (indicator_name, report_group_id) VALUES (?, ?)", inds[['Chỉ số', 'report_group_id']].dropna().values.tolist())
            self.conn.commit()

    def upsert_fact_financial(self, df_fact: pd.DataFrame):
        if df_fact.empty: return
            
        print(f"[DB] Chuẩn bị Transform và nạp {len(df_fact):,} records vào Fact_Financial...")
        cursor = self.conn.cursor()

        company_map = dict(cursor.execute("SELECT ticker, company_id FROM dim_company").fetchall())
        indicator_map = dict(cursor.execute("SELECT indicator_name, indicator_id FROM dim_indicator").fetchall())

        df_t = df_fact.copy()
        df_t['Mã CK'] = df_t['Mã CK'].astype(str).str.replace("'", "", regex=False).str.strip().str.upper()
        df_t['company_id'] = df_t['Mã CK'].map(company_map)
        df_t['indicator_id'] = df_t['Chỉ số'].map(indicator_map)
        df_t = df_t.dropna(subset=['company_id', 'indicator_id', 'Ngày']) # Phải có ngày mới đưa vào DB Time-series

        df_t['Ngày_Parsed'] = pd.to_datetime(df_t['Ngày'], format='%d/%m/%Y', errors='coerce')
        df_t = df_t.dropna(subset=['Ngày_Parsed'])
        df_t['report_date'] = df_t['Ngày_Parsed'].dt.strftime('%Y-%m-%d')
        
        df_t['fiscal_quarter'] = np.where(
            df_t['Period_Type'] == 'Q',
            ((df_t['Ngày_Parsed'].dt.month - 1) // 3) + 1,
            None
        )

        df_t['value_numeric'] = pd.to_numeric(df_t['Giá trị'], errors='coerce')
        df_t['value_text'] = np.where(df_t['value_numeric'].isna(), df_t['Giá trị'].astype(str), None)

        records = df_t[[
            'company_id', 'report_date', 'Period_Type', 
            'fiscal_quarter', 'indicator_id', 'value_numeric', 'value_text'
        ]].values.tolist()

        CHUNK_SIZE = 50000
        total_chunks = (len(records) // CHUNK_SIZE) + 1
        print(f"[DB] Bắt đầu Insert lô lớn BCTC ({total_chunks} Chunks)...")

        start_time = time.time()
        for i in range(0, len(records), CHUNK_SIZE):
            cursor.executemany("""
                INSERT OR IGNORE INTO fact_financial 
                (company_id, report_date, period_type, fiscal_quarter, indicator_id, value_numeric, value_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, records[i:i + CHUNK_SIZE])
        
        self.conn.commit()
        print(f"[DB] ✔ Insert BCTC hoàn tất trong {time.time() - start_time:.2f} giây!")

    def upsert_market_price(self, df_price: pd.DataFrame):
        """Hàm riêng biệt nạp dữ liệu Giá (OHLCV) vào bảng fact_market_price"""
        if df_price.empty: return
        print(f"[DB] Chuẩn bị nạp {len(df_price):,} records dữ liệu OHLCV...")
        cursor = self.conn.cursor()

        unique_tickers = df_price[['Ticker']].dropna().drop_duplicates()
        cursor.executemany("INSERT OR IGNORE INTO dim_company (ticker) VALUES (?)", unique_tickers.values.tolist())
        self.conn.commit()

        company_map = {ticker.replace("'", "").strip().upper(): cid
                       for ticker, cid in cursor.execute("SELECT ticker, company_id FROM dim_company").fetchall()}
        df_t = df_price.copy()
        df_t['company_id'] = df_t['Ticker'].map(company_map)
        df_t = df_t.dropna(subset=['company_id', 'Date'])

        records = df_t[[
            'company_id', 'Date', 'Price Close', 'Price Open', 
            'Price High', 'Price Low', 'Volume'
        ]].values.tolist()

        CHUNK_SIZE = 50000
        total_chunks = (len(records) // CHUNK_SIZE) + 1
        print(f"[DB] Bắt đầu Insert lô dữ liệu giá ({total_chunks} Chunks)...")

        start_time = time.time()
        for i in range(0, len(records), CHUNK_SIZE):
            cursor.executemany("""
                INSERT OR REPLACE INTO fact_market_price 
                (company_id, trading_date, price_close, price_open, price_high, price_low, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, records[i:i + CHUNK_SIZE])
            
        self.conn.commit()
        print(f"[DB] ✔ Insert OHLCV hoàn tất siêu tốc trong {time.time() - start_time:.2f} giây!")

    def close(self):
        self.conn.close()

# ==============================================================
# 3. TRÌNH QUẢN LÝ MASTER (ETL ORCHESTRATOR)
# ==============================================================
def process_dict_to_fact(data_dict: dict, period_type: str) -> pd.DataFrame:
    """Lọc và ghép nối an toàn các DataFrames mang cấu trúc Fact từ Dictionary."""
    fact_frames = []
    for sheet_name, df in data_dict.items():
        if df.empty: continue
        # Chỉ ghép những khung dữ liệu có đúng định dạng Fact
        req_cols = ['Mã CK', 'Ngày', 'Báo cáo', 'Chỉ số', 'Giá trị']
        if all(col in df.columns for col in req_cols):
            df_copy = df[req_cols].copy()
            df_copy['Period_Type'] = period_type
            fact_frames.append(df_copy)
    return pd.concat(fact_frames, ignore_index=True) if fact_frames else pd.DataFrame()

def process_raw_price_file(filepath: str) -> pd.DataFrame:
    """
    Trích xuất dữ liệu từ định dạng Excel lộn xộn, tự động nhận diện "Trượt Cột".
    """
    print(f"[*] Đọc và chuẩn hóa dữ liệu giá từ: {filepath}")
    try:
        df_raw = pd.read_excel(filepath, sheet_name='Sheet2', header=None)
        print(f"  -> File gốc có {len(df_raw)} dòng.")
        
        data_start_row = 2
        ticker_col_idx = 1
        
        for idx in range(min(15, len(df_raw))):
            for c_idx in range(min(5, len(df_raw.columns))): 
                val = str(df_raw.iloc[idx, c_idx])
                if '.IC' in val or '.VN' in val:
                    data_start_row = idx
                    ticker_col_idx = c_idx
                    break
            if '.IC' in str(df_raw.iloc[idx, ticker_col_idx]) or '.VN' in str(df_raw.iloc[idx, ticker_col_idx]):
                break
                
        print(f"  -> Dữ liệu giá nhận diện bắt đầu từ dòng index: {data_start_row}, cột Ticker tại index: {ticker_col_idx}")
        
        df_data = df_raw.iloc[data_start_row:, ticker_col_idx : ticker_col_idx + 7].copy()
        df_data.columns = ['Ticker', 'Date', 'Price Close', 'Price Open', 'Price High', 'Price Low', 'Volume']
        
        df_data['Ticker'] = df_data['Ticker'].astype(str).str.replace("'", "", regex=False).str.strip().str.upper()
        
        df_data = df_data.dropna(subset=['Ticker', 'Date'], how='any')
        
        df_data['Date'] = pd.to_datetime(df_data['Date'], errors='coerce')
        invalid_dates = df_data['Date'].isna().sum()
        if invalid_dates > 0:
            print(f"  [!] Cảnh báo: Loại bỏ {invalid_dates} dòng có định dạng ngày tháng lỗi.")
            
        df_data = df_data.dropna(subset=['Date'])
        df_data['Date'] = df_data['Date'].dt.strftime('%Y-%m-%d')
        
        numeric_cols = ['Price Close', 'Price Open', 'Price High', 'Price Low', 'Volume']
        for col in numeric_cols:
            df_data[col] = pd.to_numeric(df_data[col], errors='coerce')
            
        print(f"  -> Số lượng bản ghi OHLCV hợp lệ đã sẵn sàng nạp: {len(df_data):,}")
        return df_data
        
    except ValueError as ve:
        print(f"[-] Lỗi Tên Sheet: File Excel không có 'Sheet2'. Chi tiết: {ve}")
        return pd.DataFrame()
    except Exception as e:
        print(f"[-] Lỗi hệ thống khi xử lý file giá: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 BẮT ĐẦU ORCHESTRATING TOÀN BỘ DATA PIPELINE VÀO DATABASE")
    print("=" * 80)

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    FILE_YEAR_INPUT  = os.path.join(BASE_DIR, "Resource", "iceland YEAR.xlsx")
    FILE_QUA_INPUT   = os.path.join(BASE_DIR, "Resource", "iceland QUA.xlsx")
    FILE_PRICE_INPUT = os.path.join(BASE_DIR, "Resource", "PRICE.xlsx")   # ✔ đường dẫn đúng
    DB_OUTPUT_PATH   = os.path.join(BASE_DIR, "Output", "Financial_Data.sqlite")

    # --- 1. EXTRACT & TRANSFORM TRÊN RAM ---
    df_comp_all_list = []
    df_fact_all_list = []

    if os.path.exists(FILE_YEAR_INPUT):
        df_comp_y, dict_y = data.run_pipeline(FILE_YEAR_INPUT, period_type='A')
        df_comp_all_list.append(df_comp_y)
        df_fact_all_list.append(process_dict_to_fact(dict_y, 'A'))
    else:
        print(f"[-] Cảnh báo: Thiếu file Năm {FILE_YEAR_INPUT}")

    if os.path.exists(FILE_QUA_INPUT):
        df_comp_q, dict_q = data.run_pipeline(FILE_QUA_INPUT, period_type='Q')
        df_comp_all_list.append(df_comp_q)
        df_fact_all_list.append(process_dict_to_fact(dict_q, 'Q'))
    else:
        print(f"[-] Cảnh báo: Thiếu file Quý {FILE_QUA_INPUT}")

    df_ohlcv = pd.DataFrame()
    if os.path.exists(FILE_PRICE_INPUT):
        df_ohlcv = process_raw_price_file(FILE_PRICE_INPUT)
    else:
        print(f"[-] CẢNH BÁO QUAN TRỌNG: Không tìm thấy file dữ liệu Giá tại đường dẫn '{FILE_PRICE_INPUT}'.")

    df_comp_master = pd.concat(df_comp_all_list, ignore_index=True) if df_comp_all_list else pd.DataFrame()
    df_fact_master = pd.concat(df_fact_all_list, ignore_index=True) if df_fact_all_list else pd.DataFrame()

    # --- 2. LOAD VÀO DATABASE ---
    print("\n" + "=" * 80)
    print("💾 BƯỚC CUỐI: TƯƠNG TÁC SQLITE DATABASE")
    print("=" * 80)

    db = VNStockDatabaseManager(DB_OUTPUT_PATH)
    try:
        db.initialize_schema()

        if not df_comp_master.empty or not df_fact_master.empty:
            db.upsert_dimensions(df_comp_master, df_fact_master)
            db.upsert_fact_financial(df_fact_master)
            
        if not df_ohlcv.empty:
            conn_check = db.conn
            cursor_check = conn_check.cursor()
            valid_tickers_in_db = set(
                row[0] for row in cursor_check.execute("SELECT ticker FROM dim_company").fetchall()
            )
            if valid_tickers_in_db:
                initial_count = len(df_ohlcv)
                df_ohlcv = df_ohlcv[df_ohlcv['Ticker'].isin(valid_tickers_in_db)].copy()
                removed = initial_count - len(df_ohlcv)
                if removed > 0:
                    print(f"[*] Lọc ticker: Đã loại bỏ {removed:,} bản ghi giá không thuộc dim_company.")
                print(f"[*] Số bản ghi OHLCV hợp lệ sẽ nạp: {len(df_ohlcv):,}")
            else:
                print("[!] Cảnh báo: dim_company trống, bỏ qua import dữ liệu giá.")
                df_ohlcv = pd.DataFrame()

            if not df_ohlcv.empty:
                db.upsert_market_price(df_ohlcv)

    finally:
        db.close()

    print("\n✅ HOÀN TẤT. MỌI CHỈ SỐ ĐÃ NẰM TRỌN TRONG DATABASE.")
    print(f"📁 Đường dẫn CSDL: {os.path.abspath(DB_OUTPUT_PATH)}")