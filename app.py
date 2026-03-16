import sqlite3
import os
import io
import json
from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
import pandas as pd
import numpy as np
from datetime import datetime
from strategies import STRATEGIES, STRATEGY_LABELS, DBDataLoader

def safe_json_rows(df: pd.DataFrame):
    """Serialize DataFrame → list of dicts, converting NaN/Inf → null."""
    df = df.replace([np.inf, -np.inf], np.nan)
    return json.loads(df.to_json(orient='records', date_format='iso', default_handler=str))


app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "Output", "Financial_Data.sqlite")


# ══════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def df_from_query(query, params=()):
    conn = get_db()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# ══════════════════════════════════════
# TECHNICAL INDICATORS
# ══════════════════════════════════════
def _ema(series: pd.Series, span: int) -> pd.Series:
    """EMA không có min_periods – khớp đúng: ewm(span=N, adjust=False).mean()"""
    return series.ewm(span=span, adjust=False).mean()


def compute_technicals(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính MACD(12,26,9), RSI(14) và tín hiệu mua/bán cho từng mã.
    RSI dùng rolling mean (simple) theo công thức đã chỉ định.
    """
    results = []
    for cid, grp in price_df.groupby('company_id'):
        grp   = grp.sort_values('trading_date').reset_index(drop=True)
        close = grp['price_close'].astype(float)

        if len(close) < 9:   # cần ít nhất 9 phiên để Signal có ý nghĩa
            results.append({'company_id': cid, 'macd': None, 'rsi14': None,
                            'signal_line': None, 'trade_signal': '–'})
            continue

        macd_line   = _ema(close, 12) - _ema(close, 26)
        signal_line = _ema(macd_line, 9)

        diff = close.diff()
        gain = diff.where(diff > 0, 0).rolling(14).mean()
        loss = (-diff.where(diff < 0, 0)).rolling(14).mean()
        rsi  = 100 - (100 / (1 + gain / loss.replace(0, np.nan)))

        lm = macd_line.iloc[-1]
        ls = signal_line.iloc[-1]
        lr = rsi.iloc[-1]

        if pd.notna(lm) and pd.notna(ls) and pd.notna(lr):
            sig = 'Mua' if (lm > ls and lr < 70) else 'Bán'
        else:
            sig = 'Bán'

        results.append({
            'company_id':  cid,
            'macd':        round(float(lm), 4) if pd.notna(lm) else None,
            'rsi14':       round(float(lr), 2) if pd.notna(lr) else None,
            'signal_line': round(float(ls), 4) if pd.notna(ls) else None,
            'trade_signal': sig,
        })

    return pd.DataFrame(results) if results else pd.DataFrame()


def compute_price_changes(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    % Biến động giá theo NGÀY LỊCH, có dung sai tối đa 7 ngày:
    - Nếu ngày đích không có giao dịch (T7, CN, ngày lễ), lùi tối đa 7 ngày để tìm
    - Tất cả tính từ ngày LATEST trong DB, độc lập với bộ lọc thời gian
    - Giá tham chiếu = Close phiên liền trước
    - Giá trần = tham chiếu × 1.10 ; Giá sàn = tham chiếu × 0.90
    """
    from datetime import timedelta

    CALENDAR_OFFSETS = {
        '1D':  timedelta(days=1),
        '1W':  timedelta(weeks=1),
        '2W':  timedelta(weeks=2),
        '1M':  timedelta(days=30),
        '3M':  timedelta(days=91),
        '6M':  timedelta(days=182),
        '9M':  timedelta(days=274),
        '1Y':  timedelta(days=365),
    }
    MAX_TOLERANCE = 7   

    def get_close(grp_sorted, target_date):
        """
        Trả về giá đóng cửa gần nhất trên hoặc trước target_date,
        trong phạm vi MAX_TOLERANCE ngày lịch. None nếu ngoài phạm vi.
        """
        sub = grp_sorted[grp_sorted['trading_date'] <= target_date]
        if sub.empty:
            return None
        nearest = sub['trading_date'].iloc[-1]
        if (target_date - nearest).days <= MAX_TOLERANCE:
            return float(sub['price_close'].iloc[-1])
        return None

    results = []
    for cid, grp in price_df.groupby('company_id'):
        grp = grp.sort_values('trading_date').reset_index(drop=True)
        grp['trading_date'] = pd.to_datetime(grp['trading_date'])
        n = len(grp)
        if n == 0:
            continue

        latest_date  = grp['trading_date'].iloc[-1]
        latest_close = float(grp['price_close'].iloc[-1])
        ref_close    = float(grp['price_close'].iloc[-2]) if n >= 2 else latest_close

        row = {'company_id': cid}

        for label, delta in CALENDAR_OFFSETS.items():
            past_close = get_close(grp, latest_date - delta)
            if past_close and past_close != 0:
                row[f'change_{label}'] = round((latest_close / past_close - 1), 4)
            else:
                row[f'change_{label}'] = None

        ytd_target = pd.Timestamp(latest_date.year, 1, 1)
        ytd_close  = get_close(grp, ytd_target + timedelta(days=MAX_TOLERANCE)) 

        if ytd_close is None:
            ytd_close = get_close(grp, ytd_target)
        row['change_YTD'] = round((latest_close / ytd_close - 1), 4) if ytd_close and ytd_close != 0 else None
        last = grp.iloc[-1]
        row['price_close']  = round(latest_close, 2)
        row['price_open']   = round(float(last['price_open']),  2) if pd.notna(last['price_open'])  else None
        row['price_high']   = round(float(last['price_high']),  2) if pd.notna(last['price_high'])  else None
        row['price_low']    = round(float(last['price_low']),   2) if pd.notna(last['price_low'])   else None
        row['volume']       = int(last['volume']) if pd.notna(last['volume']) else None
        row['latest_price_date'] = str(last['trading_date'])

        results.append(row)

    return pd.DataFrame(results) if results else pd.DataFrame()


def compute_fundamental_rating(result_df: pd.DataFrame) -> pd.DataFrame:
    COL_ROE      = 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)'
    COL_GROWTH   = 'Profit_Growth'   
    COL_MARGIN   = 'Net_Margin'      
    COL_LEVERAGE = 'Leverage'        
    w1, w2, w3, w4 = 0.4, 0.3, 0.2, -0.1

    def get_col(col_name):
        if col_name not in result_df.columns:
            return pd.Series(0.0, index=result_df.index)
        return pd.to_numeric(result_df[col_name], errors='coerce') \
                 .replace([np.inf, -np.inf], np.nan).fillna(0)

    score = (
        w1 * get_col(COL_ROE) +
        w2 * get_col(COL_GROWTH) +
        w3 * get_col(COL_MARGIN) +
        w4 * get_col(COL_LEVERAGE)
    )

    result_df = result_df.copy()
    result_df['Điểm cơ bản'] = score.round(4)


    if 'rating_date' in result_df.columns:
        rank_pct = result_df.groupby('rating_date')['Điểm cơ bản'].rank(pct=True)
    else:
        rank_pct = score.rank(pct=True)

    result_df['rank_pct'] = rank_pct.round(3)
    result_df['rating']   = rank_pct.apply(
        lambda x: ('A' if x > 0.8 else 'B' if x > 0.6 else 'C' if x > 0.4 else 'D' if x > 0.2 else 'E')
        if pd.notna(x) else '–'
    )
    return result_df


def compute_raw_fundamentals(conn, max_year: str = '2024') -> pd.DataFrame:
    NI   = 'Lợi nhuận sau thuế'
    RV   = 'Tổng doanh thu gộp từ hoạt động kinh doanh'
    SD   = 'Nợ ngắn hạn và phần ngắn hạn của nợ dài hạn'
    LD   = 'Tổng nợ dài hạn'
    EQ   = 'Tổng vốn cổ phiếu thường'
    ALL  = [NI, RV, SD, LD, EQ]
    ph   = ','.join('?' for _ in ALL)

    raw  = pd.read_sql_query(f"""
        SELECT ff.company_id, di.indicator_name, ff.value_numeric, ff.report_date
        FROM fact_financial ff
        JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
        WHERE strftime('%Y', ff.report_date) <= ?
          AND di.indicator_name IN ({ph})
        ORDER BY ff.company_id, ff.report_date
    """, conn, params=[max_year] + ALL)

    if raw.empty:
        return pd.DataFrame(columns=['company_id', 'Profit_Growth', 'Net_Margin', 'Leverage'])

    results = []
    for cid, grp in raw.groupby('company_id'):
        grp   = grp.sort_values('report_date')
        pivot = grp.pivot_table(
            index='report_date', columns='indicator_name',
            values='value_numeric', aggfunc='last'
        )
        row = {'company_id': cid}

        if NI in pivot.columns:
            ni_s  = pivot[NI].replace([np.inf, -np.inf], np.nan)
            growth = ni_s.pct_change(4, fill_method=None) * 100
            last_g = growth.dropna()
            row['Profit_Growth'] = round(float(last_g.iloc[-1]), 4) if not last_g.empty else 0.0
            row['rating_date'] = str(ni_s.dropna().index[-1]) if not ni_s.dropna().empty else None
        else:
            row['Profit_Growth'] = 0.0
            row['rating_date'] = None

        def last_val(col): return float(pivot[col].dropna().iloc[-1]) if col in pivot.columns and not pivot[col].dropna().empty else None
        ni_v  = last_val(NI)
        rv_v  = last_val(RV)
        row['Net_Margin'] = round(ni_v / rv_v * 100, 4) if ni_v is not None and rv_v and rv_v != 0 else 0.0
        sd_v  = last_val(SD) or 0.0
        ld_v  = last_val(LD) or 0.0
        eq_v  = last_val(EQ)
        row['Leverage'] = round((sd_v + ld_v) / eq_v, 4) if eq_v and eq_v != 0 else 0.0

        results.append(row)

    return pd.DataFrame(results) if results else pd.DataFrame(
        columns=['company_id', 'Profit_Growth', 'Net_Margin', 'Leverage'])


# ══════════════════════════════════════
# ROUTES
# ══════════════════════════════════════
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/meta')
def api_meta():
    conn = get_db()

    years_q = conn.execute(
        "SELECT DISTINCT strftime('%Y', report_date) as yr "
        "FROM fact_financial WHERE strftime('%Y', report_date) <= '2024' ORDER BY yr DESC"
    ).fetchall()
    years = [r['yr'] for r in years_q if r['yr']]

    quarters_q = conn.execute(
        "SELECT DISTINCT fiscal_quarter as q "
        "FROM fact_financial WHERE fiscal_quarter IS NOT NULL ORDER BY q"
    ).fetchall()
    quarters = [r['q'] for r in quarters_q]

    ind_q = conn.execute("""
        SELECT di.indicator_id, di.indicator_name, drg.report_group_name as group_name
        FROM dim_indicator di
        JOIN dim_report_group drg ON di.report_group_id = drg.report_group_id
        ORDER BY drg.report_group_name, di.indicator_name
    """).fetchall()
    indicators = [{'id': r['indicator_id'], 'name': r['indicator_name'], 'group': r['group_name']}
                  for r in ind_q]

    comp_q = conn.execute("""
        SELECT dc.company_id, dc.ticker, dc.company_name, dc.exchange,
               dc.listing_date, dg.gics_industry, dg.gics_sector
        FROM dim_company dc
        LEFT JOIN dim_gics dg ON dc.gics_industry_id = dg.gics_industry_id
        ORDER BY dc.ticker
    """).fetchall()
    companies = [dict(r) for r in comp_q]

    conn.close()
    return jsonify({'years': years, 'quarters': quarters,
                    'indicators': indicators, 'companies': companies})


@app.route('/api/screener')
def api_screener():
    period_type  = request.args.get('period_type', 'A')
    year         = request.args.get('year', '')
    quarter      = request.args.get('quarter', '')
    filters_json = request.args.get('filters', '[]')
    search       = request.args.get('search', '').strip().lower()

    try:
        filters = json.loads(filters_json)
    except Exception:
        filters = []

    conn = get_db()

    # ── Date filter (always capped at 2024) ──────────────────────
    if year:
        year_cap = min(str(year), '2024')
        if period_type == 'A':
            date_filter = "AND strftime('%Y', ff.report_date) = ? AND ff.period_type = 'A'"
            params = [year_cap]
        else:
            date_filter = "AND strftime('%Y', ff.report_date) = ? AND ff.fiscal_quarter = ? AND ff.period_type = 'Q'"
            params = [year_cap, int(quarter) if quarter else 1]
    else:
        year_cap = '2024'          # default cap khi không truyền năm
        date_filter = "AND ff.period_type = ?"
        params = [period_type]

    # ── Query financial facts (hard cap ≤ 2024) ──────────────────
    sql_ff = f"""
        SELECT ff.company_id, di.indicator_name, ff.value_numeric, ff.report_date
        FROM fact_financial ff
        JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
        WHERE strftime('%Y', ff.report_date) <= '2024'
        {date_filter}
    """
    ff_df = pd.read_sql_query(sql_ff, conn, params=params)

    # ── All companies (LEFT JOIN baseline) ───────────────────────
    comp_df = pd.read_sql_query("""
        SELECT dc.company_id, dc.ticker, dc.company_name, dc.exchange, dc.listing_date,
               dg.gics_industry, dg.gics_sector
        FROM dim_company dc
        LEFT JOIN dim_gics dg ON dc.gics_industry_id = dg.gics_industry_id
    """, conn)

    # ── Price data – ALL history, INDEPENDENT of time filter ─────
    price_df = pd.read_sql_query("""
        SELECT company_id, trading_date, price_close, price_open, price_high, price_low, volume
        FROM fact_market_price
        ORDER BY company_id, trading_date
    """, conn)
    conn.close()

    # ── Pivot: sort by date → aggfunc='last' = most recent ───────
    if not ff_df.empty:
        ff_df = ff_df.sort_values('report_date')
        pivot_df = ff_df.pivot_table(
            index='company_id', columns='indicator_name',
            values='value_numeric', aggfunc='last'
        ).reset_index()
        pivot_df.columns.name = None
    else:
        pivot_df = pd.DataFrame(columns=['company_id'])

    # ── Merge (LEFT JOIN) ─────────────────────────────────────────
    result_df = comp_df.merge(pivot_df, on='company_id', how='left')

    # ── EPS_4Q alias ─────────────────────────────────────────────
    result_df['EPS_4Q'] = np.nan
    for try_col in ['EPS (Proxy TTM)', 'EPS TTM', 'EPS cơ bản']:
        if try_col in result_df.columns:
            result_df['EPS_4Q'] = result_df['EPS_4Q'].fillna(
                pd.to_numeric(result_df[try_col], errors='coerce'))

    # ── Fundamental rating – lấy dữ liệu theo filter thời gian ──────
    conn2 = get_db()

    # 1. ROE từ pre-computed indicator (nhanh, chính xác)
    roe_col = 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)'
    if period_type == 'A':
        roe_ff = pd.read_sql_query("""
            SELECT ff.company_id, ff.value_numeric, ff.report_date
            FROM fact_financial ff
            JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
            WHERE strftime('%Y', ff.report_date) <= ?
              AND ff.period_type = 'A'
              AND di.indicator_name = ?
            ORDER BY ff.company_id, ff.report_date
        """, conn2, params=[year_cap, roe_col])
    else:
        # Kỳ quý: lấy ROE của quý được chọn (hoặc gần nhất ≤ năm đó)
        roe_ff = pd.read_sql_query("""
            SELECT ff.company_id, ff.value_numeric, ff.report_date
            FROM fact_financial ff
            JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
            WHERE strftime('%Y', ff.report_date) <= ?
              AND di.indicator_name = ?
            ORDER BY ff.company_id, ff.report_date
        """, conn2, params=[year_cap, roe_col])

    # 2. Profit_Growth, Net_Margin, Leverage từ raw indicators
    raw_metrics = compute_raw_fundamentals(conn2, max_year=year_cap)
    conn2.close()

    # Tổng hợp vào rating_base
    rating_base = comp_df[['company_id']].copy()

    # Merge ROE (latest per company)
    if not roe_ff.empty:
        roe_latest = (roe_ff.sort_values('report_date')
                           .groupby('company_id')['value_numeric']
                           .last().reset_index()
                           .rename(columns={'value_numeric': roe_col}))
        rating_base = rating_base.merge(roe_latest, on='company_id', how='left')

    # Merge Profit_Growth, Net_Margin, Leverage
    if not raw_metrics.empty:
        rating_base = rating_base.merge(raw_metrics, on='company_id', how='left')

    # Tính điểm và xếp hạng
    rating_base = compute_fundamental_rating(rating_base)

    # Merge rating, rank_pct, Điểm cơ bản vào result_df
    merge_cols = [c for c in ['company_id', 'rating', 'rank_pct', 'Điểm cơ bản']
                  if c in rating_base.columns]
    result_df = result_df.merge(rating_base[merge_cols], on='company_id', how='left')

    if 'rating' not in result_df.columns:
        result_df['rating']       = '–'
        result_df['rank_pct']     = np.nan
        result_df['Điểm cơ bản'] = np.nan

    # ── Price changes + OHLCV (luôn từ ngày mới nhất, kể cả 2026) ─
    if not price_df.empty:
        price_changes = compute_price_changes(price_df)
        if not price_changes.empty:
            result_df = result_df.merge(price_changes, on='company_id', how='left')

        # MACD, RSI, trade_signal – CỐ ĐỊNH tại ngày mới nhất (kể cả 2026)
        # Không lọc theo năm/quý
        tech_df = compute_technicals(price_df)
        if not tech_df.empty:
            result_df = result_df.merge(tech_df, on='company_id', how='left')

    # ── Search ────────────────────────────────────────────────────
    if search:
        mask = (
            result_df['ticker'].str.lower().str.contains(search, na=False) |
            result_df['company_name'].str.lower().str.contains(search, na=False)
        )
        result_df = result_df[mask]

    # ── Numeric filters ───────────────────────────────────────────
    for f in filters:
        ind = f.get('indicator')
        op  = f.get('op', '>=')
        v1  = f.get('val1')
        v2  = f.get('val2')
        if v1 is None or ind not in result_df.columns:
            continue
        col = pd.to_numeric(result_df[ind], errors='coerce')
        try:
            v1 = float(v1)
            if   op == '>':       mask = col.notna() & (col > v1)
            elif op == '>=':      mask = col.notna() & (col >= v1)
            elif op == '<':       mask = col.notna() & (col < v1)
            elif op == '<=':      mask = col.notna() & (col <= v1)
            elif op == '=':       mask = col.notna() & (col == v1)
            elif op == 'between' and v2 is not None:
                mask = col.notna() & (col >= v1) & (col <= float(v2))
            elif op == 'top_n':
                mask = col.notna() & (col >= col.nlargest(int(v1)).min())
            elif op == 'bottom_n':
                mask = col.notna() & (col <= col.nsmallest(int(v1)).max())
            else:
                continue
            result_df = result_df[mask]
        except Exception:
            pass

    # ── Finalise ──────────────────────────────────────────────────
    for c in result_df.select_dtypes(include=[np.floating, float]).columns:
        result_df[c] = result_df[c].round(4)
    result_df = result_df.drop(columns=['company_id'], errors='ignore')

    rows     = safe_json_rows(result_df)
    fin_cols = [c for c in result_df.columns if c not in
                ['ticker', 'company_name', 'exchange', 'listing_date',
                 'gics_industry', 'gics_sector']]

    return jsonify({'rows': rows, 'total': len(rows), 'fin_cols': fin_cols})


@app.route('/api/export')
def api_export():
    show_cols  = request.args.getlist('show_cols[]')
    col_labels_json = request.args.get('col_labels', '{}')
    try:
        col_labels = json.loads(col_labels_json)
    except Exception:
        col_labels = {}

    resp = api_screener()
    data = resp.get_json()
    rows = data.get('rows', [])
    if not rows:
        return jsonify({'error': 'Không có dữ liệu'}), 400

    df = pd.DataFrame(rows)

    # Lọc đúng các cột đang hiển thị
    if show_cols:
        available = [c for c in show_cols if c in df.columns]
        if available:
            df = df[available]

    # Đổi tên sang label tiếng Việt (từ frontend gửi lên)
    if col_labels:
        df = df.rename(columns=col_labels)

    df = df.fillna('–')

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Screener')
    output.seek(0)

    ts = datetime.now().strftime('%Y%m%d_%H%M')
    return send_file(output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'icelands_screener_{ts}.xlsx')


@app.route('/api/autocomplete')
def api_autocomplete():
    q = request.args.get('q', '').strip().lower()
    if not q:
        return jsonify([])
    df = df_from_query("""
        SELECT dc.ticker, dc.company_name, dc.exchange, dg.gics_sector
        FROM dim_company dc
        LEFT JOIN dim_gics dg ON dc.gics_industry_id = dg.gics_industry_id
        ORDER BY dc.ticker
    """)
    mask = (
        df['ticker'].str.lower().str.contains(q, na=False) |
        df['company_name'].str.lower().str.contains(q, na=False)
    )
    return jsonify(df[mask].head(15).to_dict(orient='records'))


@app.route('/api/export_strategy')
def api_export_strategy():
    """Export Excel cho kết quả của một chiến lược cụ thể."""
    show_cols  = request.args.getlist('show_cols[]')
    col_labels_json = request.args.get('col_labels', '{}')
    try:
        col_labels = json.loads(col_labels_json)
    except Exception:
        col_labels = {}

    resp = api_strategy()
    data = resp.get_json()
    rows = data.get('rows', [])
    if not rows:
        return jsonify({'error': 'Không có dữ liệu'}), 400

    df = pd.DataFrame(rows)
    if show_cols:
        available = [c for c in show_cols if c in df.columns]
        if available:
            df = df[available]
    if col_labels:
        df = df.rename(columns=col_labels)

    df = df.fillna('–')
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Strategy')
    output.seek(0)

    strategy_key = request.args.get('strategy', 'strategy')
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    return send_file(output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'icelands_{strategy_key}_{ts}.xlsx')


@app.route('/api/strategy')
def api_strategy():
    """
    Chạy 1 chiến lược đầu tư và trả về kết quả.
    - strategy : quality | garp | value | dividend | health | efficiency | cashflow
    - filters  : JSON array – bộ lọc tùy chỉnh của user (áp dụng TRÊN strategy)
    - search   : tìm theo mã / tên
    - Thời gian luôn cố định năm=2024, period_type=A
    """
    strategy_key = request.args.get('strategy', 'quality')
    filters_json = request.args.get('filters', '[]')
    search       = request.args.get('search', '').strip().lower()

    try:
        filters = json.loads(filters_json)
    except Exception:
        filters = []

    if strategy_key not in STRATEGIES:
        return jsonify({'error': f'Unknown strategy: {strategy_key}'}), 400

    # ── Chạy strategy (luôn dùng năm 2024, period A) ──────────
    try:
        strategy  = STRATEGIES[strategy_key]
        loader    = DBDataLoader(DB_PATH, year='2024', period_type='A')
        ok = loader.load()
        if not ok:
            return jsonify({'error': 'Không thể load dữ liệu'}), 500

        strat_results = strategy.screen(loader)
        badges        = strategy.get_badge_criteria(strat_results)
        preset_cols   = getattr(strategy, 'PRESET_COLS', [])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # ── Lấy danh sách mã đạt tiêu chí strategy ────────────────
    passed_tickers = {t for t, v in strat_results.items() if v['passes']}

    # ── Lấy financial data tương tự screener (năm 2024, A) ────
    conn = get_db()

    sql_ff = """
        SELECT ff.company_id, di.indicator_name, ff.value_numeric, ff.report_date
        FROM fact_financial ff
        JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
        WHERE strftime('%Y', ff.report_date) <= '2024'
          AND ff.period_type = 'A'
    """
    ff_df = pd.read_sql_query(sql_ff, conn)

    comp_df = pd.read_sql_query("""
        SELECT dc.company_id, dc.ticker, dc.company_name, dc.exchange, dc.listing_date,
               dg.gics_industry, dg.gics_sector
        FROM dim_company dc
        LEFT JOIN dim_gics dg ON dc.gics_industry_id = dg.gics_industry_id
    """, conn)

    price_df = pd.read_sql_query("""
        SELECT company_id, trading_date, price_close, price_open, price_high, price_low, volume
        FROM fact_market_price
        ORDER BY company_id, trading_date
    """, conn)
    conn.close()

    # ── Pivot financial indicators ─────────────────────────────
    if not ff_df.empty:
        ff_df = ff_df.sort_values('report_date')
        pivot_df = ff_df.pivot_table(
            index='company_id', columns='indicator_name',
            values='value_numeric', aggfunc='last'
        ).reset_index()
        pivot_df.columns.name = None
    else:
        pivot_df = pd.DataFrame(columns=['company_id'])

    result_df = comp_df.merge(pivot_df, on='company_id', how='left')

    # ── EPS alias ──────────────────────────────────────────────
    result_df['EPS_4Q'] = np.nan
    for try_col in ['EPS (Proxy TTM)', 'EPS TTM', 'EPS cơ bản']:
        if try_col in result_df.columns:
            result_df['EPS_4Q'] = result_df['EPS_4Q'].fillna(
                pd.to_numeric(result_df[try_col], errors='coerce'))

    # ── Filter: chỉ giữ mã đạt strategy ──────────────────────
    result_df = result_df[result_df['ticker'].isin(passed_tickers)]

    # ── Price changes + OHLCV ─────────────────────────────────
    if not price_df.empty:
        price_changes = compute_price_changes(price_df)
        if not price_changes.empty:
            result_df = result_df.merge(price_changes, on='company_id', how='left')
        tech_df = compute_technicals(price_df)
        if not tech_df.empty:
            result_df = result_df.merge(tech_df, on='company_id', how='left')

    # ── Search ────────────────────────────────────────────────
    if search:
        mask = (
            result_df['ticker'].str.lower().str.contains(search, na=False) |
            result_df['company_name'].str.lower().str.contains(search, na=False)
        )
        result_df = result_df[mask]

    # ── User custom numeric filters (on TOP of strategy) ──────
    for f in filters:
        ind = f.get('indicator')
        op  = f.get('op', '>=')
        v1  = f.get('val1')
        v2  = f.get('val2')
        if v1 is None or ind not in result_df.columns:
            continue
        col = pd.to_numeric(result_df[ind], errors='coerce')
        try:
            v1 = float(v1)
            if   op == '>':       mask = col.notna() & (col > v1)
            elif op == '>=':      mask = col.notna() & (col >= v1)
            elif op == '<':       mask = col.notna() & (col < v1)
            elif op == '<=':      mask = col.notna() & (col <= v1)
            elif op == '=':       mask = col.notna() & (col == v1)
            elif op == 'between' and v2 is not None:
                mask = col.notna() & (col >= v1) & (col <= float(v2))
            elif op == 'top_n':
                mask = col.notna() & (col >= col.nlargest(int(v1)).min())
            elif op == 'bottom_n':
                mask = col.notna() & (col <= col.nsmallest(int(v1)).max())
            else:
                continue
            result_df = result_df[mask]
        except Exception:
            pass

    # ── Finalise ──────────────────────────────────────────────
    for c in result_df.select_dtypes(include=[np.floating, float]).columns:
        result_df[c] = result_df[c].round(4)
    result_df = result_df.drop(columns=['company_id'], errors='ignore')

    rows = safe_json_rows(result_df)
    fin_cols = [c for c in result_df.columns if c not in
                ['ticker', 'company_name', 'exchange', 'listing_date',
                 'gics_industry', 'gics_sector']]

    return jsonify({
        'rows'        : rows,
        'total'       : len(rows),
        'fin_cols'    : fin_cols,
        'strategy_key': strategy_key,
        'badges'      : badges,
        'preset_cols' : preset_cols,
    })


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Iceland Stocks Screener – Flask Backend")
    print(f"   Database: {DB_PATH}")
    print("   URL: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, port=5000)

