"""
BỘ LỌC CỔ PHIẾU — 7 CHIẾN LƯỢC ĐẦU TƯ
=========================================
Mỗi chiến lược có tiêu chí định lượng từ lý thuyết học thuật cụ thể.

CHIẾN LƯỢC 1 — CHẤT LƯỢNG (Quality Investing)
  Lý thuyết: Buffett (1984) + Greenblatt Magic Formula (2005)
  Tiêu chí : ROE ≥ 15% liên tục 3 năm | ROA ≥ 8% | ROCE ≥ 12%

CHIẾN LƯỢC 2 — TĂNG TRƯỞNG (GARP — Growth at a Reasonable Price)
  Lý thuyết: Peter Lynch (1989) — "One Up on Wall Street"
              PEG Ratio = P/E ÷ EPS Growth Rate ≤ 1.0
  Tiêu chí : Doanh thu tăng ≥ 10%/năm | Lợi nhuận tăng ≥ 15%/năm | P/E hợp lý

CHIẾN LƯỢC 3 — ĐỊNH GIÁ THẤP (Value Investing)
  Lý thuyết: Benjamin Graham (1949) — "The Intelligent Investor"
              Margin of Safety: mua khi giá < giá trị nội tại
  Tiêu chí : P/E ≤ 15 | P/B ≤ 1.5 | EV/EBITDA ≤ 10

CHIẾN LƯỢC 4 — CỔ TỨC CAO (Dividend Investing)
  Lý thuyết: Gordon Growth Model (Gordon & Shapiro, 1956)
              P = D₁ / (r − g) → cổ tức bền vững = giá trị bền vững
  Tiêu chí : Tỷ suất cổ tức ≥ 4% | CFO dương liên tục ≥ 3 năm | ICR ≥ 3

CHIẾN LƯỢC 5 — SỨC KHỎE TÀI CHÍNH (Financial Health)
  Lý thuyết: Piotroski F-Score (2000) — Journal of Accounting Research
              9 tiêu chí nhị phân đo lường sức khỏe tài chính toàn diện
  Tiêu chí : Current Ratio ≥ 1.5 | ICR ≥ 3 | Nợ/VCS ≤ 1.0 | CFO/Nợ NH cao

CHIẾN LƯỢC 6 — HIỆU QUẢ HOẠT ĐỘNG (Operational Efficiency)
  Lý thuyết: DuPont Analysis (Donaldson Brown, 1920) — phân tích ROE = Biên LN × Vòng quay TS × Đòn bẩy
              Vòng quay tài sản cao = dùng tài sản hiệu quả = lợi thế cạnh tranh vận hành
  Tiêu chí : Vòng quay TS ≥ 0.5 | Vòng quay VCSH ≥ 1.0 | Biên gộp ≥ 20% | DSO ≤ 60 ngày

CHIẾN LƯỢC 7 — DÒNG TIỀN BỀN VỮNG (Cash Flow Quality)
  Lý thuyết: Sloan (1996) — "Do Stock Prices Fully Reflect Information in Accruals?"
              Journal of Accounting Research — công ty ít dồn tích (accrual thấp) có giá CP bền hơn
  Tiêu chí : CFO/Lợi nhuận thuần ≥ 0.8 | Tỷ lệ dồn tích ≤ 0.05 | CFO/Tổng TS ≥ 5%


"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import warnings
import json
import sqlite3
from datetime import datetime

warnings.filterwarnings('ignore')


# ================================================================
# DB DATA LOADER — đọc từ SQLite Financial_Data.sqlite
# ================================================================

class DBDataLoader:
    """
    Load và pivot dữ liệu từ SQLite database sang dạng wide-format
    để các chiến lược dễ truy xuất.

    Output chính:
        self.annual  : DataFrame wide — index=Mã CK, columns=chỉ số (của kỳ được chọn)
        self.history : Dict[chỉ_số] → Dict[mã_CK, List[(năm, giá_trị)]]
                       dùng để kiểm tra tính nhất quán nhiều năm
    """

    def __init__(
        self,
        db_path: str,
        year: str = '2024',
        period_type: str = 'A',
        quarter: Optional[int] = None,
    ):
        self.db_path     = db_path
        self.year        = str(year)
        self.period_type = period_type
        self.quarter     = quarter

        self.annual  : pd.DataFrame = pd.DataFrame()
        self.history : Dict[str, pd.DataFrame] = {}
        self._stocks : List[str] = []

    # ----------------------------------------------------------
    def load(self) -> bool:
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            # ── Lấy tất cả mã cổ phiếu ──
            comp_df = pd.read_sql_query("""
                SELECT dc.company_id, dc.ticker
                FROM dim_company dc
                ORDER BY dc.ticker
            """, conn)

            # ── Build date filter ──
            if self.period_type == 'A':
                # Năm: lấy tất cả records với period_type='A' và năm <= year
                sql = """
                    SELECT dc.ticker AS stock, di.indicator_name, ff.value_numeric, ff.report_date,
                           CAST(strftime('%Y', ff.report_date) AS INTEGER) AS year
                    FROM fact_financial ff
                    JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
                    JOIN dim_company   dc ON ff.company_id   = dc.company_id
                    WHERE ff.period_type = 'A'
                      AND strftime('%Y', ff.report_date) <= ?
                    ORDER BY dc.ticker, di.indicator_name, ff.report_date
                """
                params = [self.year]
            else:
                # Quý cụ thể hoặc lấy tất cả quý trong năm
                if self.quarter:
                    sql = """
                        SELECT dc.ticker AS stock, di.indicator_name, ff.value_numeric, ff.report_date,
                               CAST(strftime('%Y', ff.report_date) AS INTEGER) AS year
                        FROM fact_financial ff
                        JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
                        JOIN dim_company   dc ON ff.company_id   = dc.company_id
                        WHERE ff.period_type = 'Q'
                          AND strftime('%Y', ff.report_date) <= ?
                          AND ff.fiscal_quarter = ?
                        ORDER BY dc.ticker, di.indicator_name, ff.report_date
                    """
                    params = [self.year, self.quarter]
                else:
                    sql = """
                        SELECT dc.ticker AS stock, di.indicator_name, ff.value_numeric, ff.report_date,
                               CAST(strftime('%Y', ff.report_date) AS INTEGER) AS year
                        FROM fact_financial ff
                        JOIN dim_indicator di ON ff.indicator_id = di.indicator_id
                        JOIN dim_company   dc ON ff.company_id   = dc.company_id
                        WHERE ff.period_type = 'Q'
                          AND strftime('%Y', ff.report_date) <= ?
                        ORDER BY dc.ticker, di.indicator_name, ff.report_date
                    """
                    params = [self.year]

            raw_df = pd.read_sql_query(sql, conn, params=params)
            conn.close()

            if raw_df.empty:
                print("✗ Không có dữ liệu trong database")
                return False

            raw_df['value_numeric'] = pd.to_numeric(raw_df['value_numeric'], errors='coerce')

            # ── Lấy giá trị mới nhất (sort theo report_date, lấy last) ──
            latest = (
                raw_df.sort_values('report_date')
                      .groupby(['stock', 'indicator_name'])
                      .last()
                      .reset_index()[['stock', 'indicator_name', 'value_numeric']]
            )
            self.annual = latest.pivot(
                index='stock', columns='indicator_name', values='value_numeric'
            )
            self.annual.columns.name = None

            # ── Lưu lịch sử từng chỉ số theo năm ──
            for indicator in raw_df['indicator_name'].unique():
                sub = raw_df[raw_df['indicator_name'] == indicator]
                pivot_hist = sub.pivot_table(
                    index='stock', columns='year', values='value_numeric', aggfunc='last'
                )
                self.history[indicator] = pivot_hist

            self._stocks = list(self.annual.index)

            print(f"✓ Loaded {len(self._stocks)} mã, {len(self.annual.columns)} chỉ số")
            print(f"✓ Lịch sử: {len(self.history)} chỉ số có chuỗi thời gian")
            return True

        except Exception as e:
            print(f"✗ Lỗi load dữ liệu: {e}")
            import traceback; traceback.print_exc()
            return False

    # ----------------------------------------------------------
    def get(self, stock: str, indicator: str) -> float:
        """Lấy giá trị mới nhất của 1 chỉ số cho 1 mã."""
        try:
            return float(self.annual.loc[stock, indicator])
        except Exception:
            return np.nan

    def get_history(self, stock: str, indicator: str, n_years: int = 3) -> List[float]:
        """Lấy n năm gần nhất của 1 chỉ số cho 1 mã."""
        try:
            if indicator not in self.history:
                return []
            row = self.history[indicator].loc[stock]
            vals = row.sort_index().dropna().tail(n_years).tolist()
            return vals
        except Exception:
            return []

    @property
    def stocks(self) -> List[str]:
        return self._stocks


# ================================================================
# CHIẾN LƯỢC 1 — CHẤT LƯỢNG
# Buffett (1984) + Greenblatt Magic Formula (2005)
# ================================================================

class QualityStrategy:
    """
    Tiêu chí lọc (phải đạt TẤT CẢ):
    ┌──────────────────────────────────────┬──────────────┬──────────────────────────────────────┐
    │ Tiêu chí                             │ Ngưỡng       │ Cơ sở                                │
    ├──────────────────────────────────────┼──────────────┼──────────────────────────────────────┤
    │ ROE liên tục 3 năm gần nhất          │ ≥ 15%        │ Buffett: ROE bền vững = lợi thế cạnh │
    │                                      │              │ tranh (competitive moat)             │
    │ ROA (năm gần nhất)                   │ ≥ 8%         │ Greenblatt: sinh lời trên tài sản    │
    │ ROCE (năm gần nhất)                  │ ≥ 12%        │ Greenblatt: Return on Capital ≥ 12%  │
    └──────────────────────────────────────┴──────────────┴──────────────────────────────────────┘

    Thang điểm (0–100):
      ROE score  = min(ROE_avg / 0.15, 1) × 40     (40đ — chỉ số quan trọng nhất)
      ROA score  = min(ROA     / 0.08, 1) × 30     (30đ)
      ROCE score = min(ROCE    / 0.12, 1) × 30     (30đ)
    """

    NAME = "Chất lượng (Quality)"
    KEY  = "quality"
    THRESHOLDS = {
        'roe_min'   : 0.15,   # Buffett: ROE ≥ 15%
        'roa_min'   : 0.08,   # Greenblatt: ROA ≥ 8%
        'roce_min'  : 0.12,   # Greenblatt: ROCE ≥ 12%
        'roe_years' : 3,      # số năm ROE liên tục đạt ngưỡng
    }

    CRITERIA_LABELS = [
        ("ROE≥15%",   "roe_min",  0.15, "roe_avg_3y",  True,  100),
        ("ROA≥8%",    "roa_min",  0.08, "roa",         True,  100),
        ("ROCE≥12%",  "roce_min", 0.12, "roce",        True,  100),
    ]

    COL_ROE  = 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)'
    COL_ROA  = 'Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)'
    COL_ROCE = 'Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)'

    # Preset columns for UI table
    PRESET_COLS = [
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
        'Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)',
        'Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)',
        'P/E',
        'P/B',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            roe_hist = data.get_history(stock, self.COL_ROE, self.THRESHOLDS['roe_years'])
            roa      = data.get(stock, self.COL_ROA)
            roce     = data.get(stock, self.COL_ROCE)

            roe_consistent = (
                len(roe_hist) >= self.THRESHOLDS['roe_years']
                and all(r >= self.THRESHOLDS['roe_min'] for r in roe_hist)
            )
            roe_avg = np.mean(roe_hist) if roe_hist else np.nan

            passes = (
                roe_consistent
                and pd.notna(roa)  and roa  >= self.THRESHOLDS['roa_min']
                and pd.notna(roce) and roce >= self.THRESHOLDS['roce_min']
            )

            roe_score  = min(roe_avg / self.THRESHOLDS['roe_min'],  1) * 40 if pd.notna(roe_avg)  else 0
            roa_score  = min(roa     / self.THRESHOLDS['roa_min'],  1) * 30 if pd.notna(roa)      else 0
            roce_score = min(roce    / self.THRESHOLDS['roce_min'], 1) * 30 if pd.notna(roce)     else 0
            total      = round(roe_score + roa_score + roce_score, 2)

            results[stock] = {
                'passes'          : passes,
                'score'           : total,
                'roe_avg_3y'      : round(roe_avg * 100, 2) if pd.notna(roe_avg) else None,
                'roe_history'     : [round(r * 100, 2) for r in roe_hist],
                'roe_consistent'  : roe_consistent,
                'roa'             : round(roa  * 100, 2) if pd.notna(roa)  else None,
                'roce'            : round(roce * 100, 2) if pd.notna(roce) else None,
                'reason'          : self._reason(roe_consistent, roe_avg, roa, roce),
            }
        return results

    def _reason(self, roe_ok, roe_avg, roa, roce) -> str:
        parts = []
        if pd.notna(roe_avg):
            tag = '✅' if roe_ok else '❌'
            parts.append(f"{tag} ROE 3Y avg={roe_avg*100:.1f}% (ngưỡng ≥15%)")
        if pd.notna(roa):
            tag = '✅' if roa >= 0.08 else '❌'
            parts.append(f"{tag} ROA={roa*100:.1f}% (≥8%)")
        if pd.notna(roce):
            tag = '✅' if roce >= 0.12 else '❌'
            parts.append(f"{tag} ROCE={roce*100:.1f}% (≥12%)")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        passed = sum(1 for v in results.values() if v['passes'])
        return [
            {"label": "ROE≥15% 3Y", "count": passed},
            {"label": "ROA≥8%",     "count": sum(1 for v in results.values() if v.get('roa') is not None and v['roa'] >= 8)},
            {"label": "ROCE≥12%",   "count": sum(1 for v in results.values() if v.get('roce') is not None and v['roce'] >= 12)},
        ]


# ================================================================
# CHIẾN LƯỢC 2 — TĂNG TRƯỞNG (GARP)
# Peter Lynch (1989) — PEG Ratio ≤ 1.0
# ================================================================

class GARPStrategy:
    NAME = "Tăng trưởng — GARP"
    KEY  = "garp"
    THRESHOLDS = {
        'rev_growth_min' : 0.10,
        'pft_growth_min' : 0.15,
        'pe_max'         : 30,
    }

    COL_REV = 'Tăng trưởng doanh thu thuần'
    COL_PFT = 'Tăng trưởng lợi nhuận sau thuế của CĐ công ty mẹ'
    COL_PE  = 'P/E'

    PRESET_COLS = [
        'Tăng trưởng doanh thu thuần',
        'Tăng trưởng lợi nhuận sau thuế của CĐ công ty mẹ',
        'P/E',
        'EPS (Proxy TTM)',
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            rev_growth = data.get(stock, self.COL_REV)
            pft_growth = data.get(stock, self.COL_PFT)
            pe         = data.get(stock, self.COL_PE)

            passes = (
                pd.notna(rev_growth) and rev_growth >= self.THRESHOLDS['rev_growth_min']
                and pd.notna(pft_growth) and pft_growth >= self.THRESHOLDS['pft_growth_min']
                and pd.notna(pe) and 0 < pe <= self.THRESHOLDS['pe_max']
            )

            rev_score = min(rev_growth / self.THRESHOLDS['rev_growth_min'], 2) * 25 if pd.notna(rev_growth) and rev_growth > 0 else 0
            pft_score = min(pft_growth / self.THRESHOLDS['pft_growth_min'], 2) * 25 if pd.notna(pft_growth) and pft_growth > 0 else 0
            pe_bonus  = 10 if pd.notna(pe) and 0 < pe <= 20 else 0
            total     = round(min(rev_score + pft_score + pe_bonus, 100), 2)

            results[stock] = {
                'passes'     : passes,
                'score'      : total,
                'rev_growth' : round(rev_growth * 100, 2) if pd.notna(rev_growth) else None,
                'pft_growth' : round(pft_growth * 100, 2) if pd.notna(pft_growth) else None,
                'pe'         : round(pe, 2)               if pd.notna(pe)          else None,
                'reason'     : self._reason(rev_growth, pft_growth, pe),
            }
        return results

    def _reason(self, rev, pft, pe) -> str:
        parts = []
        if pd.notna(rev):
            tag = '✅' if rev >= 0.10 else '❌'
            parts.append(f"{tag} DT tăng={rev*100:.1f}% (≥10%)")
        if pd.notna(pft):
            tag = '✅' if pft >= 0.15 else '❌'
            parts.append(f"{tag} LN tăng={pft*100:.1f}% (≥15%)")
        if pd.notna(pe):
            tag = '✅' if 0 < pe <= 30 else '❌'
            parts.append(f"{tag} P/E={pe:.1f} (≤30)")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        return [
            {"label": "DT tăng≥10%", "count": sum(1 for v in results.values() if v.get('rev_growth') is not None and v['rev_growth'] >= 10)},
            {"label": "LN tăng≥15%", "count": sum(1 for v in results.values() if v.get('pft_growth') is not None and v['pft_growth'] >= 15)},
            {"label": "P/E≤30",      "count": sum(1 for v in results.values() if v.get('pe') is not None and 0 < v['pe'] <= 30)},
        ]


# ================================================================
# CHIẾN LƯỢC 3 — ĐỊNH GIÁ THẤP (Value Investing)
# Benjamin Graham (1949) — "The Intelligent Investor"
# ================================================================

class ValueStrategy:
    NAME = "Định giá thấp (Value)"
    KEY  = "value"
    THRESHOLDS = {
        'pe_max'    : 15,
        'pb_max'    : 1.5,
        'ev_eb_max' : 10,
    }

    COL_PE   = 'P/E'
    COL_PB   = 'P/B'
    COL_EVEB = 'EV/EBITDA'

    PRESET_COLS = [
        'P/E', 'P/B', 'EV/EBITDA', 'EV/EBIT', 'P/S',
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            pe   = data.get(stock, self.COL_PE)
            pb   = data.get(stock, self.COL_PB)
            eveb = data.get(stock, self.COL_EVEB)

            passes = (
                pd.notna(pe)   and 0 < pe   <= self.THRESHOLDS['pe_max']
                and pd.notna(pb)   and 0 < pb   <= self.THRESHOLDS['pb_max']
                and pd.notna(eveb) and 0 < eveb <= self.THRESHOLDS['ev_eb_max']
            )

            pe_score   = max(1 - pe   / self.THRESHOLDS['pe_max'],   0) * 40 if pd.notna(pe)   and pe   > 0 else 0
            pb_score   = max(1 - pb   / self.THRESHOLDS['pb_max'],   0) * 30 if pd.notna(pb)   and pb   > 0 else 0
            eveb_score = max(1 - eveb / self.THRESHOLDS['ev_eb_max'],0) * 30 if pd.notna(eveb) and eveb > 0 else 0
            total      = round(pe_score + pb_score + eveb_score, 2)

            results[stock] = {
                'passes'    : passes,
                'score'     : total,
                'pe'        : round(pe,   2) if pd.notna(pe)   else None,
                'pb'        : round(pb,   2) if pd.notna(pb)   else None,
                'ev_ebitda' : round(eveb, 2) if pd.notna(eveb) else None,
                'reason'    : self._reason(pe, pb, eveb),
            }
        return results

    def _reason(self, pe, pb, eveb) -> str:
        parts = []
        if pd.notna(pe):
            tag = '✅' if 0 < pe <= 15 else '❌'
            parts.append(f"{tag} P/E={pe:.1f} (≤15)")
        if pd.notna(pb):
            tag = '✅' if 0 < pb <= 1.5 else '❌'
            parts.append(f"{tag} P/B={pb:.2f} (≤1.5)")
        if pd.notna(eveb):
            tag = '✅' if 0 < eveb <= 10 else '❌'
            parts.append(f"{tag} EV/EBITDA={eveb:.1f} (≤10)")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        return [
            {"label": "P/E≤15",       "count": sum(1 for v in results.values() if v.get('pe') is not None and 0 < v['pe'] <= 15)},
            {"label": "P/B≤1.5",      "count": sum(1 for v in results.values() if v.get('pb') is not None and 0 < v['pb'] <= 1.5)},
            {"label": "EV/EBITDA≤10", "count": sum(1 for v in results.values() if v.get('ev_ebitda') is not None and 0 < v['ev_ebitda'] <= 10)},
        ]


# ================================================================
# CHIẾN LƯỢC 4 — CỔ TỨC CAO (Dividend Investing)
# Gordon & Shapiro (1956) — Gordon Growth Model
# ================================================================

class DividendStrategy:
    NAME = "Cổ tức cao (Dividend)"
    KEY  = "dividend"
    THRESHOLDS = {
        'div_yield_min' : 0.04,
        'icr_min'       : 3.0,
        'cfo_years'     : 3,
    }

    COL_DIV = 'Tỷ suất cổ tức'
    COL_ICR = 'Khả năng thanh toán lãi vay (ICR)'
    COL_CFO = 'CFO / Doanh thu thuần'

    PRESET_COLS = [
        'Tỷ suất cổ tức',
        'Khả năng thanh toán lãi vay (ICR)',
        'CFO / Doanh thu thuần',
        'P/E', 'P/B',
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            div_yield = data.get(stock, self.COL_DIV)
            icr       = data.get(stock, self.COL_ICR)
            cfo_hist  = data.get_history(stock, self.COL_CFO, self.THRESHOLDS['cfo_years'])

            cfo_positive_years = sum(1 for c in cfo_hist if c > 0)
            cfo_ok = cfo_positive_years >= self.THRESHOLDS['cfo_years']

            passes = (
                pd.notna(div_yield) and div_yield >= self.THRESHOLDS['div_yield_min']
                and pd.notna(icr)   and icr       >= self.THRESHOLDS['icr_min']
                and cfo_ok
            )

            div_score = min(div_yield / self.THRESHOLDS['div_yield_min'], 2.5) * 40 if pd.notna(div_yield) and div_yield > 0 else 0
            icr_score = min(icr / self.THRESHOLDS['icr_min'], 2) * 30               if pd.notna(icr) and icr > 0 else 0
            cfo_score = {3: 30, 2: 15, 1: 5}.get(cfo_positive_years, 0)
            total     = round(min(div_score + icr_score + cfo_score, 100), 2)

            results[stock] = {
                'passes'             : passes,
                'score'              : total,
                'div_yield_pct'      : round(div_yield * 100, 2) if pd.notna(div_yield) else None,
                'icr'                : round(icr, 2)              if pd.notna(icr)       else None,
                'cfo_positive_years' : cfo_positive_years,
                'cfo_history'        : [round(c, 4) for c in cfo_hist],
                'reason'             : self._reason(div_yield, icr, cfo_positive_years),
            }
        return results

    def _reason(self, div, icr, cfo_yr) -> str:
        parts = []
        if pd.notna(div):
            tag = '✅' if div >= 0.04 else '❌'
            parts.append(f"{tag} Cổ tức={div*100:.2f}% (≥4%)")
        if pd.notna(icr):
            tag = '✅' if icr >= 3 else '❌'
            parts.append(f"{tag} ICR={icr:.1f} (≥3)")
        tag = '✅' if cfo_yr >= 3 else '❌'
        parts.append(f"{tag} CFO dương {cfo_yr}/3 năm")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        return [
            {"label": "Cổ tức≥4%",  "count": sum(1 for v in results.values() if v.get('div_yield_pct') is not None and v['div_yield_pct'] >= 4)},
            {"label": "ICR≥3",       "count": sum(1 for v in results.values() if v.get('icr') is not None and v['icr'] >= 3)},
            {"label": "CFO dương 3Y","count": sum(1 for v in results.values() if v.get('cfo_positive_years', 0) >= 3)},
        ]


# ================================================================
# CHIẾN LƯỢC 5 — SỨC KHỎE TÀI CHÍNH
# Piotroski F-Score (2000) — Journal of Accounting Research
# ================================================================

class FinancialHealthStrategy:
    NAME = "Sức khỏe tài chính (Piotroski)"
    KEY  = "health"
    THRESHOLDS = {
        'current_ratio_min' : 1.5,
        'icr_min'           : 3.0,
        'debt_vcs_max'      : 1.5,
        'cfo_nh_min'        : 0.2,
    }

    COL_CR     = 'Tỷ số thanh toán hiện hành (Current Ratio)'
    COL_ICR    = 'Khả năng thanh toán lãi vay (ICR)'
    COL_DEBT   = 'Nợ (PT) / VCS'
    COL_CFO_NH = 'CFO / Nợ ngắn hạn'

    PRESET_COLS = [
        'Tỷ số thanh toán hiện hành (Current Ratio)',
        'Khả năng thanh toán lãi vay (ICR)',
        'Nợ (PT) / VCS',
        'CFO / Nợ ngắn hạn',
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
        'P/B',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            cr     = data.get(stock, self.COL_CR)
            icr    = data.get(stock, self.COL_ICR)
            debt   = data.get(stock, self.COL_DEBT)
            cfo_nh = data.get(stock, self.COL_CFO_NH)

            passes = (
                pd.notna(cr)     and cr     >= self.THRESHOLDS['current_ratio_min']
                and pd.notna(icr)    and icr    >= self.THRESHOLDS['icr_min']
                and pd.notna(debt)   and debt   <= self.THRESHOLDS['debt_vcs_max']
                and pd.notna(cfo_nh) and cfo_nh >= self.THRESHOLDS['cfo_nh_min']
            )

            cr_score     = min(cr     / self.THRESHOLDS['current_ratio_min'], 2) * 12.5 if pd.notna(cr)     and cr     > 0 else 0
            icr_score    = min(icr    / self.THRESHOLDS['icr_min'],           2) * 12.5 if pd.notna(icr)    and icr    > 0 else 0
            debt_score   = max(1 - debt / self.THRESHOLDS['debt_vcs_max'],    0) * 25   if pd.notna(debt)               else 0
            cfo_nh_score = min(cfo_nh / self.THRESHOLDS['cfo_nh_min'],        2) * 12.5 if pd.notna(cfo_nh) and cfo_nh > 0 else 0
            total        = round(min(cr_score + icr_score + debt_score + cfo_nh_score, 100), 2)

            results[stock] = {
                'passes'        : passes,
                'score'         : total,
                'current_ratio' : round(cr,     2) if pd.notna(cr)     else None,
                'icr'           : round(icr,    2) if pd.notna(icr)    else None,
                'debt_vcs'      : round(debt,   2) if pd.notna(debt)   else None,
                'cfo_nh'        : round(cfo_nh, 4) if pd.notna(cfo_nh) else None,
                'reason'        : self._reason(cr, icr, debt, cfo_nh),
            }
        return results

    def _reason(self, cr, icr, debt, cfo_nh) -> str:
        parts = []
        if pd.notna(cr):
            tag = '✅' if cr >= 1.5 else '❌'
            parts.append(f"{tag} Current={cr:.2f} (≥1.5)")
        if pd.notna(icr):
            tag = '✅' if icr >= 3 else '❌'
            parts.append(f"{tag} ICR={icr:.1f} (≥3)")
        if pd.notna(debt):
            tag = '✅' if debt <= 1.5 else '❌'
            parts.append(f"{tag} Nợ/VCS={debt:.2f} (≤1.5)")
        if pd.notna(cfo_nh):
            tag = '✅' if cfo_nh >= 0.2 else '❌'
            parts.append(f"{tag} CFO/NH={cfo_nh:.2f} (≥0.2)")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        return [
            {"label": "Current≥1.5", "count": sum(1 for v in results.values() if v.get('current_ratio') is not None and v['current_ratio'] >= 1.5)},
            {"label": "ICR≥3",       "count": sum(1 for v in results.values() if v.get('icr') is not None and v['icr'] >= 3)},
            {"label": "Nợ/VCS≤1.5",  "count": sum(1 for v in results.values() if v.get('debt_vcs') is not None and v['debt_vcs'] <= 1.5)},
            {"label": "CFO/NH≥0.2",  "count": sum(1 for v in results.values() if v.get('cfo_nh') is not None and v['cfo_nh'] >= 0.2)},
        ]


# ================================================================
# CHIẾN LƯỢC 6 — HIỆU QUẢ HOẠT ĐỘNG
# DuPont Analysis (Donaldson Brown, 1920)
# ================================================================

class OperationalEfficiencyStrategy:
    NAME = "Hiệu quả hoạt động (DuPont)"
    KEY  = "efficiency"
    THRESHOLDS = {
        'asset_turn_min'  : 0.5,
        'equity_turn_min' : 1.0,
        'gross_margin_min': 0.20,
        'dso_max'         : 90,
    }

    COL_AT  = 'Vòng quay tổng tài sản'
    COL_ET  = 'Vòng quay vốn chủ sở hữu'
    COL_GM  = 'Tỷ suất lợi nhuận gộp biên'
    COL_DSO = 'Thời gian thu tiền khách hàng bình quân (DSO)'

    PRESET_COLS = [
        'Vòng quay tổng tài sản',
        'Vòng quay vốn chủ sở hữu',
        'Tỷ suất lợi nhuận gộp biên',
        'Thời gian thu tiền khách hàng bình quân (DSO)',
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
        'P/E',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            at  = data.get(stock, self.COL_AT)
            et  = data.get(stock, self.COL_ET)
            gm  = data.get(stock, self.COL_GM)
            dso = data.get(stock, self.COL_DSO)

            passes = (
                pd.notna(at)  and at  >= self.THRESHOLDS['asset_turn_min']
                and pd.notna(et)  and et  >= self.THRESHOLDS['equity_turn_min']
                and pd.notna(gm)  and gm  >= self.THRESHOLDS['gross_margin_min']
                and pd.notna(dso) and dso <= self.THRESHOLDS['dso_max']
            )

            at_score  = min(at  / self.THRESHOLDS['asset_turn_min'],  2) * 30 if pd.notna(at)  and at  > 0 else 0
            et_score  = min(et  / self.THRESHOLDS['equity_turn_min'], 2) * 20 if pd.notna(et)  and et  > 0 else 0
            gm_score  = min(gm  / self.THRESHOLDS['gross_margin_min'],2) * 30 if pd.notna(gm)  and gm  > 0 else 0
            dso_score = max(1 - dso / self.THRESHOLDS['dso_max'], 0) * 20 if pd.notna(dso) else 0
            total     = round(min(at_score + et_score + gm_score + dso_score, 100), 2)

            results[stock] = {
                'passes'      : passes,
                'score'       : total,
                'asset_turn'  : round(at,  3) if pd.notna(at)  else None,
                'equity_turn' : round(et,  3) if pd.notna(et)  else None,
                'gross_margin': round(gm * 100, 2) if pd.notna(gm) else None,
                'dso_days'    : round(dso, 1) if pd.notna(dso) else None,
                'reason'      : self._reason(at, et, gm, dso),
            }
        return results

    def _reason(self, at, et, gm, dso) -> str:
        parts = []
        if pd.notna(at):
            tag = '✅' if at >= 0.5 else '❌'
            parts.append(f"{tag} VQ TS={at:.2f}x (≥0.5)")
        if pd.notna(et):
            tag = '✅' if et >= 1.0 else '❌'
            parts.append(f"{tag} VQ VCSH={et:.2f}x (≥1.0)")
        if pd.notna(gm):
            tag = '✅' if gm >= 0.20 else '❌'
            parts.append(f"{tag} Biên gộp={gm*100:.1f}% (≥20%)")
        if pd.notna(dso):
            tag = '✅' if dso <= self.THRESHOLDS['dso_max'] else '❌'
            parts.append(f"{tag} DSO={dso:.0f} ngày (≤{self.THRESHOLDS['dso_max']})")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        return [
            {"label": "VQ TS≥0.5",   "count": sum(1 for v in results.values() if v.get('asset_turn') is not None and v['asset_turn'] >= 0.5)},
            {"label": "Biên gộp≥20%","count": sum(1 for v in results.values() if v.get('gross_margin') is not None and v['gross_margin'] >= 20)},
            {"label": "DSO≤90ngày",  "count": sum(1 for v in results.values() if v.get('dso_days') is not None and v['dso_days'] <= 90)},
        ]


# ================================================================
# CHIẾN LƯỢC 7 — DÒNG TIỀN BỀN VỮNG
# Sloan (1996) — Journal of Accounting Research
# ================================================================

class CashFlowQualityStrategy:
    NAME = "Dòng tiền bền vững (Sloan)"
    KEY  = "cashflow"
    THRESHOLDS = {
        'cfo_ni_min'  : 0.8,
        'accrual_max' : 0.05,
        'cfo_ta_min'  : 0.05,
    }

    COL_CFO_NI  = 'CFO / Lợi nhuận thuần'
    COL_ACCRUAL = 'Tỷ lệ dồn tích - CF method'
    COL_CFO_TA  = 'CFO / Tổng tài sản'

    PRESET_COLS = [
        'CFO / Lợi nhuận thuần',
        'Tỷ lệ dồn tích - CF method',
        'CFO / Tổng tài sản',
        'CFO / Doanh thu thuần',
        'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)',
        'P/E',
    ]

    def screen(self, data: DBDataLoader) -> Dict[str, Dict]:
        results = {}
        for stock in data.stocks:
            cfo_ni  = data.get(stock, self.COL_CFO_NI)
            accrual = data.get(stock, self.COL_ACCRUAL)
            cfo_ta  = data.get(stock, self.COL_CFO_TA)

            passes = (
                pd.notna(cfo_ni)  and cfo_ni  >= self.THRESHOLDS['cfo_ni_min']
                and pd.notna(accrual) and accrual <= self.THRESHOLDS['accrual_max']
                and pd.notna(cfo_ta)  and cfo_ta  >= self.THRESHOLDS['cfo_ta_min']
            )

            cfo_ni_score = min(cfo_ni / self.THRESHOLDS['cfo_ni_min'], 2) * 40 if pd.notna(cfo_ni) and cfo_ni > 0 else 0

            if pd.notna(accrual):
                if accrual <= 0:
                    accrual_score = 30 + min(abs(accrual) / 0.05, 1) * 10
                else:
                    accrual_score = max(1 - accrual / self.THRESHOLDS['accrual_max'], 0) * 30
            else:
                accrual_score = 0

            cfo_ta_score = min(cfo_ta / self.THRESHOLDS['cfo_ta_min'], 2) * 30 if pd.notna(cfo_ta) and cfo_ta > 0 else 0
            total = round(min(cfo_ni_score + accrual_score + cfo_ta_score, 100), 2)

            results[stock] = {
                'passes'       : passes,
                'score'        : total,
                'cfo_ni_ratio' : round(cfo_ni,  3) if pd.notna(cfo_ni)  else None,
                'accrual_ratio': round(accrual, 4) if pd.notna(accrual) else None,
                'cfo_ta_pct'   : round(cfo_ta * 100, 2) if pd.notna(cfo_ta) else None,
                'reason'       : self._reason(cfo_ni, accrual, cfo_ta),
            }
        return results

    def _reason(self, cfo_ni, accrual, cfo_ta) -> str:
        parts = []
        if pd.notna(cfo_ni):
            tag = '✅' if cfo_ni >= 0.8 else '❌'
            parts.append(f"{tag} CFO/NI={cfo_ni:.2f} (≥0.8)")
        if pd.notna(accrual):
            tag = '✅' if accrual <= 0.05 else '❌'
            parts.append(f"{tag} Dồn tích={accrual:.3f} (≤0.05)")
        if pd.notna(cfo_ta):
            tag = '✅' if cfo_ta >= 0.05 else '❌'
            parts.append(f"{tag} CFO/TS={cfo_ta*100:.1f}% (≥5%)")
        return ' | '.join(parts)

    def get_badge_criteria(self, results: Dict) -> List[Dict]:
        return [
            {"label": "CFO/NI≥0.8",  "count": sum(1 for v in results.values() if v.get('cfo_ni_ratio') is not None and v['cfo_ni_ratio'] >= 0.8)},
            {"label": "Dồn tích≤0.05","count": sum(1 for v in results.values() if v.get('accrual_ratio') is not None and v['accrual_ratio'] <= 0.05)},
            {"label": "CFO/TS≥5%",   "count": sum(1 for v in results.values() if v.get('cfo_ta_pct') is not None and v['cfo_ta_pct'] >= 5)},
        ]


# ================================================================
# REGISTRY — map key → strategy instance
# ================================================================

STRATEGIES: Dict[str, object] = {
    'quality'    : QualityStrategy(),
    'garp'       : GARPStrategy(),
    'value'      : ValueStrategy(),
    'dividend'   : DividendStrategy(),
    'health'     : FinancialHealthStrategy(),
    'efficiency' : OperationalEfficiencyStrategy(),
    'cashflow'   : CashFlowQualityStrategy(),
}

STRATEGY_LABELS = {
    'quality'    : 'CHẤT LƯỢNG',
    'garp'       : 'TĂNG TRƯỞNG',
    'value'      : 'ĐỊNH GIÁ THẤP',
    'dividend'   : 'CỔ TỨC CAO',
    'health'     : 'SỨC KHỎE TÀI CHÍNH',
    'efficiency' : 'HIỆU QUẢ HOẠT ĐỘNG',
    'cashflow'   : 'DÒNG TIỀN BỀN VỮNG',
}


def run_strategy(db_path: str, strategy_key: str, year: str = '2024',
                 period_type: str = 'A', quarter: int = None) -> Dict:
    """
    Chạy một chiến lược cụ thể và trả về kết quả.

    Returns:
        {
            'strategy_key': str,
            'strategy_name': str,
            'results': Dict[ticker, Dict],  # passes, score, reason, ...
            'passed_stocks': List[str],
            'badge_criteria': List[Dict],    # [{label, count}, ...]
            'preset_cols': List[str],
        }
    """
    if strategy_key not in STRATEGIES:
        raise ValueError(f"Strategy '{strategy_key}' không tồn tại. Chọn một trong: {list(STRATEGIES.keys())}")

    strategy = STRATEGIES[strategy_key]
    loader   = DBDataLoader(db_path, year=year, period_type=period_type, quarter=quarter)

    if not loader.load():
        return {'error': 'Không thể load dữ liệu từ database'}

    results      = strategy.screen(loader)
    passed       = [t for t, v in results.items() if v['passes']]
    badges       = strategy.get_badge_criteria(results)

    return {
        'strategy_key'   : strategy_key,
        'strategy_name'  : strategy.NAME,
        'results'        : results,
        'passed_stocks'  : passed,
        'badge_criteria' : badges,
        'preset_cols'    : getattr(strategy, 'PRESET_COLS', []),
        'total_stocks'   : len(results),
        'passed_count'   : len(passed),
    }
