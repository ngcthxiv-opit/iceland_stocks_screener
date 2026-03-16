/* ═══════════════════════════════════════════════════════════════
   VN STOCK SCREENER — Main JavaScript
   Modules: State, API, Table, FilterDrawer, ColumnPanel,
            Autocomplete, ExcelExport, StrategyTabs
═══════════════════════════════════════════════════════════════ */

// ─────────────────────────────────────────────────────────────
// CONSTANTS – column definitions per template
// ─────────────────────────────────────────────────────────────
const PINNED_COLS = ['ticker', 'company_name'];

const FMT = {
  pct: v => v == null ? '–' : (v > 0 ? '+' : '') + (v * 100).toFixed(2) + '%',
  pct_neutral: v => v == null ? '–' : (v * 100).toFixed(2) + '%',
  num: v => v == null ? '–' : Number(v).toLocaleString('vi-VN', { maximumFractionDigits: 2 }),
  num4: v => v == null ? '–' : parseFloat(v).toFixed(4),
  int: v => v == null ? '–' : Math.round(v).toLocaleString('vi-VN'),
  billions: v => v == null ? '–' : (v / 1e9).toFixed(1) + ' tỷ',
  date: v => v ? v.split('T')[0] : '–',
  plain: v => v == null || v === '' ? '–' : v,
};

const TEMPLATES = {
  overview: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá đóng cửa', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'volume', label: 'Khối lượng', fmt: FMT.int, cls: 'text-right', numeric: true },
    { key: 'macd', label: 'MACD', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'rsi14', label: 'RSI(14)', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'rating', label: 'Xếp hạng', fmt: 'rating', cls: 'text-center' },
    { key: 'trade_signal', label: 'Tín hiệu', fmt: 'signal', cls: 'text-center' },
    { key: 'gics_sector', label: 'Lĩnh vực', fmt: FMT.plain },
    { key: 'gics_industry', label: 'Ngành GICS', fmt: FMT.plain },
    { key: 'Vốn hóa thị trường', label: 'Vốn hóa', fmt: FMT.billions, cls: 'text-right', numeric: true },
    { key: 'EPS_4Q', label: 'EPS 4Q', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/E', label: 'P/E', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'listing_date', label: 'Ngày niêm yết', fmt: FMT.date },
  ],
  price: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_open', label: 'Mở cửa', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'price_high', label: 'Cao nhất', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'price_low', label: 'Thấp nhất', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'price_close', label: 'Đóng cửa', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_1W', label: '% 1T', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_2W', label: '% 2T', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_1M', label: '% 1M', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_3M', label: '% 3M', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_6M', label: '% 6M', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_9M', label: '% 9M', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_1Y', label: '% 1N', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'change_YTD', label: '% YTD', fmt: FMT.pct, cls: 'text-right', pct: true },
  ],
  valuation: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá đóng cửa', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Vốn hóa thị trường', label: 'Vốn hóa', fmt: FMT.billions, cls: 'text-right', numeric: true },
    { key: 'EPS_4Q', label: 'EPS 4Q', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'BVPS', label: 'BVPS', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/E', label: 'P/E', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/B', label: 'P/B', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/S', label: 'P/S', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Tỷ suất cổ tức', label: 'Div Yield', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'EV/EBIT', label: 'EV/EBIT', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'EV/EBITDA', label: 'EV/EBITDA', fmt: FMT.num, cls: 'text-right', numeric: true },
  ],
  profitability: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'Tỷ suất lợi nhuận gộp biên', label: 'Biên LN gộp', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ lệ lãi EBIT', label: 'Biên EBIT', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ lệ lãi EBITDA', label: 'Biên EBITDA', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ suất sinh lợi trên doanh thu thuần', label: 'ROS', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', label: 'ROE', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)', label: 'ROCE', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)', label: 'ROA', fmt: FMT.pct_neutral, cls: 'text-right' },
  ],
};

// ─────────────────────────────────────────────────────────────
// STRATEGY-specific preset column maps (key → array of col defs)
// ─────────────────────────────────────────────────────────────
const STRATEGY_PRESET_TEMPLATES = {
  quality: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', label: 'ROE (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ suất sinh lợi trên tổng tài sản bình quân (ROA)', label: 'ROA (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)', label: 'ROCE (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'P/E', label: 'P/E', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/B', label: 'P/B', fmt: FMT.num, cls: 'text-right', numeric: true },

  ],
  garp: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'Tăng trưởng doanh thu thuần', label: 'Tăng DT (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Tăng trưởng lợi nhuận sau thuế của CĐ công ty mẹ', label: 'Tăng LN (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'P/E', label: 'P/E', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'EPS_4Q', label: 'EPS', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', label: 'ROE (%)', fmt: FMT.pct_neutral, cls: 'text-right' },

  ],
  value: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'P/E', label: 'P/E', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/B', label: 'P/B', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'EV/EBITDA', label: 'EV/EBITDA', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'EV/EBIT', label: 'EV/EBIT', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/S', label: 'P/S', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Vốn hóa thị trường', label: 'Vốn hóa', fmt: FMT.billions, cls: 'text-right', numeric: true },

  ],
  dividend: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'Tỷ suất cổ tức', label: 'Cổ tức (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Khả năng thanh toán lãi vay (ICR)', label: 'ICR', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'CFO / Doanh thu thuần', label: 'CFO/DT', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'P/E', label: 'P/E', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'P/B', label: 'P/B', fmt: FMT.num, cls: 'text-right', numeric: true },

  ],
  health: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'Tỷ số thanh toán hiện hành (Current Ratio)', label: 'Current Ratio', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Khả năng thanh toán lãi vay (ICR)', label: 'ICR', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Nợ (PT) / VCS', label: 'Nợ/VCS', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'CFO / Nợ ngắn hạn', label: 'CFO/Nợ NH', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', label: 'ROE (%)', fmt: FMT.pct_neutral, cls: 'text-right' },

  ],
  efficiency: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'Vòng quay tổng tài sản', label: 'VQ Tài sản', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'Vòng quay vốn chủ sở hữu', label: 'VQ VCSH', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'Tỷ suất lợi nhuận gộp biên', label: 'Biên gộp (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'Thời gian thu tiền khách hàng bình quân (DSO)', label: 'DSO (ngày)', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', label: 'ROE (%)', fmt: FMT.pct_neutral, cls: 'text-right' },

  ],
  cashflow: [
    { key: 'ticker', label: 'Mã CK', fmt: FMT.plain, pinned: true },
    { key: 'company_name', label: 'Tên công ty', fmt: FMT.plain, pinned: true },
    { key: 'price_close', label: 'Giá', fmt: FMT.num, cls: 'text-right', numeric: true },
    { key: 'change_1D', label: '% 1D', fmt: FMT.pct, cls: 'text-right', pct: true },
    { key: 'CFO / Lợi nhuận thuần', label: 'CFO/NI', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'Tỷ lệ dồn tích - CF method', label: 'Dồn tích', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'CFO / Tổng tài sản', label: 'CFO/TS (%)', fmt: FMT.pct_neutral, cls: 'text-right' },
    { key: 'CFO / Doanh thu thuần', label: 'CFO/DT', fmt: FMT.num4, cls: 'text-right', numeric: true },
    { key: 'Tỷ suất lợi nhuận trên vốn chủ sở hữu bình quân (ROE)', label: 'ROE (%)', fmt: FMT.pct_neutral, cls: 'text-right' },

  ],
};

// ─────────────────────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────────────────────
const State = (() => {
  const LS_KEY = 'vn_screener_state_v2';

  const defaults = {
    periodType: 'A',
    year: '',
    quarter: '1',
    template: 'overview',
    filters: [],
    extraCols: [],
    search: '',
  };

  let s = { ...defaults };
  let meta = { years: [], quarters: [], indicators: [], companies: [] };
  let tableData = [];
  let sortCol = null;
  let sortDir = 'asc';

  try {
    const saved = JSON.parse(localStorage.getItem(LS_KEY) || '{}');
    s = { ...defaults, ...saved };
  } catch (_) { }

  function save() {
    try { localStorage.setItem(LS_KEY, JSON.stringify(s)); } catch (_) { }
  }

  function setPeriod(p) {
    s.periodType = p;
    document.getElementById('btn-period-A').classList.toggle('active', p === 'A');
    document.getElementById('btn-period-Q').classList.toggle('active', p === 'Q');
    document.getElementById('sel-quarter').classList.toggle('hidden', p === 'A');
    save(); triggerLoad();
  }

  function setYear(y) { s.year = y; save(); triggerLoad(); }
  function setQuarter(q) { s.quarter = q; save(); triggerLoad(); }
  function setSearch(v) { s.search = v; save(); triggerLoad(); }
  function setTemplate(t) {
    s.template = t;
    document.getElementById('sel-template').value = t;
    save(); Table.render(tableData);
  }

  function addFilter(f) {
    const idx = s.filters.findIndex(x => x.indicator === f.indicator);
    if (idx >= 0) s.filters[idx] = f; else s.filters.push(f);
    save(); updateFilterBadge(); FilterDrawer.renderActive();
  }

  function removeFilter(indicator) {
    s.filters = s.filters.filter(f => f.indicator !== indicator);
    save(); updateFilterBadge(); FilterDrawer.renderActive();
  }

  function clearFilters() { s.filters = []; save(); updateFilterBadge(); FilterDrawer.renderActive(); }

  function updateFilterBadge() {
    const badge = document.getElementById('filter-badge');
    const sum = document.getElementById('filter-summary');
    if (s.filters.length > 0) {
      badge.textContent = s.filters.length;
      badge.style.display = '';
      sum.style.display = 'flex';
    } else {
      badge.style.display = 'none';
      sum.style.display = 'none';
    }
    FilterSummary.render();
  }

  function setMeta(m) { meta = m; }
  function getMeta() { return meta; }
  function getState() { return s; }
  function setTableData(d) { tableData = d; }
  function getTableData() { return tableData; }
  function getSort() { return { sortCol, sortDir }; }
  function setSort(col) {
    if (sortCol === col) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    else { sortCol = col; sortDir = 'asc'; }
  }
  function setExtraCols(cols) { s.extraCols = cols; save(); }

  function init(m) {
    setMeta(m);
    const yearSel = document.getElementById('sel-year');
    yearSel.innerHTML = m.years.map(y => `<option value="${y}">${y}</option>`).join('');
    if (!s.year && m.years.length) s.year = m.years[0];
    yearSel.value = s.year;
    document.getElementById('sel-quarter').value = s.quarter;
    document.getElementById('sel-template').value = s.template;
    document.getElementById('btn-period-A').classList.toggle('active', s.periodType === 'A');
    document.getElementById('btn-period-Q').classList.toggle('active', s.periodType === 'Q');
    document.getElementById('sel-quarter').classList.toggle('hidden', s.periodType === 'A');
    updateFilterBadge();
    triggerLoad();
  }

  return {
    setPeriod, setYear, setQuarter, setSearch, setTemplate,
    addFilter, removeFilter, clearFilters, updateFilterBadge,
    setMeta, getMeta, getState, setTableData, getTableData,
    getSort, setSort, setExtraCols, init, save,
  };
})();

// ─────────────────────────────────────────────────────────────
// STRATEGY TABS
// ─────────────────────────────────────────────────────────────
const StrategyTabs = (() => {
  let currentKey = 'all';

  function setTab(key) {
    currentKey = key;

    // Update active class on buttons
    document.querySelectorAll('.stab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.key === key);
    });

    // Show/hide time picker depending on strategy tab
    const timepicker = document.getElementById('timepicker-group');
    if (timepicker) timepicker.style.opacity = key === 'all' ? '1' : '0.5';

    // Clear badges if resetting to all
    if (key === 'all') {
      document.getElementById('strategy-badges').innerHTML = '';
    }

    triggerLoad();
  }

  function reset() {
    setTab('all');
  }

  function getKey() { return currentKey; }

  function renderBadges(badges) {
    const container = document.getElementById('strategy-badges');
    if (!badges || !badges.length) {
      container.innerHTML = '';
      return;
    }
    container.innerHTML = badges.map(b =>
      `<div class="strategy-chip">
        <span class="strategy-chip-label">${b.label}</span>
        <span class="strategy-chip-count">${b.count} mã</span>
      </div>`
    ).join('');
  }

  return { setTab, reset, getKey, renderBadges };
})();

// ─────────────────────────────────────────────────────────────
// API
// ─────────────────────────────────────────────────────────────
const API = {
  async fetchMeta() {
    const r = await fetch('/api/meta');
    return r.json();
  },
  async fetchScreener() {
    const stratKey = StrategyTabs.getKey();

    // If a strategy tab is active, call /api/strategy endpoint
    if (stratKey !== 'all') {
      return this.fetchStrategy(stratKey);
    }

    const { periodType, year, quarter, filters, search } = State.getState();
    const params = new URLSearchParams();
    params.set('period_type', periodType);
    if (year) params.set('year', year);
    if (periodType === 'Q' && quarter) params.set('quarter', quarter);
    params.set('filters', JSON.stringify(filters));
    if (search) params.set('search', search);
    const r = await fetch('/api/screener?' + params.toString());
    return r.json();
  },
  async fetchStrategy(stratKey) {
    const { filters, search } = State.getState();
    const params = new URLSearchParams();
    params.set('strategy', stratKey);
    params.set('filters', JSON.stringify(filters));
    if (search) params.set('search', search);
    const r = await fetch('/api/strategy?' + params.toString());
    return r.json();
  },
  async fetchAutocomplete(q) {
    const r = await fetch('/api/autocomplete?q=' + encodeURIComponent(q));
    return r.json();
  },
};

// ─────────────────────────────────────────────────────────────
// TABLE
// ─────────────────────────────────────────────────────────────
const Table = (() => {
  function getColumns(template, extraCols, strategyKey) {
    // If strategy tab active, use strategy-specific preset columns
    if (strategyKey && strategyKey !== 'all' && STRATEGY_PRESET_TEMPLATES[strategyKey]) {
      const base = STRATEGY_PRESET_TEMPLATES[strategyKey];
      const extra = (extraCols || []).map(k => ({
        key: k, label: k, fmt: FMT.num, cls: 'text-right', numeric: true,
      }));
      const keys = new Set(base.map(c => c.key));
      return [...base, ...extra.filter(c => !keys.has(c.key))];
    }

    const base = TEMPLATES[template] || TEMPLATES.overview;
    const extra = (extraCols || []).map(k => ({
      key: k, label: k, fmt: FMT.num, cls: 'text-right', numeric: true,
    }));
    const keys = new Set(base.map(c => c.key));
    const filtered = extra.filter(c => !keys.has(c.key));
    return [...base, ...filtered];
  }

  function colClass(col, value) {
    let cls = col.cls || '';
    if (col.pct && value != null) cls += value >= 0 ? ' pos' : ' neg';
    return cls;
  }

  function renderCell(col, value) {
    if (col.fmt === 'rating') {
      if (!value || value === '–') return '<span class="null-val">–</span>';
      return `<span class="rating-badge rating-${value}">${value}</span>`;
    }
    if (col.fmt === 'signal') {
      if (!value || value === '–') return '<span class="null-val">–</span>';
      const cls = value === 'Mua' ? 'signal-buy' : value === 'Bán' ? 'signal-sell' : 'signal-neutral';
      const icon = value === 'Mua' ? '▲' : value === 'Bán' ? '▼' : '•';
      return `<span class="signal-badge ${cls}">${icon} ${value}</span>`;
    }
    if (value == null || value === '' || value === undefined) {
      return '<span class="null-val">–</span>';
    }
    return col.fmt(value);
  }

  function render(data) {
    const { sortCol, sortDir } = State.getSort();
    const { template, extraCols } = State.getState();
    const strategyKey = StrategyTabs.getKey();
    const cols = getColumns(template, extraCols, strategyKey);

    // Sort
    let sorted = [...data];
    if (sortCol) {
      sorted.sort((a, b) => {
        let av = a[sortCol], bv = b[sortCol];
        if (av == null) av = sortDir === 'asc' ? Infinity : -Infinity;
        if (bv == null) bv = sortDir === 'asc' ? Infinity : -Infinity;
        const cmp = typeof av === 'string' ? av.localeCompare(bv) : av - bv;
        return sortDir === 'asc' ? cmp : -cmp;
      });
    }

    // Head
    const thead = document.getElementById('table-head');
    thead.innerHTML = '<tr>' + cols.map(col => {
      const sorted_cls = sortCol === col.key ? ' sorted' : '';
      const arrow = sortCol === col.key
        ? `<span class="sort-arrow active">${sortDir === 'asc' ? '↑' : '↓'}</span>`
        : '<span class="sort-arrow">↕</span>';
      const pinned_cls = col.pinned ? ' pinned' : '';
      return `<th class="${sorted_cls}${pinned_cls}" onclick="Table.sort('${col.key}')">${col.label}${arrow}</th>`;
    }).join('') + '</tr>';

    const ths = thead.querySelectorAll('th.pinned');
    if (ths.length > 1) ths[1].classList.add('pinned2');

    // Body
    const tbody = document.getElementById('table-body');
    if (!sorted.length) {
      tbody.innerHTML = `<tr><td colspan="${cols.length}" style="text-align:center;padding:40px;color:var(--text-sec)">
        Không tìm thấy kết quả phù hợp</td></tr>`;
      return;
    }

    tbody.innerHTML = sorted.map(row => {
      const tds = cols.map(col => {
        const val = row[col.key];
        const css = colClass(col, val) + (col.pinned ? ' pinned' : '') +
          (col.key === 'ticker' ? ' ticker-cell' : '') +
          (val == null ? ' null-val' : '');
        return `<td class="${css}">${renderCell(col, val)}</td>`;
      }).join('');
      return `<tr>${tds}</tr>`;
    }).join('');

    const allRows = tbody.querySelectorAll('tr');
    allRows.forEach(tr => {
      const pinnedCells = tr.querySelectorAll('td.pinned');
      if (pinnedCells.length > 1) pinnedCells[1].classList.add('pinned2');
    });

    document.getElementById('table-info').textContent =
      `${sorted.length} cổ phiếu | Sắp xếp${sortCol ? ': ' + sortCol : ': Mặc định'}`;
  }

  function sort(key) {
    State.setSort(key);
    render(State.getTableData());
  }

  return { render, sort, getColumns };
})();

// ─────────────────────────────────────────────────────────────
// FILTER SUMMARY
// ─────────────────────────────────────────────────────────────
const FilterSummary = {
  render() {
    const { filters } = State.getState();
    const list = document.getElementById('chip-list');
    list.innerHTML = filters.map(f => {
      const opLabel = f.op === 'between'
        ? `${f.val1} – ${f.val2}`
        : `${f.op} ${f.val1}`;
      return `<div class="chip">
        ${f.indicator} ${opLabel}
        <button class="chip-close" onclick="State.removeFilter('${CSS.escape(f.indicator)}')" title="Xóa">✕</button>
      </div>`;
    }).join('');
  },
};

// ─────────────────────────────────────────────────────────────
// FILTER DRAWER
// ─────────────────────────────────────────────────────────────
const FilterDrawer = (() => {
  let currentTab = 'active';

  function open() {
    document.getElementById('filter-overlay').classList.add('active');
    document.getElementById('filter-drawer').classList.add('open');
    renderActive();
    renderAll();
  }

  function close() {
    document.getElementById('filter-overlay').classList.remove('active');
    document.getElementById('filter-drawer').classList.remove('open');
  }

  function switchTab(tab) {
    currentTab = tab;
    document.getElementById('dtab-active').classList.toggle('active', tab === 'active');
    document.getElementById('dtab-all').classList.toggle('active', tab === 'all');
    document.getElementById('filter-active-panel').classList.toggle('hidden', tab !== 'active');
    document.getElementById('filter-all-panel').classList.toggle('hidden', tab !== 'all');
  }

  function renderActive() {
    const panel = document.getElementById('filter-active-panel');
    const { filters } = State.getState();
    if (!filters.length) {
      panel.innerHTML = `<div class="empty-filter-msg">
        Chưa có bộ lọc nào được thêm.
        <div class="hint">Chuyển sang tab "Tất cả chỉ tiêu" để thêm điều kiện lọc.</div>
      </div>`;
      return;
    }
    panel.innerHTML = filters.map((f, i) => filterCondHTML(f, i)).join('');
  }

  function filterCondHTML(f, i) {
    const opOpts = ['>', '>=', '<', '<=', '=', 'between', 'top_n', 'bottom_n']
      .map(o => `<option value="${o}" ${o === f.op ? 'selected' : ''}>${o}</option>`).join('');
    return `<div class="filter-cond-row" data-i="${i}">
      <div class="filter-cond-top">
        <span class="filter-cond-name">${f.indicator}</span>
        <button class="icon-btn" style="font-size:12px" onclick="FilterDrawer.removeFilter('${CSS.escape(f.indicator)}')">✕</button>
      </div>
      <div class="filter-inputs">
        <select class="filter-op-sel" onchange="FilterDrawer.updateFilter(${i},'op',this.value)">${opOpts}</select>
        <input class="filter-val-input" type="number" placeholder="Giá trị" value="${f.val1 ?? ''}"
          onchange="FilterDrawer.updateFilter(${i},'val1',this.value)" />
        ${f.op === 'between' ? `<span style="color:var(--text-sec);font-size:11px">–</span>
          <input class="filter-val-input" type="number" placeholder="Đến" value="${f.val2 ?? ''}"
            onchange="FilterDrawer.updateFilter(${i},'val2',this.value)" />` : ''}
      </div>
    </div>`;
  }

  function renderAll() {
    const meta = State.getMeta();
    const { filters } = State.getState();
    const activeSet = new Set(filters.map(f => f.indicator));
    const panel = document.getElementById('filter-all-panel');

    const groups = {};
    meta.indicators.forEach(ind => {
      const g = ind.group || 'Khác';
      if (!groups[g]) groups[g] = [];
      groups[g].push(ind);
    });

    let html = `<input id="filter-search-input" type="text" class="filter-val-input" style="width:100%;margin-bottom:12px;padding:7px 10px"
      placeholder="Tìm chỉ tiêu…" oninput="FilterDrawer.filterSearch(this.value)" />`;

    Object.entries(groups).sort(([a], [b]) => a.localeCompare(b)).forEach(([g, inds]) => {
      html += `<div class="indicator-group">
        <div class="indicator-group-title">${g}</div>`;
      inds.forEach(ind => {
        const added = activeSet.has(ind.name);
        html += `<div class="indicator-item" data-name="${ind.name}">
          <span class="indicator-name">${ind.name}</span>
          <button class="indicator-add-btn ${added ? 'added' : ''}"
            onclick="FilterDrawer.addIndicator('${CSS.escape(ind.name)}')">
            ${added ? '✓ Đã chọn' : '+ Thêm'}
          </button>
        </div>`;
      });
      html += '</div>';
    });
    panel.innerHTML = html;
  }

  function filterSearch(q) {
    const items = document.querySelectorAll('#filter-all-panel .indicator-item');
    const lcq = q.toLowerCase();
    items.forEach(el => {
      const name = el.dataset.name.toLowerCase();
      el.style.display = (!q || name.includes(lcq)) ? '' : 'none';
    });
    document.querySelectorAll('#filter-all-panel .indicator-group').forEach(g => {
      const visible = [...g.querySelectorAll('.indicator-item')].some(el => el.style.display !== 'none');
      g.style.display = visible ? '' : 'none';
    });
  }

  function addIndicator(name) {
    const el = document.querySelector(`[data-name="${name}"]`);
    const realName = el ? el.dataset.name : name;
    State.addFilter({ indicator: realName, op: '>=', val1: '', val2: null });
    renderActive();
    renderAll();
    switchTab('active');
  }

  function removeFilter(name) {
    State.removeFilter(name);
    renderActive();
    renderAll();
  }

  function updateFilter(i, field, value) {
    const { filters } = State.getState();
    if (!filters[i]) return;
    filters[i][field] = value === '' ? null : (field === 'op' ? value : Number(value));
    if (field === 'op') renderActive();
    FilterSummary.render();
    State.save();
  }

  function clearAll() {
    State.clearFilters();
    renderActive();
    renderAll();
    close();
    triggerLoad();
  }

  function apply() {
    State.updateFilterBadge();
    close();
    triggerLoad();
  }

  return { open, close, switchTab, renderActive, renderAll, filterSearch, addIndicator, removeFilter, updateFilter, clearAll, apply };
})();

// ─────────────────────────────────────────────────────────────
// COLUMN PANEL
// ─────────────────────────────────────────────────────────────
const ColumnPanel = (() => {
  let selectedGroup = null;
  let pendingCols = [];

  function open() {
    const { template, extraCols } = State.getState();
    pendingCols = [...extraCols];
    document.getElementById('col-overlay').classList.add('active');
    document.getElementById('col-drawer').classList.add('open');
    renderGroups();
    renderSelected(template);
  }

  function close() {
    document.getElementById('col-overlay').classList.remove('active');
    document.getElementById('col-drawer').classList.remove('open');
  }

  function renderGroups() {
    const meta = State.getMeta();
    const groupNames = [...new Set(meta.indicators.map(i => i.group))].sort();
    const el = document.getElementById('col-groups');
    el.innerHTML = groupNames.map(g =>
      `<button class="col-group-btn ${g === selectedGroup ? 'active' : ''}" 
        onclick="ColumnPanel.selectGroup('${CSS.escape(g)}')">${g}</button>`
    ).join('');
  }

  function selectGroup(g) {
    selectedGroup = g;
    renderGroups();
    renderIndicatorList(g);
  }

  function renderIndicatorList(g) {
    const meta = State.getMeta();
    const inds = meta.indicators.filter(i => i.group === g);
    const wrap = document.querySelector('.col-selected-wrap');

    let html = `<div class="col-indicator-list" id="col-indicator-list">`;
    inds.forEach(ind => {
      const checked = pendingCols.includes(ind.name);
      html += `<label class="col-indicator-item">
        <input type="checkbox" ${checked ? 'checked' : ''} onchange="ColumnPanel.toggleCol('${CSS.escape(ind.name)}', this.checked)">
        <span>${ind.name}</span>
      </label>`;
    });
    html += '</div>';

    const existing = document.getElementById('col-indicator-list');
    if (existing) existing.remove();
    wrap.insertAdjacentHTML('afterbegin', html);
    renderSelected(State.getState().template);
  }

  function toggleCol(name, checked) {
    if (checked) {
      if (!pendingCols.includes(name)) pendingCols.push(name);
    } else {
      pendingCols = pendingCols.filter(c => c !== name);
    }
    renderSelected(State.getState().template);
  }

  function renderSelected(template) {
    const stratKey = StrategyTabs.getKey();
    const baseCols = (stratKey !== 'all' && STRATEGY_PRESET_TEMPLATES[stratKey])
      ? STRATEGY_PRESET_TEMPLATES[stratKey]
      : (TEMPLATES[template] || TEMPLATES.overview);
    const el = document.getElementById('col-selected-list');
    if (!el) return;

    const pinned = baseCols.filter(c => c.pinned).map(c =>
      `<li class="col-chip pinned-chip">🔒 ${c.label}</li>`
    );
    const base = baseCols.filter(c => !c.pinned).map(c =>
      `<li class="col-chip" data-key="${c.key}">
        ${c.label}
        <button class="col-chip-remove" title="Không thể xóa cột mặc định" style="opacity:.3" disabled>✕</button>
      </li>`
    );
    const extra = pendingCols.map(k =>
      `<li class="col-chip" data-key="${k}">
        ${k}
        <button class="col-chip-remove" onclick="ColumnPanel.toggleCol('${CSS.escape(k)}', false)">✕</button>
      </li>`
    );
    el.innerHTML = [...pinned, ...base, ...extra].join('');
  }

  function resetToDefault() {
    pendingCols = [];
    State.setExtraCols([]);
    const existing = document.getElementById('col-indicator-list');
    if (existing) existing.remove();
    renderSelected(State.getState().template);
  }

  function apply() {
    State.setExtraCols(pendingCols);
    close();
    Table.render(State.getTableData());
  }

  return { open, close, selectGroup, toggleCol, renderSelected, resetToDefault, apply };
})();

// ─────────────────────────────────────────────────────────────
// AUTOCOMPLETE
// ─────────────────────────────────────────────────────────────
const Autocomplete = (() => {
  let items = [];
  let selIdx = -1;
  let debounce = null;

  function onInput(val) {
    clearTimeout(debounce);
    if (!val.trim()) { hide(); State.setSearch(''); return; }
    debounce = setTimeout(async () => {
      items = await API.fetchAutocomplete(val);
      if (!items.length) { hide(); State.setSearch(val); return; }
      const list = document.getElementById('autocomplete-list');
      list.innerHTML = items.map((it, i) =>
        `<div class="autocomplete-item" data-i="${i}" onmousedown="Autocomplete.select(${i})">
          <span class="ac-ticker">${it.ticker}</span>
          <span class="ac-name">${it.company_name || ''}</span>
          <span class="ac-exchange">${it.exchange || ''}</span>
        </div>`
      ).join('');
      list.classList.remove('hidden');
      State.setSearch(val);
    }, 220);
  }

  function select(i) {
    const it = items[i];
    if (!it) return;
    document.getElementById('search-input').value = it.ticker;
    State.setSearch(it.ticker);
    hide();
  }

  function hide() {
    document.getElementById('autocomplete-list').classList.add('hidden');
    selIdx = -1;
  }

  function onKey(e) {
    const list = document.getElementById('autocomplete-list');
    const itms = list.querySelectorAll('.autocomplete-item');
    if (e.key === 'ArrowDown') {
      selIdx = Math.min(selIdx + 1, itms.length - 1);
    } else if (e.key === 'ArrowUp') {
      selIdx = Math.max(selIdx - 1, 0);
    } else if (e.key === 'Enter' && selIdx >= 0) {
      select(selIdx); return;
    } else if (e.key === 'Escape') {
      hide(); return;
    } else return;
    itms.forEach((el, i) => el.classList.toggle('selected', i === selIdx));
  }

  return { onInput, select, hide, onKey };
})();

// ─────────────────────────────────────────────────────────────
// EXCEL EXPORT
// ─────────────────────────────────────────────────────────────
const ExcelExport = {
  download() {
    const stratKey = StrategyTabs.getKey();
    const { periodType, year, quarter, filters, search, template, extraCols } = State.getState();
    const params = new URLSearchParams();

    if (stratKey !== 'all') {
      // Export via strategy endpoint
      params.set('strategy', stratKey);
      params.set('filters', JSON.stringify(filters));
      if (search) params.set('search', search);
      // Add column info for export
      const cols = Table.getColumns(template, extraCols, stratKey);
      cols.forEach(c => params.append('show_cols[]', c.key));
      params.set('col_labels', JSON.stringify(Object.fromEntries(cols.map(c => [c.key, c.label]))));
      window.location.href = '/api/export_strategy?' + params.toString();
    } else {
      params.set('period_type', periodType);
      if (year) params.set('year', year);
      if (periodType === 'Q' && quarter) params.set('quarter', quarter);
      params.set('filters', JSON.stringify(filters));
      if (search) params.set('search', search);
      const cols = Table.getColumns(template, extraCols, null);
      cols.forEach(c => params.append('show_cols[]', c.key));
      params.set('col_labels', JSON.stringify(Object.fromEntries(cols.map(c => [c.key, c.label]))));
      window.location.href = '/api/export?' + params.toString();
    }
  }
};

// ─────────────────────────────────────────────────────────────
// MAIN LOAD TRIGGER
// ─────────────────────────────────────────────────────────────
let _loadDebounce = null;
function triggerLoad() {
  clearTimeout(_loadDebounce);
  _loadDebounce = setTimeout(loadData, 100);
}

async function loadData() {
  const overlay = document.getElementById('loading-overlay');
  overlay.style.display = 'flex';
  document.getElementById('table-info').textContent = 'Đang tải dữ liệu…';

  try {
    const data = await API.fetchScreener();
    State.setTableData(data.rows || []);
    Table.render(data.rows || []);

    // Render strategy badges if in strategy mode
    if (data.badges) {
      StrategyTabs.renderBadges(data.badges);
    } else {
      document.getElementById('strategy-badges').innerHTML = '';
    }
  } catch (err) {
    console.error('Load error:', err);
    document.getElementById('table-body').innerHTML =
      `<tr><td colspan="20" style="text-align:center;padding:40px;color:var(--red)">
        Lỗi tải dữ liệu: ${err.message}</td></tr>`;
    document.getElementById('table-info').textContent = 'Lỗi kết nối backend';
  } finally {
    overlay.style.display = 'none';
  }
}

// ─────────────────────────────────────────────────────────────
// BOOTSTRAP
// ─────────────────────────────────────────────────────────────
(async function init() {
  try {
    const meta = await API.fetchMeta();
    State.init(meta);
    FilterDrawer.renderAll();
  } catch (err) {
    console.error('Init error:', err);
    document.getElementById('loading-overlay').innerHTML =
      `<div style="color:var(--red);padding:20px;text-align:center">
        <b>Không thể kết nối tới backend Flask.</b><br>
        Hãy chắc chắn đã chạy: <code>python app.py</code>
      </div>`;
  }
})();
