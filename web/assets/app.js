import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  LineStyle,
  createChart,
} from "/assets/vendor/lightweight-charts.standalone.production.mjs";

const api = {
  info: "/api/info",
  health: "/health",
  syncStatus: "/api/v1/sync/status",
  syncDaily: "/api/v1/sync/daily",
  syncFullRefresh: "/api/v1/sync/refresh-all",
  analysisScorecard: "/api/v1/analysis/scorecard",
  analysisScoreDetail: (code) => `/api/v1/analysis/scorecard/${code}`,
  analysisMarketOverview: "/api/v1/analysis/market-overview",
  analysisTechnicalDetail: (code) => `/api/v1/analysis/technical/${code}`,
  stockSearch: "/api/v1/stocks/search",
  stockInfo: (code) => `/api/v1/stocks/${code}`,
  latestPrice: (code) => `/api/v1/kline/latest/${code}`,
  dailyKline: (code, limit = 250) => `/api/v1/kline/daily/${code}?limit=${limit}`,
};

const CHART_HEIGHT = 360;
const CHART_FETCH_LIMIT = 4000;
const CHART_DEFAULT_RANGE = 750;
const YEAR_LOOKBACK_BARS = 250;
const SEARCH_DEBOUNCE_MS = 320;
const SYNC_SUMMARY_INTERVAL_MS = 30000;
const CHART_COLORS = {
  up: "#c63d2d",
  down: "#0d8d61",
  flat: "#5f6f76",
  ma5: "#0f9f97",
  ma10: "#d4a75f",
  ma20: "#df6c2e",
  ink: "#11212a",
  grid: "rgba(17, 33, 42, 0.08)",
  axis: "rgba(95, 111, 118, 0.92)",
};

const state = {
  selectedCode: "000001",
  searchTimer: null,
  searchAbortController: null,
  searchRequestSeq: 0,
  searchAppliedSeq: 0,
  searchCache: new Map(),
  searchSuggestionsSignature: "",
  isSearchComposing: false,
  chartRange: CHART_DEFAULT_RANGE,
  chartRows: [],
  chartVisibleStats: null,
  chartApi: null,
  chartSeries: null,
  chartRowsByTime: new Map(),
  chartFocusTradeDate: null,
  pendingChartFocusRow: null,
  chartFocusFrame: 0,
  selectionRequestId: 0,
  pendingTasks: {},
  runningTasks: {},
  syncSummaryTimer: null,
  syncSummaryRequestSeq: 0,
  syncSummaryAppliedSeq: 0,
  syncSummaryInFlight: false,
  syncSummarySignature: "",
};

const $ = (id) => document.getElementById(id);

const numberFormatter = new Intl.NumberFormat("zh-CN");
const compactNumberFormatter = new Intl.NumberFormat("zh-CN", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function parseNullableNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function hasValue(value) {
  return value !== null && value !== undefined && value !== "";
}

function formatMaybeNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  return num.toLocaleString("zh-CN", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function formatCompact(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  return compactNumberFormatter.format(num);
}

function formatMoneyFlow(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  const sign = num > 0 ? "+" : "";
  const absValue = Math.abs(num);
  if (absValue >= 1e8) {
    return `${sign}${formatMaybeNumber(num / 1e8, digits)}亿`;
  }
  if (absValue >= 1e4) {
    return `${sign}${formatMaybeNumber(num / 1e4, digits)}万`;
  }
  return `${sign}${formatMaybeNumber(num, digits)}`;
}

function formatDateTime(value) {
  if (!value) {
    return "等待数据";
  }
  return String(value).replace("T", " ").slice(0, 16);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  return `${formatMaybeNumber(value, digits)}%`;
}

function formatSignedNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  const sign = num > 0 ? "+" : "";
  return `${sign}${num.toLocaleString("zh-CN", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })}`;
}

function formatSignedPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  return `${formatSignedNumber(value, digits)}%`;
}

function formatDecimalPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  return `${formatMaybeNumber(num * 100, digits)}%`;
}

function formatSignedDecimalPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  return `${formatSignedNumber(num * 100, digits)}%`;
}

function formatChangePair(change, pctChange) {
  return `${formatSignedNumber(change)} / ${formatSignedPercent(pctChange)}`;
}

function scoreToneClass(score, positiveThreshold = 4, negativeThreshold = 1) {
  if (score >= positiveThreshold) {
    return "positive";
  }
  if (score <= negativeThreshold) {
    return "negative";
  }
  return "";
}

function signalToneClass(label) {
  const text = String(label || "");
  if (/多头|上升|偏强|强势|跑赢|待突破|良好|可控|显著|放量|已进入/.test(text)) {
    return "positive";
  }
  if (/偏弱|一般|偏大|未进入|失守|跌破/.test(text)) {
    return "negative";
  }
  return "";
}

async function fetchJson(url, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const requestUrl = method === "GET" || method === "HEAD" ? withNoCacheUrl(url) : url;
  const response = await fetch(requestUrl, {
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
    ...options,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `HTTP ${response.status}`);
  }
  return response.json();
}

function withNoCacheUrl(url) {
  const requestUrl = new URL(url, window.location.origin);
  requestUrl.searchParams.set("_ts", `${Date.now()}`);
  return requestUrl.toString();
}

function setActionMessage(text, tone = "neutral") {
  const el = $("actionMessage");
  if (!el) {
    return;
  }
  el.textContent = text;
  el.classList.remove("positive", "negative");
  if (tone === "positive") {
    el.classList.add("positive");
  } else if (tone === "negative") {
    el.classList.add("negative");
  }
}

function isTaskBusy(taskName) {
  return Boolean(state.pendingTasks[taskName] || state.runningTasks?.[taskName]?.is_running);
}

function updateTaskControls() {
  const dailyButton = $("syncDailyBtn");
  const missingButton = $("syncMissingBtn");
  if (!dailyButton || !missingButton) {
    return;
  }
  const dailyBusy = isTaskBusy("daily_kline");
  const fullRefreshBusy = isTaskBusy("full_refresh");
  const busy = dailyBusy || fullRefreshBusy;
  dailyButton.disabled = busy;
  missingButton.disabled = busy;
  dailyButton.textContent = dailyBusy ? "同步中..." : fullRefreshBusy ? "更新中..." : "同步最新日线";
  missingButton.textContent = fullRefreshBusy ? "更新中..." : dailyBusy ? "日线同步中..." : "补齐并更新数据";
}

function priceToneClass(change) {
  if (change > 0) {
    return "market-up";
  }
  if (change < 0) {
    return "market-down";
  }
  return "";
}

function applyToneClass(element, value) {
  element.classList.remove("market-up", "market-down");
  const toneClass = priceToneClass(Number(value || 0));
  if (toneClass) {
    element.classList.add(toneClass);
  }
}

async function loadHealth() {
  try {
    const data = await fetchJson(api.health);
    $("healthText").textContent = `${data.status} / ${data.database}`;
  } catch (error) {
    $("healthText").textContent = "服务异常";
  }
}

function getRunningTaskEntries(runningTasks) {
  return Object.entries(runningTasks || {})
    .filter(([, item]) => item?.is_running)
    .map(([taskName, item]) => ({
      taskName,
      label: item?.label || taskName,
    }));
}

function formatRunningTaskNames(runningTaskEntries) {
  if (!runningTaskEntries.length) {
    return "当前空闲";
  }
  return runningTaskEntries.map((item) => item.label).join(" / ");
}

function renderTableVolumeSnapshot(snapshot) {
  const cardEl = document.querySelector("#tableVolumeGrid .snapshot-summary-card");
  const titleEl = $("tableVolumeTitle");
  const metaEl = $("tableVolumeMeta");
  const summaryEl = $("tableVolumeSummary");
  const gridEl = $("tableVolumeGrid");
  if (!cardEl || !titleEl || !metaEl || !summaryEl || !gridEl) {
    return;
  }

  const items = Array.isArray(snapshot?.items)
    ? snapshot.items.filter((item) => hasValue(item?.row_count))
    : [];

  if (!items.length) {
    titleEl.textContent = "暂无快照";
    metaEl.textContent = "等待同步任务完成后自动记录";
    summaryEl.textContent = "等待同步任务完成后自动记录。";
    gridEl.innerHTML = "";
    gridEl.append(cardEl);
    return;
  }

  const metaParts = [];
  if (snapshot?.trade_date) {
    metaParts.push(`交易日 ${escapeHtml(snapshot.trade_date)}`);
  }
  if (snapshot?.trigger_sync_label) {
    metaParts.push(escapeHtml(snapshot.trigger_sync_label));
  }
  if (snapshot?.snapshot_time) {
    metaParts.push(formatDateTime(snapshot.snapshot_time));
  }
  const summaryMarkup = items
    .map((item) => {
      const rowCount = parseNullableNumber(item.row_count) || 0;
      return `
        <span class="snapshot-summary-pill">
          <span class="snapshot-summary-pill-label">${escapeHtml(item.table_label || item.table_name)}</span>
          <strong class="snapshot-summary-pill-value">${formatCompact(rowCount)}</strong>
        </span>
      `;
    })
    .join("");

  titleEl.textContent = "最近同步数据量";
  metaEl.textContent = metaParts.join(" · ") || "最近一次同步快照";
  summaryEl.innerHTML = summaryMarkup;
  gridEl.innerHTML = "";
  gridEl.append(cardEl);
}

function applySyncSummaryPayload(payload) {
  const snapshot = payload.table_volume_snapshot || {};
  const runningTaskEntries = getRunningTaskEntries(payload.running_tasks);
  const runningTaskNames = formatRunningTaskNames(runningTaskEntries);
  state.runningTasks = payload.running_tasks || {};
  updateTaskControls();

  const summarySignature = JSON.stringify({
    snapshot_time: snapshot.snapshot_time || null,
    snapshot_trade_date: snapshot.trade_date || null,
    snapshot_counts: snapshot.counts || {},
    running_tasks: runningTaskEntries,
    last_sync: (payload.last_sync || []).map((item) => ({
      sync_type: item.sync_type,
      status: item.status,
      last_time: item.last_time,
    })),
  });
  if (summarySignature === state.syncSummarySignature) {
    return;
  }
  state.syncSummarySignature = summarySignature;

  const runningTaskCount = runningTaskEntries.length;

  $("runningTaskSummary").textContent = runningTaskNames;
  $("runningTaskDetail").textContent = runningTaskCount
    ? `共 ${numberFormatter.format(runningTaskCount)} 个任务`
    : "无执行中的同步任务";
  renderTableVolumeSnapshot(snapshot);
}

async function loadSyncSummary() {
  if (state.syncSummaryInFlight) {
    return;
  }

  state.syncSummaryInFlight = true;
  const requestSeq = ++state.syncSummaryRequestSeq;
  try {
    const data = await fetchJson(api.syncStatus);
    if (requestSeq < state.syncSummaryAppliedSeq) {
      return;
    }
    state.syncSummaryAppliedSeq = requestSeq;
    applySyncSummaryPayload(data.data);
  } finally {
    state.syncSummaryInFlight = false;
  }
}

function renderSuggestions(items) {
  const box = $("suggestions");
  const signature = JSON.stringify(
    (items || []).map((item) => [item.stock_code, item.stock_name, item.market_type]),
  );
  if (signature === state.searchSuggestionsSignature) {
    box.hidden = !items.length;
    return;
  }
  state.searchSuggestionsSignature = signature;

  if (!items.length) {
    box.hidden = true;
    box.innerHTML = "";
    return;
  }

  box.hidden = false;
  box.innerHTML = items
    .map(
      (item) => `
        <button class="suggestion-item" type="button" data-code="${item.stock_code}">
          <span>
            <strong>${item.stock_name}</strong>
            <small class="muted">${item.stock_code}</small>
          </span>
          <small>${item.market_type}</small>
        </button>
      `
    )
    .join("");
}

function resetSuggestions() {
  renderSuggestions([]);
}

async function searchStocks(keyword) {
  const normalizedKeyword = keyword.trim();
  if (!normalizedKeyword) {
    if (state.searchAbortController) {
      state.searchAbortController.abort();
      state.searchAbortController = null;
    }
    resetSuggestions();
    return;
  }

  const isNumeric = /^\d+$/.test(normalizedKeyword);
  if ((isNumeric && normalizedKeyword.length < 3) || (!isNumeric && normalizedKeyword.length < 2)) {
    resetSuggestions();
    return;
  }

  if (state.searchCache.has(normalizedKeyword)) {
    renderSuggestions(state.searchCache.get(normalizedKeyword));
    return;
  }

  const requestSeq = ++state.searchRequestSeq;
  if (state.searchAbortController) {
    state.searchAbortController.abort();
  }
  const controller = new AbortController();
  state.searchAbortController = controller;

  try {
    const result = await fetchJson(`${api.stockSearch}?q=${encodeURIComponent(normalizedKeyword)}&limit=8`, {
      signal: controller.signal,
    });
    if (requestSeq < state.searchAppliedSeq || $("searchInput").value.trim() !== normalizedKeyword) {
      return;
    }
    state.searchAppliedSeq = requestSeq;
    const items = result.data.items || [];
    state.searchCache.set(normalizedKeyword, items);
    renderSuggestions(items);
  } catch (error) {
    if (error?.name === "AbortError") {
      return;
    }
    throw error;
  } finally {
    if (state.searchAbortController === controller) {
      state.searchAbortController = null;
    }
  }
}

function normalizeKlineRows(rows) {
  const parsed = [...rows].reverse().map((item) => ({
    tradeDate: item.trade_date,
    openPrice: parseNullableNumber(item.open_price),
    highPrice: parseNullableNumber(item.high_price),
    lowPrice: parseNullableNumber(item.low_price),
    closePrice: parseNullableNumber(item.close_price),
    volume: parseNullableNumber(item.volume),
    amount: parseNullableNumber(item.amount),
    turnoverRate: parseNullableNumber(item.turnover_rate),
    peRatio: parseNullableNumber(item.pe_ratio),
    pbRatio: parseNullableNumber(item.pb_ratio),
  }));

  return parsed.map((row, index) => {
    const previousClose = index > 0 ? parsed[index - 1].closePrice : null;
    const change = previousClose ? row.closePrice - previousClose : null;
    const pctChange = previousClose ? (change / previousClose) * 100 : null;
    const amplitude = previousClose ? ((row.highPrice - row.lowPrice) / previousClose) * 100 : null;

    const withIndicators = {
      ...row,
      previousClose,
      change,
      pctChange,
      amplitude,
    };

    for (const period of [5, 10, 20]) {
      if (index + 1 < period) {
        withIndicators[`ma${period}`] = null;
        continue;
      }
      const slice = parsed.slice(index + 1 - period, index + 1);
      const sum = slice.reduce((acc, current) => acc + current.closePrice, 0);
      withIndicators[`ma${period}`] = sum / period;
    }

    return withIndicators;
  });
}

function getVisibleChartRows() {
  if (!state.chartRows.length) {
    return [];
  }
  if (state.chartRange === null) {
    return state.chartRows;
  }
  return state.chartRows.slice(-Math.min(state.chartRange, state.chartRows.length));
}

function parseChartRangeValue(rawValue) {
  return rawValue === "all" ? null : Number(rawValue);
}

function updateRangeButtons() {
  document.querySelectorAll("#chartRangeSwitch [data-chart-range]").forEach((button) => {
    button.classList.toggle("active", parseChartRangeValue(button.dataset.chartRange) === state.chartRange);
  });
}

function chartTimeKey(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "string") {
    return value.slice(0, 10);
  }
  if (typeof value === "number") {
    return new Date(value * 1000).toISOString().slice(0, 10);
  }
  if (typeof value === "object" && "year" in value && "month" in value && "day" in value) {
    return `${value.year}-${String(value.month).padStart(2, "0")}-${String(value.day).padStart(2, "0")}`;
  }
  return null;
}

function buildChartSeriesData(rows) {
  return {
    candles: rows.map((row) => ({
      time: row.tradeDate,
      open: row.openPrice,
      high: row.highPrice,
      low: row.lowPrice,
      close: row.closePrice,
    })),
    volume: rows.map((row) => ({
      time: row.tradeDate,
      value: row.volume || 0,
      color: row.closePrice >= row.openPrice ? CHART_COLORS.up : CHART_COLORS.down,
    })),
    ma5: rows
      .filter((row) => row.ma5 !== null)
      .map((row) => ({ time: row.tradeDate, value: row.ma5 })),
    ma10: rows
      .filter((row) => row.ma10 !== null)
      .map((row) => ({ time: row.tradeDate, value: row.ma10 })),
    ma20: rows
      .filter((row) => row.ma20 !== null)
      .map((row) => ({ time: row.tradeDate, value: row.ma20 })),
  };
}

function queueChartFocusUpdate(row) {
  if (!row) {
    updateChartFocus(null);
    return;
  }

  state.pendingChartFocusRow = row;
  if (state.chartFocusFrame) {
    return;
  }

  state.chartFocusFrame = window.requestAnimationFrame(() => {
    state.chartFocusFrame = 0;
    const nextRow = state.pendingChartFocusRow;
    state.pendingChartFocusRow = null;
    if (!nextRow) {
      return;
    }
    if (nextRow.tradeDate === state.chartFocusTradeDate) {
      return;
    }
    updateChartFocus(nextRow);
  });
}

function handleChartCrosshairMove(param) {
  if (!state.chartRows.length) {
    return;
  }

  if (!param?.point || !param?.time) {
    queueChartFocusUpdate(state.chartRows.at(-1));
    return;
  }

  const row = state.chartRowsByTime.get(chartTimeKey(param.time));
  if (row) {
    queueChartFocusUpdate(row);
  }
}

function ensureChart() {
  if (state.chartApi) {
    return state.chartApi;
  }

  const container = $("priceChart");
  const chart = createChart(container, {
    autoSize: true,
    height: CHART_HEIGHT,
    layout: {
      background: {
        type: ColorType.Solid,
        color: "transparent",
      },
      textColor: CHART_COLORS.axis,
      fontFamily: "IBM Plex Sans, PingFang SC, Hiragino Sans GB, sans-serif",
      panes: {
        separatorColor: "rgba(17, 33, 42, 0.1)",
        separatorHoverColor: "rgba(17, 33, 42, 0.16)",
      },
    },
    grid: {
      vertLines: {
        color: CHART_COLORS.grid,
        style: LineStyle.Dotted,
      },
      horzLines: {
        color: CHART_COLORS.grid,
        style: LineStyle.Dotted,
      },
    },
    crosshair: {
      mode: CrosshairMode.MagnetOHLC,
      vertLine: {
        color: "rgba(17, 33, 42, 0.28)",
        style: LineStyle.SparseDotted,
        labelBackgroundColor: CHART_COLORS.ink,
      },
      horzLine: {
        color: "rgba(17, 33, 42, 0.28)",
        style: LineStyle.SparseDotted,
        labelBackgroundColor: CHART_COLORS.ink,
      },
    },
    rightPriceScale: {
      borderVisible: false,
      autoScale: true,
    },
    timeScale: {
      borderVisible: false,
      rightOffset: 3,
      barSpacing: 11,
      minBarSpacing: 4,
      fixLeftEdge: false,
      fixRightEdge: false,
      lockVisibleTimeRangeOnResize: false,
      allowShiftVisibleRangeOnWhitespaceReplacement: true,
      ticksVisible: true,
      timeVisible: true,
      secondsVisible: false,
    },
    handleScroll: {
      mouseWheel: true,
      pressedMouseMove: true,
      horzTouchDrag: true,
      vertTouchDrag: false,
    },
    handleScale: {
      mouseWheel: true,
      pinch: true,
      axisPressedMouseMove: true,
      axisDoubleClickReset: true,
    },
    localization: {
      locale: "zh-CN",
    },
  });

  const candles = chart.addSeries(CandlestickSeries, {
    upColor: CHART_COLORS.up,
    downColor: CHART_COLORS.down,
    wickUpColor: CHART_COLORS.up,
    wickDownColor: CHART_COLORS.down,
    borderUpColor: CHART_COLORS.up,
    borderDownColor: CHART_COLORS.down,
    lastValueVisible: true,
    priceLineVisible: true,
    priceLineWidth: 1,
    priceLineStyle: LineStyle.Dashed,
    priceLineColor: "rgba(17, 33, 42, 0.24)",
  });
  const ma5 = chart.addSeries(LineSeries, {
    color: CHART_COLORS.ma5,
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  });
  const ma10 = chart.addSeries(LineSeries, {
    color: CHART_COLORS.ma10,
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  });
  const ma20 = chart.addSeries(LineSeries, {
    color: CHART_COLORS.ma20,
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  });
  const volume = chart.addSeries(
    HistogramSeries,
    {
      priceFormat: {
        type: "volume",
      },
      lastValueVisible: false,
      priceLineVisible: false,
    },
    1,
  );

  chart.priceScale("right", 0).applyOptions({
    borderVisible: false,
    scaleMargins: {
      top: 0.08,
      bottom: 0.12,
    },
  });
  chart.priceScale("right", 1).applyOptions({
    borderVisible: false,
    scaleMargins: {
      top: 0.12,
      bottom: 0,
    },
  });

  const panes = chart.panes();
  if (panes[0]) {
    panes[0].setStretchFactor(0.74);
  }
  if (panes[1]) {
    panes[1].setStretchFactor(0.26);
  }

  chart.subscribeCrosshairMove(handleChartCrosshairMove);
  container.addEventListener("pointerleave", () => {
    if (state.chartRows.length) {
      queueChartFocusUpdate(state.chartRows.at(-1));
    }
  });

  state.chartApi = chart;
  state.chartSeries = { candles, volume, ma5, ma10, ma20 };
  return chart;
}

function clearChart() {
  if (!state.chartSeries) {
    return;
  }
  state.chartSeries.candles.setData([]);
  state.chartSeries.volume.setData([]);
  state.chartSeries.ma5.setData([]);
  state.chartSeries.ma10.setData([]);
  state.chartSeries.ma20.setData([]);
  state.chartRowsByTime = new Map();
}

function findExtremeRow(rows, key, mode = "max") {
  return rows.reduce((best, row) => {
    const value = row[key];
    if (value === null || value === undefined) {
      return best;
    }
    if (!best) {
      return row;
    }
    return mode === "min"
      ? (value < best[key] ? row : best)
      : (value > best[key] ? row : best);
  }, null);
}

function getActiveChartRangeLabel() {
  const activeButton = document.querySelector("#chartRangeSwitch .range-button.active");
  return activeButton?.textContent?.trim() || "当前窗口";
}

function resetChartSummary() {
  if (state.chartFocusFrame) {
    window.cancelAnimationFrame(state.chartFocusFrame);
    state.chartFocusFrame = 0;
  }
  state.pendingChartFocusRow = null;
  state.chartFocusTradeDate = null;
  state.chartVisibleStats = null;
  $("loadedRange").textContent = "--";
  $("loadedBars").textContent = "--";
  $("latestTradeDate").textContent = "--";
  $("latestSessionStatus").textContent = "最新可视交易日";
  $("windowReturn").textContent = "--";
  $("windowRangeNote").textContent = "--";
  $("rangeSpan").textContent = "--";
  $("rangeSpanNote").textContent = "--";
  $("yearHigh").textContent = "--";
  $("yearHighDate").textContent = "--";
  $("yearLow").textContent = "--";
  $("yearLowDate").textContent = "--";
  $("latestVolume").textContent = "--";
  $("latestTurnover").textContent = "--";
  $("latestAmount").textContent = "--";
  $("latestValuation").textContent = "--";
  $("ma5Value").textContent = "-";
  $("ma10Value").textContent = "-";
  $("ma20Value").textContent = "-";
  $("chartFocusDate").textContent = "--";
  $("chartFocusOpenPrev").textContent = "--";
  $("chartFocusCloseChange").textContent = "--";
  $("chartFocusHighLow").textContent = "--";
  $("chartFocusAmplitudeTurnover").textContent = "--";
  $("chartFocusVolume").textContent = "--";
  $("chartFocusAmount").textContent = "--";
  $("chartFocusValuation").textContent = "--";
  $("chartFocusMAFast").textContent = "--";
  $("chartFocusMASlow").textContent = "--";
  $("windowReturn").classList.remove("market-up", "market-down");
  $("chartFocusCloseChange").classList.remove("market-up", "market-down");
}

function updateSummaryStats(allRows, visibleRows) {
  const latest = allRows.at(-1);
  const loadedStart = allRows[0];
  const visibleStart = visibleRows[0];
  const visibleEnd = visibleRows.at(-1);
  const visibleHighRow = findExtremeRow(visibleRows, "highPrice", "max");
  const visibleLowRow = findExtremeRow(visibleRows, "lowPrice", "min");
  const yearRows = allRows.slice(-Math.min(YEAR_LOOKBACK_BARS, allRows.length));
  const yearHighRow = findExtremeRow(yearRows, "highPrice", "max");
  const yearLowRow = findExtremeRow(yearRows, "lowPrice", "min");
  const windowReturn = visibleStart?.closePrice
    ? ((visibleEnd.closePrice - visibleStart.closePrice) / visibleStart.closePrice) * 100
    : null;

  state.chartVisibleStats = {
    minPrice: visibleLowRow?.lowPrice ?? null,
    maxPrice: visibleHighRow?.highPrice ?? null,
  };

  $("loadedRange").textContent = `${loadedStart.tradeDate} → ${latest.tradeDate}`;
  $("loadedBars").textContent = `${numberFormatter.format(allRows.length)} 根日线`;
  $("latestTradeDate").textContent = latest.tradeDate;
  $("latestSessionStatus").textContent = `当前视窗 ${getActiveChartRangeLabel()}`;
  $("windowReturn").textContent = formatSignedPercent(windowReturn);
  $("windowRangeNote").textContent = `${visibleStart.tradeDate} → ${visibleEnd.tradeDate} · ${numberFormatter.format(visibleRows.length)} 根`;
  $("rangeSpan").textContent = `${formatMaybeNumber(visibleLowRow?.lowPrice)} / ${formatMaybeNumber(visibleHighRow?.highPrice)}`;
  $("rangeSpanNote").textContent = `最低 ${visibleLowRow?.tradeDate || "--"} · 最高 ${visibleHighRow?.tradeDate || "--"}`;
  $("yearHigh").textContent = formatMaybeNumber(yearHighRow?.highPrice);
  $("yearHighDate").textContent = yearHighRow?.tradeDate || "--";
  $("yearLow").textContent = formatMaybeNumber(yearLowRow?.lowPrice);
  $("yearLowDate").textContent = yearLowRow?.tradeDate || "--";
  $("latestVolume").textContent = formatCompact(latest.volume);
  $("latestTurnover").textContent = `换手 ${formatPercent(latest.turnoverRate)}`;
  $("latestAmount").textContent = formatCompact(latest.amount);
  $("latestValuation").textContent = `PE ${formatMaybeNumber(latest.peRatio)} / PB ${formatMaybeNumber(latest.pbRatio)}`;
  $("ma5Value").textContent = formatMaybeNumber(latest.ma5);
  $("ma10Value").textContent = formatMaybeNumber(latest.ma10);
  $("ma20Value").textContent = formatMaybeNumber(latest.ma20);
  applyToneClass($("windowReturn"), windowReturn);
}

function updateChartFocus(row) {
  if (!row) {
    state.chartFocusTradeDate = null;
    resetChartSummary();
    return;
  }

  state.chartFocusTradeDate = row.tradeDate;
  const visibleMin = state.chartVisibleStats?.minPrice;
  const visibleMax = state.chartVisibleStats?.maxPrice;
  const windowPosition = visibleMin !== null && visibleMax !== null && visibleMax > visibleMin
    ? ((row.closePrice - visibleMin) / (visibleMax - visibleMin)) * 100
    : null;

  $("chartFocusDate").textContent = row.tradeDate;
  $("chartFocusOpenPrev").textContent = `${formatMaybeNumber(row.openPrice)} / ${formatMaybeNumber(row.previousClose)}`;
  $("chartFocusCloseChange").textContent = `${formatMaybeNumber(row.closePrice)} / ${formatChangePair(row.change, row.pctChange)}`;
  $("chartFocusHighLow").textContent = `${formatMaybeNumber(row.highPrice)} / ${formatMaybeNumber(row.lowPrice)}`;
  $("chartFocusAmplitudeTurnover").textContent = `${formatPercent(row.amplitude)} / ${formatPercent(row.turnoverRate)}`;
  $("chartFocusVolume").textContent = formatCompact(row.volume);
  $("chartFocusAmount").textContent = formatCompact(row.amount);
  $("chartFocusValuation").textContent = `PE ${formatMaybeNumber(row.peRatio)} / PB ${formatMaybeNumber(row.pbRatio)}`;
  $("chartFocusMAFast").textContent = `${formatMaybeNumber(row.ma5)} / ${formatMaybeNumber(row.ma10)}`;
  $("chartFocusMASlow").textContent = `${formatMaybeNumber(row.ma20)} / ${formatPercent(windowPosition)}`;
  applyToneClass($("chartFocusCloseChange"), row.change);
}

function applyChartRange(totalBars) {
  if (!state.chartApi || !totalBars) {
    return;
  }

  if (state.chartRange === null) {
    state.chartApi.timeScale().fitContent();
    return;
  }

  const bars = Math.min(state.chartRange, totalBars);
  if (bars >= totalBars) {
    state.chartApi.timeScale().fitContent();
    return;
  }

  state.chartApi.timeScale().setVisibleLogicalRange({
    from: Math.max(totalBars - bars - 1.5, 0),
    to: totalBars + 1.5,
  });
}

function refreshChartViewport() {
  updateRangeButtons();
  if (!state.chartRows.length) {
    resetChartSummary();
    return;
  }

  const visibleRows = getVisibleChartRows();
  updateSummaryStats(state.chartRows, visibleRows.length ? visibleRows : state.chartRows);

  const focusedRow = state.chartFocusTradeDate
    ? state.chartRowsByTime.get(state.chartFocusTradeDate) || state.chartRows.at(-1)
    : state.chartRows.at(-1);
  queueChartFocusUpdate(focusedRow);
  window.requestAnimationFrame(() => applyChartRange(state.chartRows.length));
}

function renderChart(rows) {
  ensureChart();

  if (!rows.length) {
    clearChart();
    $("chartEmpty").hidden = false;
    resetChartSummary();
    return;
  }

  $("chartEmpty").hidden = true;
  state.chartRowsByTime = new Map(rows.map((row) => [chartTimeKey(row.tradeDate), row]));
  const data = buildChartSeriesData(rows);
  state.chartSeries.candles.setData(data.candles);
  state.chartSeries.volume.setData(data.volume);
  state.chartSeries.ma5.setData(data.ma5);
  state.chartSeries.ma10.setData(data.ma10);
  state.chartSeries.ma20.setData(data.ma20);
  refreshChartViewport();
}

function renderChartFromState() {
  renderChart(state.chartRows);
}

function deriveLatestFromRows(rows) {
  if (!rows.length) {
    return null;
  }
  const latest = rows.at(-1);
  return {
    trade_date: latest.tradeDate,
    close_price: latest.closePrice,
    change: latest.change,
    pct_change: latest.pctChange,
  };
}

function renderLatestPrice(latestData, rows) {
  const latest = latestData || deriveLatestFromRows(rows);

  if (!latest) {
    $("latestPrice").textContent = "--";
    $("latestChange").textContent = "暂无数据";
    $("latestPrice").classList.remove("market-up", "market-down");
    $("latestChange").classList.remove("market-up", "market-down");
    $("priceBadge").classList.remove("market-up", "market-down");
    return;
  }

  const changeValue = Number(latest.change || 0);
  $("latestPrice").textContent = formatMaybeNumber(latest.close_price);
  $("latestChange").textContent = formatChangePair(latest.change, latest.pct_change);
  $("latestPrice").classList.remove("market-up", "market-down");
  $("latestChange").classList.remove("market-up", "market-down");
  $("priceBadge").classList.remove("market-up", "market-down");
  const toneClass = priceToneClass(changeValue);
  if (toneClass) {
    $("latestPrice").classList.add(toneClass);
    $("latestChange").classList.add(toneClass);
    $("priceBadge").classList.add(toneClass);
  }
}

function formatNamedCode(name, code) {
  if (name && code) {
    return `${name}（${code}）`;
  }
  return code || name || "--";
}

function updateAnalysisTitles(name, code) {
  const identity = formatNamedCode(name, code);
  $("technicalTitle").textContent = `${identity} 技术面深度分析`;
  $("financialTitle").textContent = `${identity} 短线机会评分`;
}

function applyStockHeader(stock, fallbackCode) {
  const stockCode = stock?.stock_code || fallbackCode;
  const stockName = stock?.stock_name || "";
  const industry = stock?.industry_code ? stock.industry_code.split(":").pop() : "行业待补充";

  $("stockTitle").textContent = stockName ? `${stockName} · ${stockCode}` : stockCode;
  $("stockSubtitle").textContent = stock
    ? `市场 ${stock.market_type} / 上市 ${stock.list_date || "--"} / ${industry}`
    : "未找到股票基本信息";
  updateAnalysisTitles(stockName, stockCode);
}

function renderFactorScorecard(data, stockCode) {
  const container = $("financialSummary");
  if (!data) {
    container.innerHTML = `<div class="placeholder-card">${stockCode} 暂无短线机会评分数据。</div>`;
    return;
  }
  if (data.pending_refresh && !data.factors) {
    const identity = escapeHtml(formatNamedCode(data.stock_name, data.stock_code || stockCode));
    const tradeDate = data.trade_date || data.latest_market_trade_date || "--";
    container.innerHTML = `<div class="placeholder-card">${identity} 的短线机会评分正在后台刷新。当前市场最新交易日为 ${tradeDate}，请稍后自动重试。</div>`;
    return;
  }
  updateAnalysisTitles(data.stock_name, data.stock_code || stockCode);
  const totalScoreTone = scoreToneClass(Number(data.total_score || 0), 8, 3);
  const tierTone = /优先交易|重点关注/.test(data.tier_label || "")
    ? "positive"
    : data.is_watchlist
      ? ""
      : "negative";
  const triggerTone = data.trigger_ready ? "positive" : "negative";

  const factorCards = [
    {
      key: "setup",
      label: "背景趋势",
      score: Number(data.factors?.setup?.score || 0),
      maxScore: Number(data.factors?.setup?.max_score || 2),
      note: "收盘站上短均线，且 5/10/20 日均线抬升。",
      detail:
        hasValue(data.factors?.setup?.close_price) &&
        hasValue(data.factors?.setup?.ma5) &&
        hasValue(data.factors?.setup?.ma10) &&
        hasValue(data.factors?.setup?.ma20)
          ? `收盘 ${formatMaybeNumber(data.factors.setup.close_price)} / MA5 ${formatMaybeNumber(data.factors.setup.ma5)} / MA10 ${formatMaybeNumber(data.factors.setup.ma10)} / MA20 ${formatMaybeNumber(data.factors.setup.ma20)}`
          : "数据不足",
    },
    {
      key: "relative_strength",
      label: "相对强弱",
      score: Number(data.factors?.relative_strength?.score || 0),
      maxScore: Number(data.factors?.relative_strength?.max_score || 2),
      note: "近 5/10 日不只上涨，还要持续跑赢基准。",
      detail:
        hasValue(data.factors?.relative_strength?.return_5d) &&
        hasValue(data.factors?.relative_strength?.excess_return_5d)
          ? `5日 ${formatSignedDecimalPercent(data.factors.relative_strength.return_5d)} / ${escapeHtml(data.factors.relative_strength.benchmark_name || "基准")}超额 ${formatSignedDecimalPercent(data.factors.relative_strength.excess_return_5d)}`
          : "数据不足",
    },
    {
      key: "breakout",
      label: "突破位置",
      score: Number(data.factors?.breakout?.score || 0),
      maxScore: Number(data.factors?.breakout?.max_score || 2),
      note: "贴近或越过近 20 日突破位，但不能离均线太远。",
      detail:
        hasValue(data.factors?.breakout?.breakout_level) &&
        hasValue(data.factors?.breakout?.distance_to_breakout)
          ? `突破位 ${formatMaybeNumber(data.factors.breakout.breakout_level)} / 距离 ${formatSignedDecimalPercent(data.factors.breakout.distance_to_breakout)}`
          : "数据不足",
    },
    {
      key: "volume_trigger",
      label: "量价触发",
      score: Number(data.factors?.volume_trigger?.score || 0),
      maxScore: Number(data.factors?.volume_trigger?.max_score || 2),
      note: "最好出现涨时放量，且短期量能开始抬升。",
      detail:
        hasValue(data.factors?.volume_trigger?.latest_volume_ratio) &&
        hasValue(data.factors?.volume_trigger?.volume_ratio)
          ? `当日/5日均量 ${formatMaybeNumber(data.factors.volume_trigger.latest_volume_ratio, 2)}x / 5日量比 ${formatMaybeNumber(data.factors.volume_trigger.volume_ratio, 2)}x`
          : "数据不足",
    },
    {
      key: "risk_control",
      label: "风险回撤",
      score: Number(data.factors?.risk_control?.score || 0),
      maxScore: Number(data.factors?.risk_control?.max_score || 1),
      note: "一周内机会要有收益空间，也要有止损空间。",
      detail:
        hasValue(data.factors?.risk_control?.atr5_pct) &&
        hasValue(data.factors?.risk_control?.stop_distance)
          ? `ATR5/价 ${formatDecimalPercent(data.factors.risk_control.atr5_pct)} / 止损空间 ${formatDecimalPercent(data.factors.risk_control.stop_distance)}`
          : "数据不足",
    },
    {
      key: "liquidity",
      label: "流动性",
      score: Number(data.factors?.liquidity?.score || 0),
      maxScore: Number(data.factors?.liquidity?.max_score || 1),
      note: "成交额和换手都要足够，机会才能真正做进去。",
      detail:
        hasValue(data.factors?.liquidity?.avg_amount_20) &&
        hasValue(data.factors?.liquidity?.turnover_rate)
          ? `20日均额 ${formatCompact(data.factors.liquidity.avg_amount_20)} / 换手 ${formatPercent(data.factors.liquidity.turnover_rate)}`
          : "数据不足",
    },
  ];

  container.innerHTML = `
    ${data.pending_refresh ? `<p class="muted">评分卡正在后台刷新，当前先展示截至 ${escapeHtml(data.trade_date || "--")} 的缓存结果。</p>` : ""}
    <div class="factor-summary-grid">
      <div class="financial-item ${totalScoreTone}">
        <span>总分</span>
        <strong>${data.total_score} / ${data.max_score || 10}</strong>
      </div>
      <div class="financial-item ${tierTone}">
        <span>分层</span>
        <strong>${escapeHtml(data.tier_label || "--")}</strong>
      </div>
      <div class="financial-item ${triggerTone}">
        <span>触发状态</span>
        <strong>${data.trigger_ready ? "可优先盯盘" : "继续等待"}</strong>
      </div>
      <div class="financial-item">
        <span>评分日期</span>
        <strong>${data.trade_date || "--"}</strong>
      </div>
    </div>
    <div class="factor-grid">
      ${factorCards
        .map((item) => {
          const toneClass = item.score >= item.maxScore ? "pass" : item.score > 0 ? "" : "fail";
          const badgeClass = item.score >= item.maxScore ? "positive" : item.score > 0 ? "" : "negative";
          return `
            <article class="factor-card ${toneClass}">
              <div class="factor-card-head">
                <span>${item.label}</span>
                <strong class="signal-badge ${badgeClass}">${item.score} / ${item.maxScore}</strong>
              </div>
              <small>${item.note}</small>
              <p>${item.detail}</p>
            </article>
          `;
        })
        .join("")}
    </div>
    <div class="factor-grid">
      <article class="factor-card ${data.trigger_ready ? "pass" : ""}">
        <div class="factor-card-head">
          <span>执行提示</span>
          <strong class="signal-badge ${data.trigger_ready ? "positive" : ""}">${data.trigger_ready ? "就绪" : "等待"}</strong>
        </div>
        <small>短线评分只负责排序，不代替实际盘中确认。</small>
        <p>${escapeHtml(data.trigger_text || "等待更清晰的放量与突破信号。")}</p>
      </article>
    </div>
  `;
}

function renderTechnicalAnalysis(data, stockCode) {
  const container = $("technicalAnalysis");
  if (!data) {
    container.innerHTML = `<div class="placeholder-card">${stockCode} 暂无技术面分析数据。</div>`;
    return;
  }
  updateAnalysisTitles(data.stock_name, data.stock_code || stockCode);

  const score = Number(data.signal_summary?.score || 0);
  const maxScore = Number(data.signal_summary?.max_score || 5);
  const toneClass = scoreToneClass(score);
  const reasons = (data.signal_summary?.reasons || [])
    .filter(Boolean)
    .map((item) => `<span class="technical-chip positive">${escapeHtml(item)}</span>`)
    .join("");
  const sectionCards = [
    {
      title: "趋势结构",
      label: data.trend?.label || "--",
      tone: signalToneClass(data.trend?.label),
      metrics: [
        `收盘 ${formatMaybeNumber(data.trend?.close_price)}`,
        `MA20 ${formatMaybeNumber(data.trend?.ma20)}`,
        `MA60 ${formatMaybeNumber(data.trend?.ma60)}`,
        `MA120 ${formatMaybeNumber(data.trend?.ma120)}`,
      ],
      summary: data.trend?.summary || "数据不足",
    },
    {
      title: "强弱",
      label: data.strength?.label || "--",
      tone: signalToneClass(data.strength?.label),
      metrics: [
        `20日 ${formatSignedDecimalPercent(data.strength?.return_20d)}`,
        `60日 ${formatSignedDecimalPercent(data.strength?.return_60d)}`,
        `120日 ${formatSignedDecimalPercent(data.strength?.return_120d)}`,
        `距60日高 ${formatSignedDecimalPercent(data.strength?.distance_to_60d_high)}`,
      ],
      summary: data.strength?.summary || "数据不足",
    },
    {
      title: "相对强弱",
      label: data.relative_strength?.label || "--",
      tone: signalToneClass(data.relative_strength?.label),
      metrics: [
        `${data.relative_strength?.benchmark_name || "基准"}超额20日 ${formatSignedDecimalPercent(data.relative_strength?.excess_return_20d)}`,
        `${data.relative_strength?.benchmark_name || "基准"}超额60日 ${formatSignedDecimalPercent(data.relative_strength?.excess_return_60d)}`,
        `行业超额20日 ${formatSignedDecimalPercent(data.relative_strength?.industry_excess_return_20d)}`,
        `行业分位 ${formatDecimalPercent(data.relative_strength?.industry_percentile_20d)}`,
      ],
      summary: data.relative_strength?.summary || "数据不足",
    },
    {
      title: "量价",
      label: data.volume_price?.label || "--",
      tone: signalToneClass(data.volume_price?.label),
      metrics: [
        `5日均量 ${formatCompact(data.volume_price?.avg_volume_5)}`,
        `20日均量 ${formatCompact(data.volume_price?.avg_volume_20)}`,
        `量比 ${hasValue(data.volume_price?.volume_ratio) ? `${formatMaybeNumber(data.volume_price.volume_ratio, 2)}x` : "--"}`,
        `上涨量占比 ${formatDecimalPercent(data.volume_price?.up_day_volume_ratio_20d)}`,
      ],
      summary: data.volume_price?.summary || "数据不足",
    },
    {
      title: "波动风险",
      label: data.volatility_risk?.label || "--",
      tone: signalToneClass(data.volatility_risk?.label),
      metrics: [
        `ATR20 ${formatMaybeNumber(data.volatility_risk?.atr20, 3)}`,
        `ATR/价 ${formatDecimalPercent(data.volatility_risk?.atr20_pct)}`,
        `波动分位 ${formatDecimalPercent(data.volatility_risk?.atr_percentile_120d)}`,
        `布林宽度 ${formatDecimalPercent(data.volatility_risk?.boll_width_20d)}`,
      ],
      summary: data.volatility_risk?.summary || "数据不足",
    },
    {
      title: "关键位置",
      label: data.key_levels?.label || "--",
      tone: "",
      metrics: [
        `${escapeHtml(data.key_levels?.support_label || "支撑")} ${formatMaybeNumber(data.key_levels?.support_level)}`,
        `${escapeHtml(data.key_levels?.resistance_label || "压力")} ${formatMaybeNumber(data.key_levels?.resistance_level)}`,
        `20日低 ${formatMaybeNumber(data.key_levels?.low_20d)}`,
        `120日高 ${formatMaybeNumber(data.key_levels?.high_120d)}`,
      ],
      summary: data.key_levels?.summary || "数据不足",
    },
  ];

  container.innerHTML = `
    <article class="technical-hero ${toneClass}">
      <div class="technical-hero-head">
        <div>
          <span class="technical-kicker ${toneClass}">${escapeHtml(data.signal_summary?.bias || "--")}</span>
          <strong>${escapeHtml(data.signal_summary?.verdict || "暂无结论")}</strong>
        </div>
        <span class="technical-score ${toneClass}">${score} / ${maxScore}</span>
      </div>
      <div class="technical-plan-grid">
        <div class="technical-plan-item positive">
          <span>触发条件</span>
          <strong>${escapeHtml(data.signal_summary?.trigger || "--")}</strong>
        </div>
        <div class="technical-plan-item negative">
          <span>失效条件</span>
          <strong>${escapeHtml(data.signal_summary?.invalidation || "--")}</strong>
        </div>
      </div>
      <div class="technical-chip-row">
        <span class="technical-chip">截止 ${escapeHtml(data.trade_date || "--")}</span>
        <span class="technical-chip">样本 ${numberFormatter.format(data.bars_count || 0)} 根</span>
        ${reasons}
      </div>
    </article>
    <div class="technical-grid">
      ${sectionCards
        .map(
          (item) => `
            <article class="technical-card ${item.tone}">
              <div class="technical-card-head">
                <span>${item.title}</span>
                <strong class="signal-badge ${item.tone}">${escapeHtml(item.label)}</strong>
              </div>
              <small>${escapeHtml(item.metrics.filter(Boolean).join(" · "))}</small>
              <p>${escapeHtml(item.summary)}</p>
            </article>
          `
        )
        .join("")}
    </div>
  `;
}

async function loadFactorScorecard(stockCode, requestId = state.selectionRequestId) {
  try {
    const result = await fetchJson(api.analysisScoreDetail(stockCode));
    if (requestId !== state.selectionRequestId) {
      return;
    }
    renderFactorScorecard(result.data, stockCode);
  } catch (error) {
    if (requestId !== state.selectionRequestId) {
      return;
    }
    renderFactorScorecard(null, stockCode);
  }
}

async function loadTechnicalAnalysis(stockCode, requestId = state.selectionRequestId) {
  try {
    const result = await fetchJson(api.analysisTechnicalDetail(stockCode));
    if (requestId !== state.selectionRequestId) {
      return;
    }
    renderTechnicalAnalysis(result.data, stockCode);
  } catch (error) {
    if (requestId !== state.selectionRequestId) {
      return;
    }
    renderTechnicalAnalysis(null, stockCode);
  }
}

async function selectStock(stockCode) {
  const requestId = ++state.selectionRequestId;
  state.selectedCode = stockCode;
  $("searchInput").value = stockCode;
  $("stockTitle").textContent = stockCode;
  $("stockSubtitle").textContent = "正在读取股票基本信息";
  updateAnalysisTitles("", stockCode);

  const stockInfoPromise = fetchJson(api.stockInfo(stockCode));
  const latestPromise = fetchJson(api.latestPrice(stockCode));
  const klinePromise = fetchJson(api.dailyKline(stockCode, CHART_FETCH_LIMIT));

  try {
    const stock = await stockInfoPromise;
    if (requestId === state.selectionRequestId) {
      applyStockHeader(stock, stockCode);
    }
  } catch (error) {
    if (requestId === state.selectionRequestId) {
      applyStockHeader(null, stockCode);
    }
  }

  const [latestResult, klineResult] = await Promise.allSettled([latestPromise, klinePromise]);

  if (requestId !== state.selectionRequestId) {
    return;
  }

  if (klineResult.status === "fulfilled") {
    state.chartRows = normalizeKlineRows(klineResult.value.data || []);
    renderChartFromState();
  } else {
    state.chartRows = [];
    renderChartFromState();
  }

  const latestPayload = latestResult.status === "fulfilled" ? latestResult.value : null;
  renderLatestPrice(latestPayload, state.chartRows);

  await Promise.all([
    loadFactorScorecard(stockCode, requestId),
    loadTechnicalAnalysis(stockCode, requestId),
  ]);
}

function renderObservationPool(data) {
  const container = $("watchlist");
  const items = data?.items || [];
  if (!items.length) {
    if (data?.pending_refresh) {
      const tradeDate = data?.latest_market_trade_date || data?.trade_date || "--";
      container.innerHTML = `<div class="placeholder-card">短线机会评分正在后台刷新，当前市场最新交易日为 ${tradeDate}，请稍后自动更新。</div>`;
      return;
    }
    container.innerHTML = `<div class="placeholder-card">当前没有达到 ${data?.min_score ?? 6} 分的短线机会。</div>`;
    return;
  }

  container.innerHTML = items
    .map(
      (item, index) => `
        <button class="watchlist-item" type="button" data-code="${item.stock_code}">
          <div class="watchlist-head">
            <span class="watchlist-meta">
              <span class="watchlist-title-line">
                <strong class="watchlist-rank">#${index + 1}</strong>
                <strong class="watchlist-name">${escapeHtml(item.stock_name)} <span class="watchlist-inline-code">${item.stock_code} · ${escapeHtml(item.market_type || "--")}</span></strong>
              </span>
            </span>
            <strong class="factor-score-total ${item.trigger_ready ? "positive" : ""}">${numberFormatter.format(item.total_score || 0)} / 10</strong>
          </div>
          <div class="watchlist-tail">
            <span>${escapeHtml(item.tier_label || "观察")} · ${escapeHtml((item.passed_factors || []).join(" / ") || "继续等待")}</span>
            <span>5日超额 ${formatSignedDecimalPercent(item.excess_return_5d)}</span>
          </div>
          <div class="watchlist-tail">
            <span>距突破 ${formatSignedDecimalPercent(item.distance_to_breakout)}</span>
            <span>量比 ${item.volume_ratio === null || item.volume_ratio === undefined ? "--" : `${formatMaybeNumber(item.volume_ratio, 2)}x`} / 换手 ${formatPercent(item.turnover_rate)}</span>
          </div>
        </button>
      `
    )
    .join("");

  container.querySelectorAll("[data-code]").forEach((button) => {
    button.addEventListener("click", () => selectStock(button.dataset.code));
  });
}

async function loadObservationPool() {
  try {
    const result = await fetchJson(`${api.analysisScorecard}?limit=12&min_score=6`);
    renderObservationPool(result.data);
  } catch (error) {
    $("watchlist").innerHTML = `<div class="placeholder-card">短线机会池加载失败。</div>`;
  }
}

function renderMarketOverview(data) {
  const container = $("marketOverview");
  if (!data) {
    container.innerHTML = `<div class="placeholder-card">市场结构加载失败。</div>`;
    return;
  }

  const sentiment = data.sentiment;
  if (!sentiment) {
    if (data.pending_refresh) {
      const tradeDate = data.latest_market_trade_date || data.trade_date || "--";
      container.innerHTML = `<div class="placeholder-card">市场结构正在后台刷新，当前市场最新交易日为 ${tradeDate}，请稍后自动更新。</div>`;
      return;
    }
    container.innerHTML = `<div class="placeholder-card">暂无市场结构数据。</div>`;
    return;
  }

  const sentimentTone = scoreToneClass(Number(sentiment.sentiment_score || 0), 60, 30);
  const fundFlow = data.fund_flow?.market || null;
  const fundFlowTone = Number(fundFlow?.main_net_inflow || 0) > 0
    ? "positive"
    : Number(fundFlow?.main_net_inflow || 0) < 0
      ? "negative"
      : "";
  const leaders = data.events?.leaders || [];
  const failedLimits = data.events?.failed_limits || [];
  const benchmarks = data.benchmarks || [];
  const sectors = data.sectors || [];
  const fundIndustries = data.fund_flow?.industries || [];
  const benchmarkTags = benchmarks.length
    ? benchmarks
        .map(
          (item) => `
            <span class="financial-item overview-benchmark-tag ${Number(item.pct_change || 0) > 0 ? "positive" : Number(item.pct_change || 0) < 0 ? "negative" : ""}">
              <span>${escapeHtml(item.index_name || item.index_code)}</span>
              <strong class="overview-benchmark-change ${priceToneClass(item.pct_change)}">${formatSignedPercent(item.pct_change)}</strong>
              <small>5日 ${formatSignedDecimalPercent(item.return_5d)}</small>
            </span>
          `
        )
        .join("")
    : `<span class="financial-item overview-benchmark-tag overview-benchmark-tag-muted">暂无指数强弱数据</span>`;

  const renderEventRows = (items, emptyText, toneResolver) =>
    items.length
      ? items
          .map((item) => {
            const tone = toneResolver(item);
            const extraLabel = item.event_type === "failed_limit_up"
              ? `回落 ${item.event_value === null || item.event_value === undefined ? "--" : formatDecimalPercent(Math.abs(item.event_value))}`
              : `${item.consecutive_days >= 2 ? `${item.consecutive_days} 连板` : "首板"} / ${formatSignedPercent(item.pct_change)}`;
            return `
              <div class="overview-event-row">
                <span>
                  <strong>${escapeHtml(item.stock_name)}</strong>
                  <small>${item.stock_code} · ${escapeHtml(item.sector_name || "--")}</small>
                </span>
                <span class="signal-badge ${tone}">${escapeHtml(item.event_label || "--")}</span>
                <small>${escapeHtml(item.note || extraLabel)}</small>
              </div>
            `;
          })
          .join("")
      : `<div class="placeholder-card overview-placeholder">${emptyText}</div>`;

  container.innerHTML = `
    ${data.pending_refresh ? `<p class="muted">市场结构正在后台刷新，当前先展示截至 ${escapeHtml(data.trade_date || "--")} 的缓存结果。</p>` : ""}
    <div class="overview-summary-grid">
      <div class="financial-item ${sentimentTone}">
        <span>情绪温度</span>
        <strong>${sentiment.sentiment_score} / 100 · ${escapeHtml(sentiment.sentiment_label || "--")}</strong>
      </div>
      <div class="financial-item ${Number(sentiment.rising_count || 0) >= Number(sentiment.falling_count || 0) ? "positive" : "negative"}">
        <span>涨跌家数</span>
        <strong>${numberFormatter.format(sentiment.rising_count || 0)} / ${numberFormatter.format(sentiment.falling_count || 0)}</strong>
      </div>
      <div class="financial-item ${Number(sentiment.limit_up_count || 0) >= Number(sentiment.failed_limit_count || 0) ? "positive" : "negative"}">
        <span>涨停 / 炸板 / 跌停</span>
        <strong>${numberFormatter.format(sentiment.limit_up_count || 0)} / ${numberFormatter.format(sentiment.failed_limit_count || 0)} / ${numberFormatter.format(sentiment.limit_down_count || 0)}</strong>
      </div>
      <div class="financial-item ${fundFlowTone}">
        <span>主力净流入</span>
        <strong>${formatMoneyFlow(fundFlow?.main_net_inflow)}${fundFlow?.main_net_inflow_ratio === null || fundFlow?.main_net_inflow_ratio === undefined ? "" : ` · ${formatSignedPercent(fundFlow.main_net_inflow_ratio)}`}</strong>
      </div>
      ${benchmarkTags}
    </div>
    <article class="overview-note ${sentimentTone}">
      <strong>${escapeHtml(sentiment.summary || "暂无市场结论。")}</strong>
      <small>最新交易日 ${escapeHtml(data.trade_date || "--")} · 5日均值 ${formatMaybeNumber(sentiment.score_avg_5d, 1)} · 较前一日 ${formatSignedNumber(sentiment.score_change_1d, 0)}</small>
    </article>
    <div class="overview-grid overview-grid-triple">
      <section class="overview-block">
        <div class="overview-block-head">
          <span>热点行业</span>
          <small>强度 + 趋势 + 资金</small>
        </div>
        <div class="overview-sector-grid">
          ${sectors.length
            ? sectors
                .map((item) => {
                  const sectorTone = scoreToneClass(Number(item.strength_score || 0), 60, 35);
                  const risingRatio = item.stock_count ? (Number(item.rising_count || 0) / Number(item.stock_count || 1)) * 100 : null;
                  return `
                    <article class="overview-sector-card ${sectorTone}">
                      <div class="overview-sector-head">
                        <span>${escapeHtml(item.sector_name || "--")}</span>
                        <strong class="signal-badge ${sectorTone}">${item.strength_score}</strong>
                      </div>
                      <small>${formatSignedPercent(item.avg_pct_change)} · 5日 ${formatSignedDecimalPercent(item.avg_return_5d)} · 上方 ${formatDecimalPercent(item.above_ma20_ratio)}</small>
                      <p>上涨 ${formatMaybeNumber(risingRatio, 0)}% / 涨停 ${numberFormatter.format(item.limit_up_count || 0)} / 主力 ${formatMoneyFlow(item.main_net_inflow)}</p>
                      <small>龙头 ${escapeHtml(item.leading_stock_name || "--")} ${item.leading_stock_code ? `· ${item.leading_stock_code}` : ""}</small>
                    </article>
                  `;
                })
                .join("")
            : `<div class="placeholder-card overview-placeholder">暂无行业强度数据。</div>`}
        </div>
      </section>
      <section class="overview-block">
        <div class="overview-block-head">
          <span>资金流前排</span>
          <small>${escapeHtml(fundFlow?.trade_date || data.trade_date || "--")}</small>
        </div>
        <div class="overview-event-list">
          ${fundIndustries.length
            ? fundIndustries
                .map(
                  (item) => `
                    <div class="overview-event-row">
                      <span>
                        <strong>${escapeHtml(item.sector_name || "--")}</strong>
                        <small>${item.leading_stock_name ? `主力偏好 ${escapeHtml(item.leading_stock_name)}` : "行业资金流"}</small>
                      </span>
                      <span class="signal-badge ${Number(item.main_net_inflow || 0) > 0 ? "positive" : Number(item.main_net_inflow || 0) < 0 ? "negative" : ""}">${formatMoneyFlow(item.main_net_inflow)}</span>
                      <small>${formatSignedPercent(item.pct_change)} · 占比 ${formatSignedPercent(item.main_net_inflow_ratio)}</small>
                    </div>
                  `
                )
                .join("")
            : `<div class="placeholder-card overview-placeholder">暂无行业资金流数据。</div>`}
        </div>
      </section>
      <section class="overview-block">
        <div class="overview-block-head">
          <span>连板与炸板</span>
          <small>${numberFormatter.format(data.events?.consecutive_count || 0)} 只连板股 · 最高 ${numberFormatter.format(data.events?.max_consecutive_days || 0)} 板</small>
        </div>
        <div class="overview-event-columns">
          <div class="overview-event-list">
            <p class="overview-subtitle">连板前排</p>
            ${renderEventRows(leaders, "今日暂无连板股。", (item) => (Number(item.consecutive_days || 0) >= 2 ? "positive" : ""))}
          </div>
          <div class="overview-event-list">
            <p class="overview-subtitle">炸板观察</p>
            ${renderEventRows(failedLimits, "今日暂无明显炸板。", () => "negative")}
          </div>
        </div>
      </section>
    </div>
  `;
}

async function loadMarketOverview() {
  try {
    const result = await fetchJson(api.analysisMarketOverview);
    renderMarketOverview(result.data);
  } catch (error) {
    $("marketOverview").innerHTML = `<div class="placeholder-card">市场结构加载失败。</div>`;
  }
}

async function triggerSync(url, message, taskName) {
  state.pendingTasks[taskName] = true;
  updateTaskControls();
  setActionMessage(`${message}中...`);
  try {
    const result = await fetchJson(url, { method: "POST" });
    if (result.code && result.code >= 400) {
      setActionMessage(result.message, result.code >= 500 ? "negative" : "neutral");
      return;
    }
    setActionMessage(result.message, "positive");
    await new Promise((resolve) => setTimeout(resolve, 600));
    await refreshDashboard({
      includeObservationPool: true,
      includeSelectedStock: true,
    });
  } catch (error) {
    setActionMessage(`执行失败: ${error.message}`, "negative");
  } finally {
    delete state.pendingTasks[taskName];
    updateTaskControls();
  }
}

async function refreshDashboard({
  includeObservationPool = false,
  includeSelectedStock = false,
} = {}) {
  const tasks = [
    loadHealth().catch(() => null),
    loadSyncSummary().catch(() => null),
    loadMarketOverview().catch(() => null),
  ];

  if (includeObservationPool) {
    tasks.push(loadObservationPool().catch(() => null));
  }
  if (includeSelectedStock) {
    tasks.push(selectStock(state.selectedCode).catch(() => null));
  }

  await Promise.all(tasks);
}

function bindEvents() {
  $("searchForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const keyword = $("searchInput").value.trim();
    if (!keyword) {
      return;
    }

    if (/^\d{6}$/.test(keyword)) {
      await selectStock(keyword);
      resetSuggestions();
      return;
    }

    try {
      const result = await fetchJson(`${api.stockSearch}?q=${encodeURIComponent(keyword)}&limit=1`);
      const target = result.data.items?.[0];
      if (target) {
        await selectStock(target.stock_code);
        resetSuggestions();
      } else {
        setActionMessage("未找到匹配股票", "negative");
      }
    } catch (error) {
      setActionMessage(`搜索失败: ${error.message}`, "negative");
    }
  });

  $("suggestions").addEventListener("click", (event) => {
    const button = event.target.closest("[data-code]");
    if (!button) {
      return;
    }
    selectStock(button.dataset.code);
    resetSuggestions();
  });

  $("searchInput").addEventListener("input", (event) => {
    if (state.isSearchComposing) {
      return;
    }
    const keyword = event.target.value;
    clearTimeout(state.searchTimer);
    state.searchTimer = setTimeout(() => {
      searchStocks(keyword).catch(() => resetSuggestions());
    }, SEARCH_DEBOUNCE_MS);
  });

  $("searchInput").addEventListener("compositionstart", () => {
    state.isSearchComposing = true;
    clearTimeout(state.searchTimer);
  });

  $("searchInput").addEventListener("compositionend", (event) => {
    state.isSearchComposing = false;
    clearTimeout(state.searchTimer);
    state.searchTimer = setTimeout(() => {
      searchStocks(event.target.value).catch(() => resetSuggestions());
    }, 120);
  });

  document.addEventListener("click", (event) => {
    if (!event.target.closest(".search-form") && !event.target.closest(".suggestions")) {
      resetSuggestions();
    }
  });

  $("syncDailyBtn").addEventListener("click", () => triggerSync(api.syncDaily, "最新日线同步", "daily_kline"));
  $("syncMissingBtn").addEventListener("click", () => triggerSync(api.syncFullRefresh, "全量补齐更新", "full_refresh"));

  document.querySelectorAll("#chartRangeSwitch [data-chart-range]").forEach((button) => {
    button.addEventListener("click", () => {
      state.chartRange = parseChartRangeValue(button.dataset.chartRange);
      refreshChartViewport();
    });
  });

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      return;
    }
    loadSyncSummary().catch(() => null);
    loadMarketOverview().catch(() => null);
    loadObservationPool().catch(() => null);
    loadFactorScorecard(state.selectedCode).catch(() => null);
    loadTechnicalAnalysis(state.selectedCode).catch(() => null);
  });
}

async function initialize() {
  updateTaskControls();
  updateRangeButtons();
  resetChartSummary();
  await Promise.all([loadHealth(), selectStock(state.selectedCode), loadObservationPool(), loadMarketOverview()]);
  loadSyncSummary().catch(() => null);
}

bindEvents();
state.syncSummaryTimer = window.setInterval(() => {
  if (document.hidden) {
    return;
  }
  loadSyncSummary().catch(() => null);
  loadMarketOverview().catch(() => null);
  loadObservationPool().catch(() => null);
  loadFactorScorecard(state.selectedCode).catch(() => null);
  loadTechnicalAnalysis(state.selectedCode).catch(() => null);
}, SYNC_SUMMARY_INTERVAL_MS);
initialize().catch((error) => {
  setActionMessage(`初始化失败: ${error.message}`, "negative");
});
