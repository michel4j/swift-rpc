/**
 * Methods page controller (Methods C — Issue #21).
 *
 * Pure, DOM-independent helpers (`computeMethodStats`, `filterAndSortMethods`,
 * `errorRateBadgeClass`, `computeWhiskerGeometry`) are exported for direct
 * unit testing under `node --test`.
 *
 * View lifecycle exports: `mount(container, ctx)`, `update(ctx)`, `unmount()`.
 */

import {
  compareIgnoreCase,
  escapeHtml,
  formatBytes,
  formatDuration,
  toFiniteNumber,
} from '../format.js';

const VALID_SORT_COLS = new Set([
  'method',
  'count',
  'error_rate',
  'avg_duration',
  'p95_duration',
  'total_bytes',
]);

const COLUMNS = [
  { key: 'method', label: 'Method', align: 'start', defaultDir: 'asc' },
  { key: 'count', label: 'Calls', align: 'end', defaultDir: 'desc' },
  { key: 'error_rate', label: 'Error %', align: 'end', defaultDir: 'desc' },
  { key: 'avg_duration', label: 'Avg', align: 'end', defaultDir: 'desc' },
  { key: 'p95_duration', label: 'p95', align: 'end', defaultDir: 'desc' },
  { key: null, label: 'Latency', align: 'start', defaultDir: null },
  { key: 'total_bytes', label: 'Payload', align: 'end', defaultDir: 'desc' },
];

function getMethodErrorRatePct(method) {
  if (!method || typeof method !== 'object') {
    return 0;
  }
  const count = Math.max(0, toFiniteNumber(method.count, 0));
  if (method.errors !== undefined && method.errors !== null) {
    const errors = Math.max(0, toFiniteNumber(method.errors, 0));
    return count > 0 ? (errors / count) * 100 : 0;
  }
  return Math.max(0, toFiniteNumber(method.error_rate, 0));
}

function formatPercent(pct) {
  const val = toFiniteNumber(pct, 0);
  if (val <= 0) return '0%';
  if (val < 0.1) return '<0.1%';
  return `${val.toFixed(1).replace(/\.0$/, '')}%`;
}

/**
 * Aggregate window-level KPI statistics across method summary rows.
 * @param {Array<object>} methods
 * @returns {{ totalCalls: number, totalErrors: number, errorRatePct: number, worstP95: number, totalBytes: number }}
 */
export function computeMethodStats(methods) {
  const list = Array.isArray(methods) ? methods : [];
  let totalCalls = 0;
  let totalErrors = 0;
  let worstP95 = 0;
  let totalBytes = 0;

  for (const item of list) {
    if (!item || typeof item !== 'object') continue;
    const count = Math.max(0, toFiniteNumber(item.count, 0));
    const errors = Math.max(0, toFiniteNumber(item.errors, 0));
    const p95 = Math.max(0, toFiniteNumber(item.p95_duration, 0));
    const bytes = Math.max(0, toFiniteNumber(item.total_bytes, 0));

    totalCalls += count;
    totalErrors += errors;
    if (p95 > worstP95) {
      worstP95 = p95;
    }
    totalBytes += bytes;
  }

  const errorRatePct = totalCalls > 0 ? (totalErrors / totalCalls) * 100 : 0;
  return { totalCalls, totalErrors, errorRatePct, worstP95, totalBytes };
}

/**
 * Map an error-rate percentage to a Tabler soft badge class.
 * `0` -> `'bg-green-lt'`, `>0 && <=5` -> `'bg-yellow-lt'`, `>5` -> `'bg-red-lt'`.
 * @param {number} errorRatePct
 * @returns {'bg-green-lt' | 'bg-yellow-lt' | 'bg-red-lt'}
 */
export function errorRateBadgeClass(errorRatePct) {
  const pct = toFiniteNumber(errorRatePct, 0);
  if (pct > 5) return 'bg-red-lt';
  if (pct > 0) return 'bg-yellow-lt';
  return 'bg-green-lt';
}

/**
 * Filter methods by case-insensitive substring query and sort by column/direction.
 * Returns a new array without mutating the input.
 * @param {Array<object>} methods
 * @param {{ query?: string, sortCol?: string, sortDir?: 'asc' | 'desc' }} [options]
 * @returns {Array<object>}
 */
export function filterAndSortMethods(
  methods,
  { query = '', sortCol = 'count', sortDir = 'desc' } = {},
) {
  const list = Array.isArray(methods) ? methods : [];
  const needle = String(query ?? '').trim().toLowerCase();
  const col = VALID_SORT_COLS.has(sortCol) ? sortCol : 'count';
  const dir = sortDir === 'asc' ? 1 : -1;

  const filtered = needle
    ? list.filter((item) => String(item?.method ?? '').toLowerCase().includes(needle))
    : list.slice();

  filtered.sort((a, b) => {
    const aName = String(a?.method ?? '');
    const bName = String(b?.method ?? '');
    if (col === 'method') {
      return compareIgnoreCase(aName, bName) * dir;
    }

    const aVal = col === 'error_rate'
      ? getMethodErrorRatePct(a)
      : toFiniteNumber(a?.[col], 0);
    const bVal = col === 'error_rate'
      ? getMethodErrorRatePct(b)
      : toFiniteNumber(b?.[col], 0);

    if (aVal !== bVal) {
      return (aVal - bVal) * dir;
    }
    return compareIgnoreCase(aName, bName);
  });

  return filtered;
}

/**
 * Compute inline latency whisker bar geometry scaled `0..globalMaxDuration`.
 * @param {object} method
 * @param {number} globalMaxDuration
 * @returns {{ minPct: number, rangeWidthPct: number, avgPct: number, p95Pct: number }}
 */
export function computeWhiskerGeometry(method, globalMaxDuration) {
  const scale = toFiniteNumber(globalMaxDuration, 0);
  if (!method || typeof method !== 'object' || scale <= 0) {
    return { minPct: 0, rangeWidthPct: 0, avgPct: 0, p95Pct: 0 };
  }

  const clampPct = (val) => Math.max(0, Math.min(100, val));
  const minDur = Math.max(0, toFiniteNumber(method.min_duration, 0));
  const maxDur = Math.max(minDur, toFiniteNumber(method.max_duration, 0));
  const avgDur = Math.max(0, toFiniteNumber(method.avg_duration, 0));
  const p95Dur = Math.max(0, toFiniteNumber(method.p95_duration, 0));

  const minPct = clampPct((minDur / scale) * 100);
  const maxPct = clampPct((maxDur / scale) * 100);
  const rangeWidthPct = Math.max(0, maxPct - minPct);
  const avgPct = clampPct((avgDur / scale) * 100);
  const p95Pct = clampPct((p95Dur / scale) * 100);

  return { minPct, rangeWidthPct, avgPct, p95Pct };
}

// --- View controller state ---

let root = null;
let currentCtx = null;
let searchQuery = '';
let sortCol = 'count';
let sortDir = 'desc';
let els = null;
let lastSummaryRef = undefined;
let lastRenderKey = '';

function buildHeaderCellsHtml() {
  return COLUMNS.map((col) => {
    const alignCls = col.align === 'end' ? ' class="text-end"' : '';
    if (!col.key) {
      return `<th scope="col"${alignCls}>${escapeHtml(col.label)}</th>`;
    }
    const isSorted = col.key === sortCol;
    const ariaSort = isSorted ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none';
    const arrow = isSorted ? (sortDir === 'asc' ? '▲' : '▼') : '⇅';
    const indicatorCls = isSorted ? '' : ' opacity-50';
    return `
      <th scope="col"${alignCls} aria-sort="${ariaSort}" data-sort-th="${col.key}">
        <button type="button" class="sort-btn" data-sort-col="${col.key}">
          <span>${escapeHtml(col.label)}</span>
          <span class="small${indicatorCls}" data-sort-arrow aria-hidden="true">${arrow}</span>
        </button>
      </th>
    `;
  }).join('');
}

function buildShellHtml(win) {
  return `
    <div class="page-header d-print-none mb-3">
      <div class="row align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Methods</h1>
        </div>
        <div class="col-auto text-secondary small">
          Window: <span class="font-mono" data-methods-window>${escapeHtml(win)}</span>
        </div>
      </div>
    </div>

    <div class="row row-deck row-cards mb-3">
      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Window Calls</div>
            <div class="h2 mb-0 mt-1 font-mono" data-kpi-calls>—</div>
            <div class="text-secondary small mt-1" data-kpi-calls-sub>&nbsp;</div>
          </div>
        </div>
      </div>
      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Error Rate</div>
            <div class="d-flex align-items-baseline gap-2 mt-1">
              <div class="h2 mb-0 font-mono" data-kpi-error-rate>—</div>
              <span class="badge d-none" data-kpi-error-badge></span>
            </div>
            <div class="text-secondary small mt-1" data-kpi-error-sub>&nbsp;</div>
          </div>
        </div>
      </div>
      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Worst p95</div>
            <div class="h2 mb-0 mt-1 font-mono" data-kpi-worst-p95>—</div>
            <div class="text-secondary small text-truncate mt-1" data-kpi-worst-p95-sub>&nbsp;</div>
          </div>
        </div>
      </div>
      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Total Payload</div>
            <div class="h2 mb-0 mt-1 font-mono" data-kpi-payload>—</div>
            <div class="text-secondary small mt-1" data-kpi-payload-sub>&nbsp;</div>
          </div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-header d-flex flex-wrap align-items-center justify-content-between gap-2">
        <div class="d-flex align-items-center gap-2">
          <h2 class="card-title mb-0">Method Statistics</h2>
          <span class="badge bg-secondary-lt font-mono" data-methods-count>0</span>
        </div>
        <div class="d-flex flex-wrap align-items-center gap-3 ms-auto">
          <div class="whisker-legend" aria-label="Latency whisker legend">
            <span>
              <span class="whisker-legend-swatch"
                    style="width: 14px; height: 8px; background-color: color-mix(in srgb, var(--tblr-primary) 28%, transparent); border-radius: 2px;"
                    aria-hidden="true"></span>min–max
            </span>
            <span>
              <span class="whisker-legend-swatch"
                    style="width: 2px; height: 12px; background-color: var(--tblr-primary); border-radius: 1px;"
                    aria-hidden="true"></span>avg
            </span>
            <span>
              <span class="whisker-legend-swatch"
                    style="width: 2px; height: 12px; background-color: var(--tblr-warning); border-radius: 1px;"
                    aria-hidden="true"></span>p95
            </span>
          </div>
          <div style="min-width: 190px;">
            <input type="search"
                   class="form-control form-control-sm"
                   data-methods-search
                   placeholder="Filter method..."
                   aria-label="Filter methods by name"
                   autocomplete="off">
          </div>
        </div>
      </div>

      <div class="table-responsive">
        <table class="table table-vcenter table-hover card-table mb-0">
          <thead>
            <tr>${buildHeaderCellsHtml()}</tr>
          </thead>
          <tbody data-methods-tbody></tbody>
        </table>
      </div>
    </div>
  `;
}

function updateSortHeaders() {
  if (!els) return;
  for (const th of els.sortHeaders) {
    const colKey = th.dataset.sortTh;
    const isSorted = colKey === sortCol;
    const ariaSort = isSorted ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none';
    th.setAttribute('aria-sort', ariaSort);
    const arrowEl = th.querySelector('[data-sort-arrow]');
    if (arrowEl) {
      arrowEl.textContent = isSorted ? (sortDir === 'asc' ? '▲' : '▼') : '⇅';
      arrowEl.classList.toggle('opacity-50', !isSorted);
    }
  }
}

function findWorstP95Method(methods) {
  let worst = null;
  let maxP95 = -1;
  for (const item of methods) {
    if (!item || typeof item !== 'object') continue;
    const p95 = Math.max(0, toFiniteNumber(item.p95_duration, 0));
    if (p95 > maxP95) {
      maxP95 = p95;
      worst = item.method || null;
    }
  }
  return worst;
}

function renderKpis(summary, allMethods, win) {
  if (!els) return;
  if (!summary) {
    els.kpiCalls.textContent = '—';
    els.kpiCallsSub.textContent = `Window: ${win}`;
    els.kpiErrorRate.textContent = '—';
    els.kpiErrorBadge.className = 'badge d-none';
    els.kpiErrorSub.textContent = 'Waiting for summary…';
    els.kpiWorstP95.textContent = '—';
    els.kpiWorstP95Sub.textContent = '\u00a0';
    els.kpiPayload.textContent = '—';
    els.kpiPayloadSub.textContent = '\u00a0';
    return;
  }

  const stats = computeMethodStats(allMethods);
  const methodCount = allMethods.length;
  const worstMethod = methodCount > 0 ? findWorstP95Method(allMethods) : null;

  els.kpiCalls.textContent = stats.totalCalls.toLocaleString();
  els.kpiCallsSub.textContent = `${methodCount} ${methodCount === 1 ? 'Method' : 'Methods'} in ${win}`;

  const errPctLabel = formatPercent(stats.errorRatePct);
  const badgeCls = errorRateBadgeClass(stats.errorRatePct);
  els.kpiErrorRate.textContent = errPctLabel;
  if (stats.totalCalls === 0) {
    els.kpiErrorBadge.className = 'badge d-none';
    els.kpiErrorBadge.textContent = '';
  } else {
    els.kpiErrorBadge.className = `badge ${badgeCls}`;
    els.kpiErrorBadge.textContent = stats.totalErrors === 0 ? 'Healthy' : (stats.errorRatePct > 5 ? 'High' : 'Warn');
  }
  els.kpiErrorSub.textContent = `${stats.totalErrors.toLocaleString()} ${stats.totalErrors === 1 ? 'error' : 'errors'}`;

  els.kpiWorstP95.textContent = methodCount > 0 ? formatDuration(stats.worstP95) : '-';
  els.kpiWorstP95Sub.textContent = worstMethod ? worstMethod : 'No completed Requests';
  els.kpiWorstP95Sub.title = worstMethod || '';

  els.kpiPayload.textContent = formatBytes(stats.totalBytes);
  els.kpiPayloadSub.textContent = `Sent in ${win}`;
}

function renderTableBody(summary, allMethods, visibleMethods, win) {
  if (!els) return;

  if (!summary) {
    els.tbody.innerHTML = `
      <tr>
        <td colspan="7" class="text-center py-5 text-secondary">
          Loading Method statistics…
        </td>
      </tr>
    `;
    return;
  }

  if (allMethods.length === 0) {
    els.tbody.innerHTML = `
      <tr>
        <td colspan="7" class="text-center py-5 text-secondary">
          <div class="fw-medium mb-1">No recorded Methods in the last ${escapeHtml(win)}</div>
          <div class="small">Try selecting a wider time window or wait for incoming Requests.</div>
        </td>
      </tr>
    `;
    return;
  }

  if (visibleMethods.length === 0) {
    els.tbody.innerHTML = `
      <tr>
        <td colspan="7" class="text-center py-5 text-secondary">
          <div class="fw-medium mb-1">No Methods matching &ldquo;${escapeHtml(searchQuery.trim())}&rdquo;</div>
          <div class="small mt-2">
            <button type="button" class="btn btn-sm btn-ghost-secondary" data-clear-method-filter>Clear filter</button>
          </div>
        </td>
      </tr>
    `;
    return;
  }

  const globalMaxDuration = visibleMethods.reduce((acc, item) => {
    const candidate = Math.max(
      toFiniteNumber(item?.max_duration, 0),
      toFiniteNumber(item?.p95_duration, 0),
      toFiniteNumber(item?.avg_duration, 0),
    );
    return candidate > acc ? candidate : acc;
  }, 0);

  const encodedWin = encodeURIComponent(win);

  els.tbody.innerHTML = visibleMethods.map((item) => {
    const methodName = String(item?.method ?? '');
    const count = Math.max(0, toFiniteNumber(item?.count, 0));
    const errors = Math.max(0, toFiniteNumber(item?.errors, 0));
    const errPct = getMethodErrorRatePct(item);
    const badgeCls = errorRateBadgeClass(errPct);
    const errPctLabel = formatPercent(errPct);
    const errTooltip = `${errors.toLocaleString()} ${errors === 1 ? 'error' : 'errors'} of ${count.toLocaleString()} ${count === 1 ? 'call' : 'calls'}`;

    const minLabel = formatDuration(item?.min_duration);
    const avgLabel = formatDuration(item?.avg_duration);
    const p95Label = formatDuration(item?.p95_duration);
    const maxLabel = formatDuration(item?.max_duration);
    const whiskerTooltip = `min ${minLabel} / avg ${avgLabel} / p95 ${p95Label} / max ${maxLabel}`;
    const geom = computeWhiskerGeometry(item, globalMaxDuration);

    const href = `/requests?window=${encodedWin}&method=${encodeURIComponent(methodName)}`;

    return `
      <tr>
        <td>
          <a href="${escapeHtml(href)}"
             class="font-mono fw-medium text-reset"
             data-method-drilldown="${escapeHtml(methodName)}"
             title="View Requests for ${escapeHtml(methodName)}">${escapeHtml(methodName)}</a>
        </td>
        <td class="num">${escapeHtml(count.toLocaleString())}</td>
        <td class="num" title="${escapeHtml(errTooltip)}">
          <span class="badge ${badgeCls} font-mono">${escapeHtml(errPctLabel)}</span>
          <span class="text-secondary small font-mono ms-1">(${escapeHtml(errors.toLocaleString())})</span>
        </td>
        <td class="num">${escapeHtml(avgLabel)}</td>
        <td class="num">${escapeHtml(p95Label)}</td>
        <td>
          <div class="latency-whisker"
               role="img"
               title="${escapeHtml(whiskerTooltip)}"
               aria-label="${escapeHtml(whiskerTooltip)}">
            <span class="latency-whisker-track" aria-hidden="true"></span>
            <span class="latency-whisker-range" aria-hidden="true"
                  style="left: ${geom.minPct.toFixed(2)}%; width: ${geom.rangeWidthPct.toFixed(2)}%;"></span>
            <span class="latency-whisker-avg" aria-hidden="true"
                  style="left: ${geom.avgPct.toFixed(2)}%;"></span>
            <span class="latency-whisker-p95" aria-hidden="true"
                  style="left: ${geom.p95Pct.toFixed(2)}%;"></span>
          </div>
        </td>
        <td class="num">${escapeHtml(formatBytes(item?.total_bytes))}</td>
      </tr>
    `;
  }).join('');
}

function render(force = false) {
  if (!root || !els || !currentCtx) return;

  const summary = currentCtx.state.summary || null;
  const win = currentCtx.state.window || '15m';
  const renderKey = `${win}|${searchQuery}|${sortCol}|${sortDir}`;

  if (!force && summary === lastSummaryRef && renderKey === lastRenderKey) {
    return;
  }
  lastSummaryRef = summary;
  lastRenderKey = renderKey;

  const allMethods = Array.isArray(summary?.methods) ? summary.methods : [];
  const visibleMethods = filterAndSortMethods(allMethods, {
    query: searchQuery,
    sortCol,
    sortDir,
  });

  els.windowLabel.textContent = win;
  els.countBadge.textContent = searchQuery.trim()
    ? `${visibleMethods.length} / ${allMethods.length}`
    : String(allMethods.length);

  renderKpis(summary, allMethods, win);
  updateSortHeaders();
  renderTableBody(summary, allMethods, visibleMethods, win);
}

function handleSearchInput(event) {
  searchQuery = event.target.value || '';
  render(true);
}

function handleRootClick(event) {
  const sortBtn = event.target.closest('button[data-sort-col]');
  if (sortBtn) {
    const colKey = sortBtn.dataset.sortCol;
    if (VALID_SORT_COLS.has(colKey)) {
      if (sortCol === colKey) {
        sortDir = sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        const colDef = COLUMNS.find((c) => c.key === colKey);
        sortCol = colKey;
        sortDir = colDef?.defaultDir || 'desc';
      }
      render(true);
    }
    return;
  }

  const clearBtn = event.target.closest('[data-clear-method-filter]');
  if (clearBtn) {
    searchQuery = '';
    if (els?.searchInput) {
      els.searchInput.value = '';
      els.searchInput.focus();
    }
    render(true);
    return;
  }

  const drillLink = event.target.closest('[data-method-drilldown]');
  if (drillLink) {
    if (
      event.metaKey
      || event.ctrlKey
      || event.shiftKey
      || event.altKey
      || event.button !== 0
    ) {
      return;
    }
    event.preventDefault();
    const method = drillLink.dataset.methodDrilldown;
    if (method && currentCtx && typeof currentCtx.navigate === 'function') {
      currentCtx.navigate('/requests', { method });
    }
  }
}

export function mount(container, ctx) {
  root = container;
  currentCtx = ctx;
  searchQuery = '';
  sortCol = 'count';
  sortDir = 'desc';
  lastSummaryRef = undefined;
  lastRenderKey = '';

  const win = ctx?.state?.window || '15m';
  root.innerHTML = buildShellHtml(win);

  els = {
    windowLabel: root.querySelector('[data-methods-window]'),
    kpiCalls: root.querySelector('[data-kpi-calls]'),
    kpiCallsSub: root.querySelector('[data-kpi-calls-sub]'),
    kpiErrorRate: root.querySelector('[data-kpi-error-rate]'),
    kpiErrorBadge: root.querySelector('[data-kpi-error-badge]'),
    kpiErrorSub: root.querySelector('[data-kpi-error-sub]'),
    kpiWorstP95: root.querySelector('[data-kpi-worst-p95]'),
    kpiWorstP95Sub: root.querySelector('[data-kpi-worst-p95-sub]'),
    kpiPayload: root.querySelector('[data-kpi-payload]'),
    kpiPayloadSub: root.querySelector('[data-kpi-payload-sub]'),
    countBadge: root.querySelector('[data-methods-count]'),
    searchInput: root.querySelector('[data-methods-search]'),
    sortHeaders: Array.from(root.querySelectorAll('th[data-sort-th]')),
    tbody: root.querySelector('[data-methods-tbody]'),
  };

  els.searchInput?.addEventListener('input', handleSearchInput);
  root.addEventListener('click', handleRootClick);

  render(true);
}

export function update(ctx) {
  currentCtx = ctx;
  render(false);
}

export function unmount() {
  if (els?.searchInput) {
    els.searchInput.removeEventListener('input', handleSearchInput);
  }
  if (root) {
    root.removeEventListener('click', handleRootClick);
  }
  root = null;
  currentCtx = null;
  els = null;
  lastSummaryRef = undefined;
  lastRenderKey = '';
}
