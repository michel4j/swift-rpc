/**
 * Requests page controller (Requests C — Issue #22).
 *
 * Top card: Live Active Requests (`card-status-top bg-primary`, 1s `/api/data`),
 * respecting active Method/Worker filters and hidden when 0 matching active Requests.
 * Bottom card: Paginated Historical Requests (2s `/api/calls`, page size 25),
 * with Status (`All`/`DONE`/`ERROR`), exact-match Method & Worker dropdowns,
 * Duration percentile dropdown (`All`/`P5`/`P95`), Clear button, and Prev/Next pagination.
 *
 * Pure, DOM-independent helpers are exported for direct unit testing under `node --test`.
 * View lifecycle exports: `mount(container, ctx)`, `update(ctx)`, `unmount()`.
 */

import {
  REQUESTS_PAGE_SIZE,
  compareIgnoreCase,
  computeElapsedSeconds,
  escapeHtml,
  extractRawMethod,
  formatBytes,
  formatCallTimestamp,
  formatDisplayMethod,
  formatDuration,
  normalizeDurationParam,
  normalizeStatusParam,
  statusBadgeClass,
  toFiniteNumber,
} from '../format.js';
import { buildUrl } from '../router.js';

export { REQUESTS_PAGE_SIZE, normalizeDurationParam, normalizeStatusParam };

/**
 * Filter live active Requests by exact-match `method` and `worker` filters
 * (consistent with `SqliteMonitorStore.get_calls` equality matching).
 * @param {Array<object>} activeCalls
 * @param {{ method?: string, worker?: string }} [filters]
 * @returns {Array<object>}
 */
export function filterActiveCalls(activeCalls, { method = '', worker = '' } = {}) {
  if (!Array.isArray(activeCalls)) return [];
  const methodFilter = String(method ?? '').trim();
  const workerFilter = String(worker ?? '').trim();

  return activeCalls.filter((call) => {
    if (!call || typeof call !== 'object') return false;
    if (methodFilter && extractRawMethod(call) !== methodFilter) {
      return false;
    }
    if (workerFilter && String(call.worker_id ?? '').trim() !== workerFilter) {
      return false;
    }
    return true;
  });
}

/**
 * Filter historical `/api/calls` rows according to the active status dropdown.
 * In `All` (`''`) view, flushed rows with `status === 'ACTIVE'` are excluded so
 * live Requests are never duplicated across the Active and Historical cards.
 * @param {Array<object>} calls
 * @param {string} [statusFilter='']
 * @returns {Array<object>}
 */
export function filterHistoricalCalls(calls, statusFilter = '') {
  if (!Array.isArray(calls)) return [];
  const normalized = normalizeStatusParam(statusFilter);
  if (!normalized) {
    return calls.filter(
      (call) => call && typeof call === 'object' && String(call.status ?? '').toUpperCase() !== 'ACTIVE',
    );
  }
  return calls.filter(
    (call) => call && typeof call === 'object' && String(call.status ?? '').toUpperCase() === normalized,
  );
}

/**
 * Collect deduplicated, sorted Method and Worker names for HTML `<datalist>` options
 * from `/api/data`, `/api/calls`, and any previously seen sets.
 * @param {{ data?: object|null, calls?: object|null, extraMethods?: Iterable<string>|null, extraWorkers?: Iterable<string>|null }} [sources]
 * @returns {{ methods: string[], workers: string[] }}
 */
export function collectKnownMethodsAndWorkers({
  data = null,
  calls = null,
  extraMethods = null,
  extraWorkers = null,
} = {}) {
  const methodSet = new Set();
  const workerSet = new Set();

  const addMethod = (val) => {
    if (typeof val === 'string') {
      const trimmed = val.trim();
      if (trimmed) methodSet.add(trimmed);
    }
  };
  const addWorker = (val) => {
    if (typeof val === 'string') {
      const trimmed = val.trim();
      if (trimmed) workerSet.add(trimmed);
    }
  };

  if (extraMethods) {
    for (const m of extraMethods) addMethod(m);
  }
  if (extraWorkers) {
    for (const w of extraWorkers) addWorker(w);
  }

  if (data && typeof data === 'object') {
    if (Array.isArray(data.methods)) {
      for (const m of data.methods) addMethod(m);
    }
    if (Array.isArray(data.workers)) {
      for (const w of data.workers) addWorker(w);
    }
    if (data.worker_methods && typeof data.worker_methods === 'object') {
      for (const [wid, list] of Object.entries(data.worker_methods)) {
        addWorker(wid);
        if (Array.isArray(list)) {
          for (const m of list) addMethod(m);
        }
      }
    }
    if (data.worker_metrics && typeof data.worker_metrics === 'object') {
      for (const wid of Object.keys(data.worker_metrics)) {
        addWorker(wid);
      }
    }
    for (const group of [data.active_calls, data.historical_calls]) {
      if (Array.isArray(group)) {
        for (const row of group) {
          addMethod(extractRawMethod(row));
          addWorker(row?.worker_id);
        }
      }
    }
  }

  const callRows = Array.isArray(calls?.calls) ? calls.calls : (Array.isArray(calls) ? calls : []);
  for (const row of callRows) {
    addMethod(extractRawMethod(row));
    addWorker(row?.worker_id);
  }

  return {
    methods: Array.from(methodSet).sort(compareIgnoreCase),
    workers: Array.from(workerSet).sort(compareIgnoreCase),
  };
}

/**
 * Compute pagination display bounds and Prev/Next button states.
 * @param {{ total?: number, offset?: number, limit?: number, displayedCount?: number }} [params]
 * @returns {{ start: number, end: number, total: number, hasPrev: boolean, hasNext: boolean, prevOffset: number, nextOffset: number }}
 */
export function computePaginationRange({
  total = 0,
  offset = 0,
  limit = REQUESTS_PAGE_SIZE,
  displayedCount = undefined,
} = {}) {
  const safeLimit = Math.max(1, Math.floor(toFiniteNumber(limit, REQUESTS_PAGE_SIZE)));
  const safeTotal = Math.max(0, Math.floor(toFiniteNumber(total, 0)));
  const safeOffset = Math.max(0, Math.floor(toFiniteNumber(offset, 0)));
  const rowCount = displayedCount !== undefined
    ? Math.max(0, Math.floor(toFiniteNumber(displayedCount, 0)))
    : Math.max(0, Math.min(safeLimit, safeTotal - safeOffset));

  const hasPrev = safeOffset > 0;
  const hasNext = safeOffset + safeLimit < safeTotal;
  const prevOffset = Math.max(0, safeOffset - safeLimit);
  const nextOffset = safeOffset + safeLimit;

  if (safeTotal === 0 || rowCount === 0) {
    return {
      start: 0,
      end: 0,
      total: safeTotal,
      hasPrev,
      hasNext,
      prevOffset,
      nextOffset,
    };
  }

  const start = safeOffset + 1;
  const end = safeOffset + rowCount;
  return {
    start,
    end,
    total: Math.max(safeTotal, end),
    hasPrev,
    hasNext,
    prevOffset,
    nextOffset,
  };
}

// --- View controller state ---

let root = null;
let currentCtx = null;
let els = null;
let lastActiveSig = null;
let activeElapsedMap = new Map();
let lastHistorySig = null;
let lastMethodsSelectSig = '';
let lastWorkersSelectSig = '';
let seenMethods = new Set();
let seenWorkers = new Set();
let pendingPagination = { prevOffset: 0, nextOffset: REQUESTS_PAGE_SIZE };

function buildRequestHref(ctx, reqId) {
  const query = { ...(ctx?.state?.query || {}), window: ctx?.state?.window || '15m', request: reqId };
  return buildUrl('/requests', query);
}

function buildShellHtml(win) {
  return `
    <div class="page-header d-print-none mb-3">
      <div class="row g-2 align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Requests</h1>
        </div>
        <div class="col-auto text-secondary small">
          Window: <span class="font-mono" data-requests-window>${escapeHtml(win)}</span>
        </div>
      </div>
    </div>
    
    <div class="card mb-3">
      <div class="card-header d-flex flex-wrap align-items-center justify-content-between gap-2">
        <div class="d-flex align-items-center gap-2">
          <h2 class="card-title mb-0">Historical Requests</h2>
          <span class="badge bg-secondary-lt font-mono" data-history-total>0</span>
        </div>
        <search class="requests-filter-bar ms-auto" aria-label="Filter Requests">
          <select class="form-select form-select-sm" data-filter-status aria-label="Filter by status">
            <option value="">All Statuses</option>
            <option value="DONE">DONE</option>
            <option value="ERROR">ERROR</option>
          </select>
          <select class="form-select form-select-sm font-mono" data-filter-method aria-label="Filter by Method">
            <option value="">All Methods</option>
          </select>
          <select class="form-select form-select-sm font-mono" data-filter-worker aria-label="Filter by Worker">
            <option value="">All Workers</option>
          </select>
          <select class="form-select form-select-sm" data-filter-duration aria-label="Filter by Duration">
            <option value="">All Durations</option>
            <option value="p5">Shortest (P5)</option>
            <option value="p95">Longest (P95)</option>
          </select>
          <button type="button"
                  class="btn btn-sm btn-ghost-secondary d-none"
                  data-clear-filters>Clear</button>
        </search>
      </div>

      <div class="table-responsive small">
        <table class="table table-vcenter table-hover card-table mb-0">
          <thead>
            <tr>
              <th scope="col">Request ID</th>
              <th scope="col">Status</th>
              <th scope="col">Method</th>
              <th scope="col">Worker</th>
              <th scope="col">Client</th>
              <th scope="col">Time</th>
              <th scope="col" class="text-end">Duration</th>
              <th scope="col" class="text-end">Updates</th>
              <th scope="col" class="text-end">Sent</th>
            </tr>
          </thead>
          <tbody data-history-tbody></tbody>
        </table>
      </div>

      <div class="card-footer d-flex flex-wrap align-items-center justify-content-between gap-2">
        <div class="text-secondary small font-mono" data-pagination-summary>Loading…</div>
        <div class="btn-group btn-group-sm" role="group" aria-label="Historical Requests pagination">
          <button type="button" class="btn btn-outline-secondary" data-page-prev disabled>Prev</button>
          <button type="button" class="btn btn-outline-secondary" data-page-next disabled>Next</button>
        </div>
      </div>
    </div>
    
    <div class="card mb-3 d-none" data-active-card>
      <div class="card-status-top bg-primary"></div>
      <div class="card-header">
        <div class="d-flex align-items-center gap-2 min-w-0">
          <span class="status-dot status-dot-animated bg-primary flex-shrink-0" aria-hidden="true"></span>
          <h2 class="card-title mb-0">Active Requests</h2>
          <span class="badge bg-blue-lt font-mono" data-active-count>0</span>
        </div>
        <div class="card-actions text-secondary small">Live</div>
      </div>
      <div class="table-responsive small">
        <table class="table table-vcenter table-hover card-table mb-0">
          <thead>
            <tr>
              <th scope="col">Request ID</th>
              <th scope="col">Method</th>
              <th scope="col">Worker</th>
              <th scope="col">Client</th>
              <th scope="col" class="text-end">Elapsed</th>
              <th scope="col" class="text-end">Updates</th>
              <th scope="col" class="text-end">Sent</th>
            </tr>
          </thead>
          <tbody data-active-tbody></tbody>
        </table>
      </div>
    </div>
  `;
}

function updateClearButtonVisibility(query) {
  if (!els) return;
  const hasFilter = Boolean(
    normalizeStatusParam(query?.status)
    || String(query?.method ?? '').trim()
    || String(query?.worker ?? '').trim()
    || normalizeDurationParam(query?.duration)
    || els.methodSelect.value.trim()
    || els.workerSelect.value.trim(),
  );
  els.clearBtn.classList.toggle('d-none', !hasFilter);
}

function syncControlsFromQuery(query) {
  if (!els) return;
  const status = normalizeStatusParam(query?.status);
  const method = String(query?.method ?? '').trim();
  const worker = String(query?.worker ?? '').trim();
  const duration = normalizeDurationParam(query?.duration);

  if (els.statusSelect.value !== status) {
    els.statusSelect.value = status;
  }
  if (els.methodSelect.value !== method) {
    els.methodSelect.value = method;
  }
  if (els.workerSelect.value !== worker) {
    els.workerSelect.value = worker;
  }
  if (els.durationSelect.value !== duration) {
    els.durationSelect.value = duration;
  }
  updateClearButtonVisibility(query);
}

function patchFilterSelects(data, calls, query) {
  if (!els) return;
  const currentMethod = String(query?.method ?? '').trim();
  const currentWorker = String(query?.worker ?? '').trim();

  const { methods, workers } = collectKnownMethodsAndWorkers({
    data,
    calls,
    extraMethods: currentMethod ? [currentMethod, ...seenMethods] : seenMethods,
    extraWorkers: currentWorker ? [currentWorker, ...seenWorkers] : seenWorkers,
  });

  for (const m of methods) seenMethods.add(m);
  for (const w of workers) seenWorkers.add(w);

  const methodsSig = JSON.stringify(methods);
  if (methodsSig !== lastMethodsSelectSig) {
    lastMethodsSelectSig = methodsSig;
    els.methodSelect.innerHTML = '<option value="">All Methods</option>' + methods
      .map((m) => `<option value="${escapeHtml(m)}">${escapeHtml(m)}</option>`)
      .join('');
    els.methodSelect.value = currentMethod;
  }

  const workersSig = JSON.stringify(workers);
  if (workersSig !== lastWorkersSelectSig) {
    lastWorkersSelectSig = workersSig;
    els.workerSelect.innerHTML = '<option value="">All Workers</option>' + workers
      .map((w) => `<option value="${escapeHtml(w)}">${escapeHtml(w)}</option>`)
      .join('');
    els.workerSelect.value = currentWorker;
  }
}

function patchActiveCard(ctx) {
  if (!els) return;
  const data = ctx.state.data;
  const query = ctx.state.query || {};
  const matching = filterActiveCalls(data?.active_calls || [], {
    method: query.method || '',
    worker: query.worker || '',
  });

  if (matching.length === 0) {
    els.activeCard.classList.add('d-none');
    els.activeCount.textContent = '0';
    if (lastActiveSig !== '__empty__') {
      lastActiveSig = '__empty__';
      activeElapsedMap = new Map();
      els.activeTbody.replaceChildren();
    }
    return;
  }

  els.activeCard.classList.remove('d-none');
  els.activeCount.textContent = String(matching.length);

  const nowSec = Date.now() / 1000;
  const sig = JSON.stringify([
    ctx.state.window,
    query.method || '',
    query.worker || '',
    matching.map((c) => [
      c.request_id,
      formatDisplayMethod(c),
      c.worker_id,
      c.client_id,
      c.updates,
      c.sent_bytes,
    ]),
  ]);

  if (sig === lastActiveSig) {
    for (const call of matching) {
      const el = activeElapsedMap.get(String(call.request_id ?? ''));
      if (el) {
        el.textContent = formatDuration(computeElapsedSeconds(call.start_time, nowSec));
      }
    }
    return;
  }

  lastActiveSig = sig;
  activeElapsedMap = new Map();

  els.activeTbody.innerHTML = matching.map((call) => {
    const reqId = String(call.request_id ?? '');
    const methodLabel = formatDisplayMethod(call);
    const workerId = String(call.worker_id || '—');
    const clientId = String(call.client_id || '—');
    const elapsed = formatDuration(computeElapsedSeconds(call.start_time, nowSec));
    const updates = Math.max(0, toFiniteNumber(call.updates, 0));
    const sent = formatBytes(call.sent_bytes ?? 0);
    const href = buildRequestHref(ctx, reqId);

    return `
      <tr data-active-row="${escapeHtml(reqId)}">
        <td class="text-truncate" style="max-width: 200px;">
          <a href="${escapeHtml(href)}"
             class="font-mono fw-medium link-primary text-decoration-none"
             data-open-req="${escapeHtml(reqId)}"
             title="${escapeHtml(reqId)}">${escapeHtml(reqId)}</a>
        </td>
        <td class="font-mono text-truncate" style="max-width: 240px;" title="${escapeHtml(methodLabel)}">${escapeHtml(methodLabel)}</td>
        <td class="font-mono text-truncate" style="max-width: 160px;" title="${escapeHtml(workerId)}">${escapeHtml(workerId)}</td>
        <td class="font-mono text-secondary text-truncate" style="max-width: 160px;" title="${escapeHtml(clientId)}">${escapeHtml(clientId)}</td>
        <td class="num"><span class="badge bg-blue-lt font-mono" data-elapsed>${escapeHtml(elapsed)}</span></td>
        <td class="num">${escapeHtml(updates.toLocaleString())}</td>
        <td class="num">${escapeHtml(sent)}</td>
      </tr>
    `;
  }).join('');

  els.activeTbody.querySelectorAll('tr[data-active-row]').forEach((tr) => {
    const elapsedEl = tr.querySelector('[data-elapsed]');
    if (elapsedEl) {
      activeElapsedMap.set(tr.dataset.activeRow, elapsedEl);
    }
  });
}

function patchHistoryCard(ctx) {
  if (!els) return;
  const win = ctx.state.window || '15m';
  const query = ctx.state.query || {};
  const statusFilter = normalizeStatusParam(query.status);
  const methodFilter = String(query.method ?? '').trim();
  const workerFilter = String(query.worker ?? '').trim();
  const durationFilter = normalizeDurationParam(query.duration);
  const hasFilters = Boolean(statusFilter || methodFilter || workerFilter || durationFilter);
  const callsPayload = ctx.state.calls;

  if (!callsPayload) {
    els.historyTotal.textContent = '0';
    els.paginationSummary.textContent = `Loading (${win})…`;
    els.prevBtn.disabled = true;
    els.nextBtn.disabled = true;
    if (lastHistorySig !== `__loading__|${win}`) {
      lastHistorySig = `__loading__|${win}`;
      els.historyTbody.innerHTML = `
        <tr>
          <td colspan="9" class="text-center py-5 text-secondary">Loading Requests…</td>
        </tr>
      `;
    }
    return;
  }

  const rows = filterHistoricalCalls(callsPayload.calls || [], statusFilter);
  const offset = callsPayload.offset !== undefined
    ? Math.max(0, Math.floor(toFiniteNumber(callsPayload.offset, 0)))
    : Math.max(0, Number.parseInt(query.offset || '0', 10) || 0);
  const limit = Math.max(1, Math.floor(toFiniteNumber(callsPayload.limit, REQUESTS_PAGE_SIZE)));
  const total = Math.max(0, Math.floor(toFiniteNumber(callsPayload.total, 0)));

  const page = computePaginationRange({
    total,
    offset,
    limit,
    displayedCount: rows.length,
  });

  pendingPagination = { prevOffset: page.prevOffset, nextOffset: page.nextOffset };
  els.historyTotal.textContent = page.total.toLocaleString();
  els.paginationSummary.textContent = page.start > 0
    ? `Showing ${page.start}-${page.end} of ${page.total.toLocaleString()} (${win})`
    : `Showing 0 of ${page.total.toLocaleString()} (${win})`;
  els.prevBtn.disabled = !page.hasPrev;
  els.nextBtn.disabled = !page.hasNext;

  const sig = JSON.stringify([
    win,
    statusFilter,
    methodFilter,
    workerFilter,
    durationFilter,
    offset,
    page.total,
    rows.map((c) => [
      c.request_id,
      c.status,
      formatDisplayMethod(c),
      c.worker_id,
      c.client_id,
      c.date_time,
      c.duration,
      c.updates,
      c.sent_bytes,
    ]),
  ]);

  if (sig === lastHistorySig) {
    return;
  }
  lastHistorySig = sig;

  if (rows.length === 0) {
    const emptyTitle = hasFilters
      ? `No historical Requests matching active filters in ${win}`
      : `No historical Requests in the last ${win}`;
    const actionHtml = hasFilters
      ? '<button type="button" class="btn btn-sm btn-ghost-secondary mt-2" data-clear-filters>Clear filters</button>'
      : (page.hasPrev
        ? '<button type="button" class="btn btn-sm btn-ghost-secondary mt-2" data-page-first>Back to first page</button>'
        : '');
    els.historyTbody.innerHTML = `
      <tr>
        <td colspan="9" class="text-center py-5 text-secondary">
          <div class="fw-medium">${escapeHtml(emptyTitle)}</div>
          ${actionHtml}
        </td>
      </tr>
    `;
    return;
  }

  els.historyTbody.innerHTML = rows.map((call) => {
    const reqId = String(call.request_id ?? '');
    const status = String(call.status || 'UNKNOWN');
    const methodLabel = formatDisplayMethod(call);
    const workerId = String(call.worker_id || '—');
    const clientId = String(call.client_id || '—');
    const timestamp = formatCallTimestamp(call);
    const duration = formatDuration(call.duration);
    const updates = Math.max(0, toFiniteNumber(call.updates, 0));
    const sent = formatBytes(call.sent_bytes ?? 0);
    const href = buildRequestHref(ctx, reqId);

    return `
      <tr>
        <td class="text-truncate" style="max-width: 200px;">
          <a href="${escapeHtml(href)}"
             class="font-mono fw-medium link-primary text-decoration-none"
             data-open-req="${escapeHtml(reqId)}"
             title="${escapeHtml(reqId)}">${escapeHtml(reqId)}</a>
        </td>
        <td><span class="${statusBadgeClass(status)}">${escapeHtml(status)}</span></td>
        <td class="font-mono text-truncate" style="max-width: 220px;" title="${escapeHtml(methodLabel)}">${escapeHtml(methodLabel)}</td>
        <td class="font-mono text-truncate" style="max-width: 150px;" title="${escapeHtml(workerId)}">${escapeHtml(workerId)}</td>
        <td class="font-mono text-secondary text-truncate" style="max-width: 150px;" title="${escapeHtml(clientId)}">${escapeHtml(clientId)}</td>
        <td class="font-mono text-secondary text-nowrap small">${escapeHtml(timestamp)}</td>
        <td class="num">${escapeHtml(duration)}</td>
        <td class="num">${escapeHtml(updates.toLocaleString())}</td>
        <td class="num">${escapeHtml(sent)}</td>
      </tr>
    `;
  }).join('');
}

function render(ctx) {
  if (!root || !els || !ctx) return;
  const win = ctx.state.window || '15m';
  const query = ctx.state.query || {};
  els.windowLabel.textContent = win;
  patchFilterSelects(ctx.state.data, ctx.state.calls, query);
  syncControlsFromQuery(query);
  patchActiveCard(ctx);
  patchHistoryCard(ctx);
}

function commitSelectFilter(field, value) {
  if (!currentCtx) return;
  currentCtx.replaceQuery({ [field]: value || null, offset: null }, { push: false });
}

function handleStatusChange() {
  if (!els) return;
  commitSelectFilter('status', normalizeStatusParam(els.statusSelect.value));
}

function handleMethodChange() {
  if (!els) return;
  commitSelectFilter('method', els.methodSelect.value.trim());
}

function handleWorkerChange() {
  if (!els) return;
  commitSelectFilter('worker', els.workerSelect.value.trim());
}

function handleDurationChange() {
  if (!els) return;
  commitSelectFilter('duration', normalizeDurationParam(els.durationSelect.value));
}

function handleRootClick(event) {
  if (!currentCtx || !els) return;

  if (event.target.closest('[data-clear-filters]')) {
    els.statusSelect.value = '';
    els.methodSelect.value = '';
    els.workerSelect.value = '';
    els.durationSelect.value = '';
    currentCtx.replaceQuery(
      { status: null, method: null, worker: null, duration: null, offset: null },
      { push: false },
    );
    return;
  }

  const prevBtn = event.target.closest('[data-page-prev]');
  if (prevBtn && !prevBtn.disabled) {
    const target = pendingPagination.prevOffset;
    currentCtx.replaceQuery({ offset: target > 0 ? target : null }, { push: false });
    return;
  }

  const nextBtn = event.target.closest('[data-page-next]');
  if (nextBtn && !nextBtn.disabled) {
    currentCtx.replaceQuery({ offset: pendingPagination.nextOffset }, { push: false });
    return;
  }

  if (event.target.closest('[data-page-first]')) {
    currentCtx.replaceQuery({ offset: null }, { push: false });
  }
}

export function mount(container, ctx) {
  root = container;
  currentCtx = ctx;
  lastActiveSig = null;
  activeElapsedMap = new Map();
  lastHistorySig = null;
  lastMethodsSelectSig = '';
  lastWorkersSelectSig = '';
  seenMethods = new Set();
  seenWorkers = new Set();

  const win = ctx?.state?.window || '15m';
  root.innerHTML = buildShellHtml(win);

  els = {
    windowLabel: root.querySelector('[data-requests-window]'),
    activeCard: root.querySelector('[data-active-card]'),
    activeCount: root.querySelector('[data-active-count]'),
    activeTbody: root.querySelector('[data-active-tbody]'),
    historyTotal: root.querySelector('[data-history-total]'),
    statusSelect: root.querySelector('[data-filter-status]'),
    methodSelect: root.querySelector('[data-filter-method]'),
    workerSelect: root.querySelector('[data-filter-worker]'),
    durationSelect: root.querySelector('[data-filter-duration]'),
    clearBtn: root.querySelector('[data-clear-filters]'),
    historyTbody: root.querySelector('[data-history-tbody]'),
    paginationSummary: root.querySelector('[data-pagination-summary]'),
    prevBtn: root.querySelector('[data-page-prev]'),
    nextBtn: root.querySelector('[data-page-next]'),
  };

  els.statusSelect?.addEventListener('change', handleStatusChange);
  els.methodSelect?.addEventListener('change', handleMethodChange);
  els.workerSelect?.addEventListener('change', handleWorkerChange);
  els.durationSelect?.addEventListener('change', handleDurationChange);
  root.addEventListener('click', handleRootClick);

  render(ctx);
}

export function update(ctx) {
  currentCtx = ctx;
  render(ctx);
}

export function unmount() {
  els?.statusSelect?.removeEventListener('change', handleStatusChange);
  els?.methodSelect?.removeEventListener('change', handleMethodChange);
  els?.workerSelect?.removeEventListener('change', handleWorkerChange);
  els?.durationSelect?.removeEventListener('change', handleDurationChange);
  root?.removeEventListener('click', handleRootClick);

  root = null;
  currentCtx = null;
  els = null;
  lastActiveSig = null;
  activeElapsedMap = new Map();
  lastHistorySig = null;
  lastMethodsSelectSig = '';
  lastWorkersSelectSig = '';
  seenMethods = new Set();
  seenWorkers = new Set();
}
