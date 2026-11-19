/**
 * Overview page controller (Overview A — Issue #19).
 *
 * Pure, DOM-independent helpers (`computeErrorRatePct`, `selectTopMethods`,
 * `extractBrokerEndpoints`, `computeElapsedSeconds`) are exported for direct
 * unit testing under `node --test`.
 *
 * View lifecycle exports: `mount(container, ctx)`, `update(ctx)`, `unmount()`.
 */

import { escapeHtml, formatDuration } from '../format.js';

const ENDPOINT_SPECS = Object.freeze([
  { key: 'frontend', label: 'Frontend' },
  { key: 'backend', label: 'Backend' },
  { key: 'publish', label: 'Publish' },
  { key: 'subscribe', label: 'Subscribe' },
]);

const TOP_METHODS_LIMIT = 5;
const COPY_FEEDBACK_MS = 1500;

const COPY_ICON_SVG = `
  <svg class="icon icon-sm" width="16" height="16" viewBox="0 0 24 24"
       fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
       aria-hidden="true" focusable="false">
    <path d="M7 7m0 2.667a2.667 2.667 0 0 1 2.667 -2.667h8.666a2.667 2.667 0 0 1 2.667 2.667v8.666a2.667 2.667 0 0 1 -2.667 2.667h-8.666a2.667 2.667 0 0 1 -2.667 -2.667z"/>
    <path d="M4.012 16.737a2.005 2.005 0 0 1 -1.012 -1.737v-10c0 -1.1 .9 -2 2 -2h10c.75 0 1.158 .385 1.5 1"/>
  </svg>
`;

const CHECK_ICON_SVG = `
  <svg class="icon icon-sm" width="16" height="16" viewBox="0 0 24 24"
       fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
       aria-hidden="true" focusable="false">
    <path d="M5 12l5 5l10 -10"/>
  </svg>
`;

function toFiniteNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

/**
 * Compute clamped error-rate percentage in `[0, 100]`.
 * @param {number} total
 * @param {number} errors
 * @returns {number}
 */
export function computeErrorRatePct(total, errors) {
  const safeTotal = Math.max(0, toFiniteNumber(total, 0));
  const safeErrors = Math.max(0, toFiniteNumber(errors, 0));
  if (safeTotal <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(100, (safeErrors / safeTotal) * 100));
}

/**
 * Return the top N methods sorted by `count` descending (tie-breaking by
 * `errors` descending, then `method` name ascending) without mutating input.
 * @param {Array<object>} methods
 * @param {number} [limit=5]
 * @returns {Array<object>}
 */
export function selectTopMethods(methods, limit = TOP_METHODS_LIMIT) {
  if (!Array.isArray(methods)) {
    return [];
  }
  const maxItems = Math.max(0, Math.floor(toFiniteNumber(limit, TOP_METHODS_LIMIT)));
  if (maxItems === 0) {
    return [];
  }

  return methods
    .filter((item) => item && typeof item === 'object')
    .slice()
    .sort((a, b) => {
      const countDiff = Math.max(0, toFiniteNumber(b.count, 0)) - Math.max(0, toFiniteNumber(a.count, 0));
      if (countDiff !== 0) return countDiff;

      const errDiff = Math.max(0, toFiniteNumber(b.errors, 0)) - Math.max(0, toFiniteNumber(a.errors, 0));
      if (errDiff !== 0) return errDiff;

      const aName = String(a.method ?? '');
      const bName = String(b.method ?? '');
      const aLower = aName.toLowerCase();
      const bLower = bName.toLowerCase();
      if (aLower < bLower) return -1;
      if (aLower > bLower) return 1;
      if (aName < bName) return -1;
      if (aName > bName) return 1;
      return 0;
    })
    .slice(0, maxItems);
}

/**
 * Extract non-empty Broker endpoints (`frontend`, `backend`, `publish`, `subscribe`).
 * @param {object|null|undefined} data
 * @returns {Array<{ key: string, label: string, url: string }>}
 */
export function extractBrokerEndpoints(data) {
  if (!data || typeof data !== 'object') {
    return [];
  }
  const result = [];
  for (const spec of ENDPOINT_SPECS) {
    const raw = data[spec.key];
    if (typeof raw === 'string' && raw.trim()) {
      result.push({ key: spec.key, label: spec.label, url: raw.trim() });
    }
  }
  return result;
}

/**
 * Compute non-negative elapsed seconds from an epoch `startTime` in seconds.
 * Returns `null` if `startTime` is missing or invalid.
 * @param {number} startTime
 * @param {number} [nowSeconds]
 * @returns {number|null}
 */
export function computeElapsedSeconds(startTime, nowSeconds = Date.now() / 1000) {
  const start = Number(startTime);
  if (!Number.isFinite(start) || start <= 0) {
    return null;
  }
  return Math.max(0, nowSeconds - start);
}

function formatDisplayMethod(call) {
  const rawMethod = typeof call?.raw_method === 'string' ? call.raw_method.trim() : '';
  const methodStr = typeof call?.method === 'string' ? call.method.trim() : '';
  if (!methodStr) {
    return rawMethod || '—';
  }
  if (rawMethod && methodStr === `${rawMethod}()`) {
    return rawMethod;
  }
  return methodStr;
}

function formatCallTimestamp(call) {
  if (typeof call?.date_time === 'string' && call.date_time.trim()) {
    return call.date_time.trim();
  }
  const start = Number(call?.start_time);
  if (Number.isFinite(start) && start > 0) {
    return new Date(start * 1000).toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
  }
  return '—';
}

// --- View controller state ---

let root = null;
let currentCtx = null;
let els = null;
let lastEndpointsSig = null;
let lastTopMethodsSig = null;
let lastActiveCallsSig = null;
let activeElapsedMap = new Map();
let lastFailuresSig = null;
const copyTimers = new Map();

function clearCopyTimers() {
  for (const timer of copyTimers.values()) {
    clearTimeout(timer);
  }
  copyTimers.clear();
}

function flashCopyButton(btn) {
  if (!btn) return;
  const prev = copyTimers.get(btn);
  if (prev) {
    clearTimeout(prev);
  }
  btn.innerHTML = CHECK_ICON_SVG;
  btn.classList.add('text-success');
  const timer = setTimeout(() => {
    copyTimers.delete(btn);
    if (btn.isConnected) {
      btn.innerHTML = COPY_ICON_SVG;
      btn.classList.remove('text-success');
    }
  }, COPY_FEEDBACK_MS);
  copyTimers.set(btn, timer);
}

function buildShellHtml(win) {
  return `
    <div class="page-header d-print-none mb-3">
      <div class="row align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Overview</h1>
        </div>
        <div class="col-auto text-secondary small">
          Window: <span class="font-mono" data-overview-window>${escapeHtml(win)}</span>
        </div>
      </div>
    </div>

    <div class="row row-deck row-cards mb-3">
      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Uptime</div>
            <div class="h1 mb-0 mt-1 font-mono" data-kpi-uptime>—</div>
            <div class="text-secondary small mt-2" data-kpi-uptime-sub>Waiting for Monitor data…</div>
          </div>
        </div>
      </div>

      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Requests</div>
            <div class="h1 mb-0 mt-1 font-mono" data-kpi-requests>—</div>
            <div class="text-secondary small mt-2" data-kpi-requests-sub>Loading ${escapeHtml(win)} window…</div>
          </div>
        </div>
      </div>

      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Failures</div>
            <div class="h1 mb-0 mt-1 font-mono" data-kpi-failures>—</div>
            <div class="text-secondary small mt-1 font-mono" data-kpi-error-rate-text>0.0% error rate</div>
            <div class="progress progress-sm mt-2">
              <div class="progress-bar bg-danger"
                   role="progressbar"
                   style="width: 0%"
                   aria-label="Lifetime error rate"
                   aria-valuemin="0"
                   aria-valuemax="100"
                   aria-valuenow="0"
                   data-kpi-error-bar></div>
            </div>
          </div>
        </div>
      </div>

      <div class="col-sm-6 col-lg-3">
        <div class="card card-sm">
          <div class="card-body">
            <div class="subheader">Active Workers</div>
            <div class="d-flex align-items-center gap-2 mt-1">
              <span class="status-dot bg-secondary" data-kpi-workers-dot aria-hidden="true"></span>
              <div class="h1 mb-0 font-mono" data-kpi-workers>—</div>
            </div>
            <div class="mt-2 small">
              <a href="/workers" data-nav="/workers" class="link-primary text-decoration-none d-inline-flex align-items-center gap-1">
                <span>View Workers</span>
                <span aria-hidden="true">&rarr;</span>
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="row row-cards">
      <div class="col-lg-6 d-flex flex-column gap-3">
        <div class="card">
          <div class="card-header">
            <h2 class="card-title mb-0">Broker Endpoints</h2>
          </div>
          <div class="card-body" data-overview-endpoints>
            <div class="text-secondary small">Loading Broker endpoints…</div>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="d-flex align-items-center gap-2 min-w-0">
              <h2 class="card-title mb-0">Top Methods</h2>
              <span class="badge bg-secondary-lt font-mono" data-top-methods-window>${escapeHtml(win)}</span>
            </div>
            <div class="card-actions">
              <a href="/methods" data-nav="/methods" class="btn btn-sm btn-ghost-secondary">
                All Methods <span aria-hidden="true" class="ms-1">&rarr;</span>
              </a>
            </div>
          </div>
          <div data-overview-top-methods>
            <div class="card-body text-secondary small">Loading Method summary…</div>
          </div>
        </div>
      </div>

      <div class="col-lg-6 d-flex flex-column gap-3">
        <div class="card">
          <div class="card-header">
            <div class="d-flex align-items-center gap-2 min-w-0">
              <h2 class="card-title mb-0">Active Requests</h2>
              <span class="badge bg-secondary-lt font-mono" data-active-requests-count>0</span>
            </div>
            <div class="card-actions">
              <a href="/requests" data-nav="/requests" class="btn btn-sm btn-ghost-secondary">
                All Requests <span aria-hidden="true" class="ms-1">&rarr;</span>
              </a>
            </div>
          </div>
          <div data-overview-active-requests>
            <div class="card-body text-secondary small">Loading active Requests…</div>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="d-flex align-items-center gap-2 min-w-0">
              <h2 class="card-title mb-0">Recent Failures</h2>
              <span class="badge bg-secondary-lt font-mono" data-recent-failures-count>0</span>
            </div>
            <div class="card-actions">
              <a href="/requests?status=ERROR" data-nav="/requests?status=ERROR" class="btn btn-sm btn-ghost-secondary">
                All Failures <span aria-hidden="true" class="ms-1">&rarr;</span>
              </a>
            </div>
          </div>
          <div data-overview-recent-failures>
            <div class="card-body text-secondary small">Loading recent failures…</div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function patchKpis(data, summary, win) {
  if (!els) return;
  els.windowLabel.textContent = win;

  const stats = data?.stats || null;
  if (!stats) {
    els.kpiUptime.textContent = '—';
    els.kpiUptimeSub.textContent = 'Waiting for Monitor data…';
    els.kpiRequests.textContent = '—';
    els.kpiRequestsSub.textContent = summary
      ? `${Math.max(0, toFiniteNumber(summary.total_requests, 0)).toLocaleString()} in ${summary.window || win}`
      : `Loading ${win} window…`;
    els.kpiFailures.textContent = '—';
    els.kpiErrorRateText.textContent = '0.0% error rate';
    els.kpiErrorBar.style.width = '0%';
    els.kpiErrorBar.setAttribute('aria-valuenow', '0');
    els.kpiWorkers.textContent = '—';
    els.kpiWorkersDot.className = 'status-dot bg-secondary';
    return;
  }

  const uptimeSec = Math.floor(Math.max(0, toFiniteNumber(stats.uptime, 0)));
  els.kpiUptime.textContent = formatDuration(uptimeSec);
  els.kpiUptimeSub.textContent = 'Since Monitor start';

  const totalRequests = Math.max(0, toFiniteNumber(stats.total_requests, 0));
  els.kpiRequests.textContent = totalRequests.toLocaleString();
  if (summary) {
    const winCount = Math.max(0, toFiniteNumber(summary.total_requests, 0));
    const winLabel = summary.window || win;
    els.kpiRequestsSub.textContent = `${winCount.toLocaleString()} in ${winLabel}`;
  } else {
    els.kpiRequestsSub.textContent = `Loading ${win} window…`;
  }

  const totalErrors = Math.max(0, toFiniteNumber(stats.total_errors, 0));
  const errPct = computeErrorRatePct(totalRequests, totalErrors);
  const errPctFixed = errPct.toFixed(1);
  els.kpiFailures.textContent = totalErrors.toLocaleString();
  els.kpiErrorRateText.textContent = `${errPctFixed}% error rate`;
  els.kpiErrorBar.style.width = `${errPctFixed}%`;
  els.kpiErrorBar.setAttribute('aria-valuenow', errPctFixed);

  const activeWorkersCount = stats.active_workers_count !== undefined && stats.active_workers_count !== null
    ? Math.max(0, toFiniteNumber(stats.active_workers_count, 0))
    : (Array.isArray(data.workers) ? data.workers.length : 0);
  els.kpiWorkers.textContent = activeWorkersCount.toLocaleString();
  els.kpiWorkersDot.className = activeWorkersCount > 0
    ? 'status-dot status-dot-animated bg-success'
    : 'status-dot bg-secondary';
}

function patchBrokerEndpoints(data) {
  if (!els) return;
  const loaded = data !== null && data !== undefined;
  const endpoints = extractBrokerEndpoints(data);
  const sig = loaded ? JSON.stringify(endpoints) : '__loading__';
  if (sig === lastEndpointsSig) {
    return;
  }
  lastEndpointsSig = sig;
  clearCopyTimers();

  if (!loaded) {
    els.endpointsBody.innerHTML = '<div class="text-secondary small">Loading Broker endpoints…</div>';
    return;
  }

  if (endpoints.length === 0) {
    els.endpointsBody.innerHTML = '<div class="text-secondary small">No Broker endpoints configured.</div>';
    return;
  }

  const itemsHtml = endpoints.map((ep) => `
    <div class="datagrid-item min-w-0">
      <div class="datagrid-title">${escapeHtml(ep.label)}</div>
      <div class="datagrid-content d-flex align-items-center justify-content-between gap-2 min-w-0">
        <code class="small text-break flex-grow-1 min-w-0">${escapeHtml(ep.url)}</code>
        <button type="button"
                class="btn btn-sm btn-icon btn-ghost-secondary flex-shrink-0"
                data-copy="${escapeHtml(ep.url)}"
                data-copy-label="Copied ${escapeHtml(ep.key)} endpoint"
                data-overview-copy
                aria-label="Copy ${escapeHtml(ep.key)} endpoint"
                title="Copy ${escapeHtml(ep.key)} endpoint">
          ${COPY_ICON_SVG}
        </button>
      </div>
    </div>
  `).join('');

  els.endpointsBody.innerHTML = `
    <div class="datagrid" style="--tblr-datagrid-padding: 1rem; --tblr-datagrid-item-width: 12rem;">
      ${itemsHtml}
    </div>
  `;
}

function patchTopMethods(summary, win) {
  if (!els) return;
  const displayWin = summary?.window || win;
  els.topMethodsWindow.textContent = displayWin;

  const loaded = summary !== null && summary !== undefined;
  const topMethods = loaded ? selectTopMethods(summary.methods, TOP_METHODS_LIMIT) : [];
  const sig = loaded
    ? JSON.stringify([
      displayWin,
      topMethods.map((m) => [m.method, m.count, m.errors, m.p95_duration]),
    ])
    : `__loading__|${win}`;

  if (sig === lastTopMethodsSig) {
    return;
  }
  lastTopMethodsSig = sig;

  if (!loaded) {
    els.topMethodsBody.innerHTML = '<div class="card-body text-secondary small">Loading Method summary…</div>';
    return;
  }

  if (topMethods.length === 0) {
    els.topMethodsBody.innerHTML = `
      <div class="card-body text-secondary small text-center py-4">
        No Methods recorded in <span class="font-mono">${escapeHtml(displayWin)}</span>.
      </div>
    `;
    return;
  }

  const encodedWin = encodeURIComponent(displayWin);
  const rowsHtml = topMethods.map((item) => {
    const name = String(item.method ?? '');
    const count = Math.max(0, toFiniteNumber(item.count, 0));
    const errors = Math.max(0, toFiniteNumber(item.errors, 0));
    const p95Label = formatDuration(item.p95_duration);
    const href = `/requests?window=${encodedWin}&method=${encodeURIComponent(name)}`;
    const errorsHtml = errors > 0
      ? `<span class="badge bg-red-lt font-mono">${escapeHtml(errors.toLocaleString())}</span>`
      : '<span class="text-secondary">0</span>';

    return `
      <tr>
        <td class="text-truncate" style="max-width: 220px;">
          <a href="${escapeHtml(href)}"
             class="font-mono fw-medium text-reset"
             data-method-drilldown="${escapeHtml(name)}"
             title="View Requests for ${escapeHtml(name)}">${escapeHtml(name)}</a>
        </td>
        <td class="num">${escapeHtml(count.toLocaleString())}</td>
        <td class="num">${errorsHtml}</td>
        <td class="num">${escapeHtml(p95Label)}</td>
      </tr>
    `;
  }).join('');

  els.topMethodsBody.innerHTML = `
    <div class="table-responsive">
      <table class="table table-vcenter table-hover card-table mb-0">
        <thead>
          <tr>
            <th scope="col">Method</th>
            <th scope="col" class="text-end">Calls</th>
            <th scope="col" class="text-end">Errors</th>
            <th scope="col" class="text-end">p95</th>
          </tr>
        </thead>
        <tbody>${rowsHtml}</tbody>
      </table>
    </div>
  `;
}

function patchActiveRequests(data, win) {
  if (!els) return;
  const loaded = data !== null && data !== undefined;
  const activeCalls = loaded && Array.isArray(data.active_calls) ? data.active_calls : [];
  const count = activeCalls.length;

  els.activeRequestsCount.textContent = String(count);
  els.activeRequestsCount.className = count > 0
    ? 'badge bg-blue-lt font-mono'
    : 'badge bg-secondary-lt font-mono';

  const nowSec = Date.now() / 1000;
  const structureSig = loaded
    ? JSON.stringify([
      win,
      activeCalls.map((c) => [c?.request_id, formatDisplayMethod(c), c?.worker_id]),
    ])
    : '__loading__';

  if (structureSig === lastActiveCallsSig) {
    // Structure unchanged: patch ticking elapsed durations in place without touching focus/scroll
    if (count > 0) {
      for (const call of activeCalls) {
        const reqId = String(call?.request_id ?? '');
        const elapsedEl = activeElapsedMap.get(reqId);
        if (elapsedEl) {
          elapsedEl.textContent = formatDuration(computeElapsedSeconds(call?.start_time, nowSec));
        }
      }
    }
    return;
  }

  lastActiveCallsSig = structureSig;
  activeElapsedMap = new Map();

  if (!loaded) {
    els.activeRequestsBody.innerHTML = '<div class="card-body text-secondary small">Loading active Requests…</div>';
    return;
  }

  if (count === 0) {
    els.activeRequestsBody.innerHTML = `
      <div class="card-body text-secondary small text-center py-4">
        No active Requests
      </div>
    `;
    return;
  }

  const encodedWin = encodeURIComponent(win);
  const itemsHtml = activeCalls.map((call) => {
    const reqId = String(call?.request_id ?? '');
    const methodLabel = formatDisplayMethod(call);
    const workerId = typeof call?.worker_id === 'string' && call.worker_id.trim()
      ? call.worker_id.trim()
      : '—';
    const elapsedLabel = formatDuration(computeElapsedSeconds(call?.start_time, nowSec));
    const href = `/?window=${encodedWin}&request=${encodeURIComponent(reqId)}`;

    return `
      <a href="${escapeHtml(href)}"
         class="list-group-item list-group-item-action py-2"
         data-open-req="${escapeHtml(reqId)}"
         data-active-req-item="${escapeHtml(reqId)}">
        <div class="d-flex align-items-center justify-content-between gap-2">
          <div class="d-flex align-items-center gap-2 min-w-0">
            <span class="status-dot status-dot-animated bg-primary flex-shrink-0" aria-hidden="true"></span>
            <span class="font-mono fw-medium text-truncate" title="${escapeHtml(methodLabel)}">${escapeHtml(methodLabel)}</span>
          </div>
          <span class="badge bg-blue-lt font-mono flex-shrink-0" data-elapsed>${escapeHtml(elapsedLabel)}</span>
        </div>
        <div class="d-flex align-items-center justify-content-between gap-2 small text-secondary mt-1">
          <span class="font-mono text-truncate" title="${escapeHtml(reqId)}">${escapeHtml(reqId)}</span>
          <span class="font-mono text-truncate flex-shrink-0" title="Worker ${escapeHtml(workerId)}">${escapeHtml(workerId)}</span>
        </div>
      </a>
    `;
  }).join('');

  els.activeRequestsBody.innerHTML = `
    <div class="list-group list-group-flush list-group-hoverable">
      ${itemsHtml}
    </div>
  `;

  els.activeRequestsBody.querySelectorAll('[data-active-req-item]').forEach((itemEl) => {
    const elapsedEl = itemEl.querySelector('[data-elapsed]');
    if (elapsedEl) {
      activeElapsedMap.set(itemEl.dataset.activeReqItem, elapsedEl);
    }
  });
}

function patchRecentFailures(recentFailures, win) {
  if (!els) return;
  const loaded = recentFailures !== null && recentFailures !== undefined;
  const calls = loaded && Array.isArray(recentFailures.calls) ? recentFailures.calls : [];
  const total = loaded
    ? Math.max(calls.length, toFiniteNumber(recentFailures.total, calls.length))
    : 0;

  els.recentFailuresCount.textContent = String(total);
  els.recentFailuresCount.className = total > 0
    ? 'badge bg-red-lt font-mono'
    : 'badge bg-secondary-lt font-mono';

  const sig = loaded
    ? JSON.stringify([
      win,
      total,
      calls.map((c) => [c?.request_id, formatDisplayMethod(c), c?.worker_id, c?.duration, c?.date_time]),
    ])
    : `__loading__|${win}`;

  if (sig === lastFailuresSig) {
    return;
  }
  lastFailuresSig = sig;

  if (!loaded) {
    els.recentFailuresBody.innerHTML = '<div class="card-body text-secondary small">Loading recent failures…</div>';
    return;
  }

  if (calls.length === 0) {
    els.recentFailuresBody.innerHTML = `
      <div class="card-body text-secondary small text-center py-4">
        No failures in <span class="font-mono">${escapeHtml(win)}</span>
      </div>
    `;
    return;
  }

  const encodedWin = encodeURIComponent(win);
  const itemsHtml = calls.map((call) => {
    const reqId = String(call?.request_id ?? '');
    const methodLabel = formatDisplayMethod(call);
    const workerId = typeof call?.worker_id === 'string' && call.worker_id.trim()
      ? call.worker_id.trim()
      : '—';
    const durationLabel = formatDuration(call?.duration);
    const timestamp = formatCallTimestamp(call);
    const href = `/?window=${encodedWin}&request=${encodeURIComponent(reqId)}`;

    return `
      <a href="${escapeHtml(href)}"
         class="list-group-item list-group-item-action py-2"
         data-open-req="${escapeHtml(reqId)}">
        <div class="d-flex align-items-center justify-content-between gap-2">
          <div class="d-flex align-items-center gap-2 min-w-0">
            <span class="status-dot bg-danger flex-shrink-0" aria-hidden="true"></span>
            <span class="font-mono fw-medium text-truncate" title="${escapeHtml(methodLabel)}">${escapeHtml(methodLabel)}</span>
          </div>
          <span class="badge bg-red-lt font-mono flex-shrink-0">${escapeHtml(durationLabel)}</span>
        </div>
        <div class="d-flex align-items-center justify-content-between gap-2 small text-secondary mt-1">
          <span class="font-mono text-truncate" title="${escapeHtml(reqId)} · ${escapeHtml(workerId)}">
            ${escapeHtml(reqId)} · ${escapeHtml(workerId)}
          </span>
          <span class="font-mono text-nowrap flex-shrink-0" title="${escapeHtml(timestamp)}">${escapeHtml(timestamp)}</span>
        </div>
      </a>
    `;
  }).join('');

  els.recentFailuresBody.innerHTML = `
    <div class="list-group list-group-flush list-group-hoverable">
      ${itemsHtml}
    </div>
  `;
}

function render(ctx) {
  if (!root || !els || !ctx) return;
  const { state } = ctx;
  const win = state.window || '15m';
  const data = state.data ?? null;
  const summary = state.summary ?? null;
  const recentFailures = state.recentFailures ?? null;

  patchKpis(data, summary, win);
  patchBrokerEndpoints(data);
  patchTopMethods(summary, win);
  patchActiveRequests(data, win);
  patchRecentFailures(recentFailures, win);
}

function handleRootClick(event) {
  const copyBtn = event.target.closest('[data-overview-copy]');
  if (copyBtn) {
    // Allow app.js delegated `[data-copy]` handler to perform clipboard copy & announce
    flashCopyButton(copyBtn);
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
  lastEndpointsSig = null;
  lastTopMethodsSig = null;
  lastActiveCallsSig = null;
  activeElapsedMap = new Map();
  lastFailuresSig = null;
  clearCopyTimers();

  const win = ctx?.state?.window || '15m';
  root.innerHTML = buildShellHtml(win);

  els = {
    windowLabel: root.querySelector('[data-overview-window]'),
    kpiUptime: root.querySelector('[data-kpi-uptime]'),
    kpiUptimeSub: root.querySelector('[data-kpi-uptime-sub]'),
    kpiRequests: root.querySelector('[data-kpi-requests]'),
    kpiRequestsSub: root.querySelector('[data-kpi-requests-sub]'),
    kpiFailures: root.querySelector('[data-kpi-failures]'),
    kpiErrorRateText: root.querySelector('[data-kpi-error-rate-text]'),
    kpiErrorBar: root.querySelector('[data-kpi-error-bar]'),
    kpiWorkers: root.querySelector('[data-kpi-workers]'),
    kpiWorkersDot: root.querySelector('[data-kpi-workers-dot]'),
    endpointsBody: root.querySelector('[data-overview-endpoints]'),
    topMethodsWindow: root.querySelector('[data-top-methods-window]'),
    topMethodsBody: root.querySelector('[data-overview-top-methods]'),
    activeRequestsCount: root.querySelector('[data-active-requests-count]'),
    activeRequestsBody: root.querySelector('[data-overview-active-requests]'),
    recentFailuresCount: root.querySelector('[data-recent-failures-count]'),
    recentFailuresBody: root.querySelector('[data-overview-recent-failures]'),
  };

  root.addEventListener('click', handleRootClick);
  render(ctx);
}

export function update(ctx) {
  currentCtx = ctx;
  render(ctx);
}

export function unmount() {
  if (root) {
    root.removeEventListener('click', handleRootClick);
  }
  clearCopyTimers();
  root = null;
  currentCtx = null;
  els = null;
  lastEndpointsSig = null;
  lastTopMethodsSig = null;
  lastActiveCallsSig = null;
  activeElapsedMap = new Map();
  lastFailuresSig = null;
}
