/**
 * Workers page controller (Workers C).
 * Displays a responsive 3-column grid of Tabler-themed Worker sparkline cards
 * with All / Active / Offline status filter pills, live counts, URL query sync,
 * offscreen content-visibility render skipping, and method -> Requests drilldown.
 */

import { escapeHtml, formatDuration } from '../format.js';
import { partitionWorkers, updateWorkerSparklines } from '../../sparklines.js';

const VALID_STATUSES = new Set(['all', 'active', 'offline']);

let root = null;
let filterGroupEl = null;
let gridContainerEl = null;
let gridEl = null;
let emptyEl = null;
let latestCtx = null;
let isGridOffscreen = false;
let lastRender = null;

export function normalizeStatusFilter(raw) {
  const value = String(raw || '').toLowerCase();
  return VALID_STATUSES.has(value) ? value : 'all';
}

function renderEmptyState(emptyTarget, { loading, statusFilter, counts, windowKey }) {
  if (loading) {
    emptyTarget.innerHTML = `
      <div class="card">
        <div class="card-body text-secondary small">
          Loading Worker telemetry for the last <span class="font-mono">${escapeHtml(windowKey)}</span>…
        </div>
      </div>
    `;
    return;
  }

  if (counts.all === 0) {
    emptyTarget.innerHTML = `
      <div class="card">
        <div class="empty">
          <p class="empty-title">No Worker telemetry in this window</p>
          <p class="empty-subtitle text-secondary">
            No active or historical Workers recorded metrics or completed Requests in the last
            <span class="font-mono">${escapeHtml(windowKey)}</span>.
          </p>
        </div>
      </div>
    `;
    return;
  }

  if (statusFilter === 'active') {
    const offlineWord = counts.offline === 1 ? 'Worker' : 'Workers';
    emptyTarget.innerHTML = `
      <div class="card">
        <div class="empty">
          <p class="empty-title">No active Workers</p>
          <p class="empty-subtitle text-secondary">
            <span class="font-mono">${escapeHtml(counts.offline)}</span> offline ${offlineWord} recorded in the last
            <span class="font-mono">${escapeHtml(windowKey)}</span>.
          </p>
          <div class="empty-action">
            <button type="button" class="btn btn-sm btn-primary" data-reset-status>
              Show all Workers (${escapeHtml(counts.all)})
            </button>
          </div>
        </div>
      </div>
    `;
    return;
  }

  const activeWord = counts.active === 1 ? 'Worker' : 'Workers';
  const verb = counts.active === 1 ? 'is' : 'are';
  emptyTarget.innerHTML = `
    <div class="card">
      <div class="empty">
        <p class="empty-title">No offline Workers</p>
        <p class="empty-subtitle text-secondary">
          All <span class="font-mono">${escapeHtml(counts.active)}</span> ${activeWord} recorded in the last
          <span class="font-mono">${escapeHtml(windowKey)}</span> ${verb} currently active.
        </p>
        <div class="empty-action">
          <button type="button" class="btn btn-sm btn-primary" data-reset-status>
            Show all Workers (${escapeHtml(counts.all)})
          </button>
        </div>
      </div>
    </div>
  `;
}

function updateFilterPills(statusFilter, counts) {
  if (!filterGroupEl) return;

  filterGroupEl.querySelectorAll('button[data-status]').forEach((btn) => {
    const key = btn.dataset.status;
    const isSelected = key === statusFilter;
    btn.classList.toggle('btn-primary', isSelected);
    btn.classList.toggle('active', isSelected);
    btn.setAttribute('aria-pressed', String(isSelected));

    const badge = btn.querySelector('[data-count]');
    if (badge) {
      badge.textContent = String(counts[key] ?? 0);
      if (isSelected) {
        badge.className = 'badge bg-white text-primary ms-1 font-mono';
      } else if (key === 'active') {
        badge.className = 'badge bg-green-lt ms-1 font-mono';
      } else {
        badge.className = 'badge bg-secondary-lt ms-1 font-mono';
      }
    }
  });
}

function render(ctx, { force = false } = {}) {
  if (!root || !gridEl || !emptyEl) return;
  latestCtx = ctx;

  const { state } = ctx;
  const statusFilter = normalizeStatusFilter(state.query?.status);
  const windowKey = state.window || '15m';
  const activeWorkersList = Array.isArray(state.data?.workers) ? state.data.workers : [];
  const activeWorkers = new Set(activeWorkersList);
  const workerMetrics = state.data?.worker_metrics || {};
  const workerMethods = state.data?.worker_methods || {};
  const metricsPayload = state.metrics;
  const tasksPayload = state.tasks;

  const { active, offline, ordered } = partitionWorkers({
    metricsPayload,
    tasksPayload,
    activeWorkers,
  });

  const counts = {
    all: ordered.length,
    active: active.length,
    offline: offline.length,
  };

  updateFilterPills(statusFilter, counts);

  if (isGridOffscreen) {
    return;
  }

  const hasLoaded = state.data !== null || state.metrics !== null || state.tasks !== null;
  const visibleCount = counts[statusFilter] ?? 0;

  const series = (metricsPayload && metricsPayload.series) || {};
  const fallbackMetrics = {};
  for (const wid of active) {
    if ((!series[wid] || series[wid].length === 0) && workerMetrics[wid]) {
      fallbackMetrics[wid] = workerMetrics[wid];
    }
  }
  const liveSig = JSON.stringify([active, workerMethods, fallbackMetrics]);

  if (
    !force
    && lastRender
    && lastRender.metrics === metricsPayload
    && lastRender.tasks === tasksPayload
    && lastRender.status === statusFilter
    && lastRender.window === windowKey
    && lastRender.hasLoaded === hasLoaded
    && lastRender.liveSig === liveSig
  ) {
    return;
  }

  lastRender = {
    metrics: metricsPayload,
    tasks: tasksPayload,
    status: statusFilter,
    window: windowKey,
    hasLoaded,
    liveSig,
  };

  if (!hasLoaded || visibleCount === 0) {
    gridEl.classList.add('d-none');
    gridEl.replaceChildren();
    emptyEl.classList.remove('d-none');
    renderEmptyState(emptyEl, {
      loading: !hasLoaded,
      statusFilter,
      counts,
      windowKey,
    });
    return;
  }

  emptyEl.classList.add('d-none');
  emptyEl.replaceChildren();
  gridEl.classList.remove('d-none');

  updateWorkerSparklines({
    container: gridEl,
    metricsPayload: metricsPayload || { series: {} },
    tasksPayload: tasksPayload || { tasks: {} },
    activeWorkers,
    workerMetrics,
    workerMethods,
    formatDuration,
    offscreen: isGridOffscreen,
    statusFilter,
    onMethodClick: (workerId, methodName) => {
      if (latestCtx) {
        latestCtx.navigate('/requests', { method: methodName, worker: workerId });
      }
    },
  });
}

function handleFilterClick(event) {
  const btn = event.target.closest('button[data-status]');
  if (!btn || !latestCtx) return;
  const nextStatus = normalizeStatusFilter(btn.dataset.status);
  latestCtx.replaceQuery({ status: nextStatus }, { push: false });
}

function handleEmptyClick(event) {
  const resetBtn = event.target.closest('[data-reset-status]');
  if (!resetBtn || !latestCtx) return;
  latestCtx.replaceQuery({ status: 'all' }, { push: false });
}

function handleContentVisibilityChange(event) {
  isGridOffscreen = Boolean(event.skipped);
  if (!isGridOffscreen && latestCtx) {
    render(latestCtx, { force: true });
  }
}

export function mount(container, ctx) {
  root = container;
  latestCtx = ctx;
  isGridOffscreen = false;
  lastRender = null;

  root.innerHTML = `
    <div class="page-header d-print-none mb-3">
      <div class="row g-2 align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Workers</h1>
        </div>
        <div class="col-auto ms-auto d-print-none">
          <div class="d-inline-flex flex-wrap gap-2" role="group" aria-label="Filter Workers by status" data-workers-filter>
            <button type="button" class="btn btn-sm btn-pill" data-status="all" aria-pressed="true">
              All <span class="badge bg-secondary-lt ms-1 font-mono" data-count="all">0</span>
            </button>
            <button type="button" class="btn btn-sm btn-pill" data-status="active" aria-pressed="false">
              Active <span class="badge bg-green-lt ms-1 font-mono" data-count="active">0</span>
            </button>
            <button type="button" class="btn btn-sm btn-pill" data-status="offline" aria-pressed="false">
              Offline <span class="badge bg-secondary-lt ms-1 font-mono" data-count="offline">0</span>
            </button>
          </div>
        </div>
      </div>
    </div>
    <div class="workers-grid-container" id="worker-sparklines-card" data-workers-grid-container>
      <div class="worker-spark-grid" data-workers-grid></div>
      <div class="d-none" data-workers-empty></div>
    </div>
  `;

  filterGroupEl = root.querySelector('[data-workers-filter]');
  gridContainerEl = root.querySelector('[data-workers-grid-container]');
  gridEl = root.querySelector('[data-workers-grid]');
  emptyEl = root.querySelector('[data-workers-empty]');

  filterGroupEl?.addEventListener('click', handleFilterClick);
  emptyEl?.addEventListener('click', handleEmptyClick);
  gridContainerEl?.addEventListener('contentvisibilityautostatechange', handleContentVisibilityChange);

  render(ctx, { force: true });
}

export function update(ctx) {
  render(ctx);
}

export function unmount() {
  filterGroupEl?.removeEventListener('click', handleFilterClick);
  emptyEl?.removeEventListener('click', handleEmptyClick);
  gridContainerEl?.removeEventListener('contentvisibilityautostatechange', handleContentVisibilityChange);

  root = null;
  filterGroupEl = null;
  gridContainerEl = null;
  gridEl = null;
  emptyEl = null;
  latestCtx = null;
  isGridOffscreen = false;
  lastRender = null;
}
