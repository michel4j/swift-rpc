/**
 * Monitor Dashboard v2 Entrypoint.
 * Wires Tabler folded-hover chrome, History API router, shared state store,
 * visibility-aware polling with AbortController cancellation, delegated
 * navigation/drawer/copy handlers, and active page lifecycle.
 */

import { announce, copyText, escapeHtml } from './format.js';
import { createRouter, normalizeRoute, parseQuery } from './router.js';
import { createDrawer } from './drawer.js';
import * as overviewPage from './pages/overview.js';
import * as workersPage from './pages/workers.js';
import * as methodsPage from './pages/methods.js';
import * as requestsPage from './pages/requests.js';

const HEARTBEAT_INTERVAL_MS = 1000;
const REQUESTS_PAGE_SIZE = 25;
const PAGE_POLL_INTERVAL_MS = {
  '/': 5000,
  '/workers': 5000,
  '/methods': 5000,
  '/requests': 2000,
};

const PAGES = {
  '/': overviewPage,
  '/workers': workersPage,
  '/methods': methodsPage,
  '/requests': requestsPage,
};

const state = {
  route: '/',
  window: '15m',
  query: {},
  data: null,
  summary: null,
  metrics: null,
  tasks: null,
  calls: null,
  recentFailures: null,
  autoRefresh: true,
  lastUpdated: null,
  connectionError: null,
};

const pageRoot = document.getElementById('page-root');
const serviceDescEl = document.getElementById('service-description');
const serviceVerEl = document.getElementById('service-version');
const topbarEndpointsEl = document.getElementById('topbar-broker-endpoints');
const navBadgeWorkersEl = document.getElementById('nav-badge-workers');
const navBadgeRequestsEl = document.getElementById('nav-badge-requests');
const connectionBannerEl = document.getElementById('connection-banner');
const connectionDetailEl = document.getElementById('connection-banner-detail');
const footerUpdatedEl = document.getElementById('footer-updated');
const autoRefreshInput = document.getElementById('auto-refresh');
const refreshNowBtn = document.getElementById('refresh-now');
const connectionRetryBtn = document.getElementById('connection-retry');
const themeToggleBtn = document.getElementById('theme-toggle');
const sidebarPinLabelEl = document.getElementById('sidebar-pin-label');
const sidebarMenuEl = document.getElementById('sidebar-menu');

let activePage = null;
let routeAbortController = new AbortController();
let heartbeatController = null;
let pageFetchController = null;
let heartbeatTimer = null;
let pageTimer = null;
let lastEndpointsSignature = '';
let openedDrawerRequestId = null;

function formatClock(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return '—';
  }
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function isAbortError(err) {
  return Boolean(err && (err.name === 'AbortError' || err.code === 20));
}

async function fetchJson(url, signal) {
  const response = await fetch(url, {
    signal,
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}`);
  }
  return response.json();
}

function makeLinkedController(parentSignal) {
  const controller = new AbortController();
  if (parentSignal) {
    if (parentSignal.aborted) {
      controller.abort();
    } else {
      parentSignal.addEventListener('abort', () => controller.abort(), { once: true });
    }
  }
  return controller;
}

function buildContext() {
  return {
    state,
    navigate: (route, params, opts) => router.navigate(route, params, opts),
    replaceQuery: (patch, opts) => router.replaceQuery(patch, opts),
    openRequest,
    refresh,
    fetchCalls,
    signal: routeAbortController.signal,
  };
}

function notifyActivePage() {
  if (activePage && typeof activePage.update === 'function') {
    activePage.update(buildContext());
  }
}

function updateConnectionBanner() {
  if (!connectionBannerEl) return;
  if (!state.connectionError) {
    connectionBannerEl.classList.add('d-none');
    return;
  }
  if (connectionDetailEl) {
    connectionDetailEl.textContent = state.lastUpdated
      ? `Last updated ${formatClock(state.lastUpdated)}. Showing last known data.`
      : 'Unable to reach Monitor API.';
  }
  connectionBannerEl.classList.remove('d-none');
}

function renderTopbarEndpoints(data) {
  if (!topbarEndpointsEl || !data) return;
  const pairs = [
    ['frontend', data.frontend],
    ['backend', data.backend],
    ['publish', data.publish],
    ['subscribe', data.subscribe],
  ].filter(([, value]) => Boolean(value));

  const sig = JSON.stringify(pairs);
  if (sig === lastEndpointsSignature) {
    return;
  }
  lastEndpointsSignature = sig;

  if (pairs.length === 0) {
    topbarEndpointsEl.innerHTML = '<span class="text-secondary">No Broker endpoints advertised.</span>';
    return;
  }

  topbarEndpointsEl.innerHTML = pairs.map(([label, url]) => `
    <div class="monitor-endpoint-row">
      <div class="min-w-0">
        <div class="text-secondary text-uppercase" style="font-size: 0.65rem; letter-spacing: 0.04em;">${escapeHtml(label)}</div>
        <code class="small text-break">${escapeHtml(url)}</code>
      </div>
      <button type="button" class="btn btn-sm btn-ghost-secondary py-0 px-2 flex-shrink-0"
              data-copy="${escapeHtml(url)}"
              data-copy-label="Copied ${escapeHtml(label)} endpoint"
              aria-label="Copy ${escapeHtml(label)} endpoint">Copy</button>
    </div>
  `).join('');
}

function updateChromeFromData(data) {
  if (!data) return;

  const description = data.description || 'Swift RPC';
  if (serviceDescEl) {
    serviceDescEl.textContent = description;
    serviceDescEl.title = description;
  }
  if (serviceVerEl) {
    if (data.version) {
      serviceVerEl.textContent = `v${String(data.version).replace(/^v/i, '')}`;
      serviceVerEl.classList.remove('d-none');
    } else {
      serviceVerEl.classList.add('d-none');
    }
  }
  document.title = data.description ? `${data.description} — Swift RPC Monitor` : 'Swift RPC Monitor';

  renderTopbarEndpoints(data);

  const activeWorkersCount = data.stats?.active_workers_count ?? (data.workers || []).length;
  if (navBadgeWorkersEl) {
    navBadgeWorkersEl.textContent = String(activeWorkersCount);
    navBadgeWorkersEl.setAttribute('aria-label', `${activeWorkersCount} active Workers`);
  }

  const activeRequestsCount = (data.active_calls || []).length;
  if (navBadgeRequestsEl) {
    navBadgeRequestsEl.textContent = String(activeRequestsCount);
    navBadgeRequestsEl.setAttribute('aria-label', `${activeRequestsCount} active Requests`);
    navBadgeRequestsEl.classList.toggle('d-none', activeRequestsCount <= 0);
  }

  if (footerUpdatedEl && state.lastUpdated) {
    footerUpdatedEl.textContent = formatClock(state.lastUpdated);
  }
}

function updateNavChrome() {
  document.querySelectorAll('[data-nav-item]').forEach((item) => {
    const isActive = item.getAttribute('data-nav-item') === state.route;
    item.classList.toggle('active', isActive);
    const link = item.querySelector('a.nav-link');
    if (link) {
      if (isActive) {
        link.setAttribute('aria-current', 'page');
      } else {
        link.removeAttribute('aria-current');
      }
    }
  });

  document.querySelectorAll('#window-selector button[data-window]').forEach((btn) => {
    const isSelected = btn.dataset.window === state.window;
    btn.classList.toggle('active', isSelected);
    btn.setAttribute('aria-pressed', String(isSelected));
  });
}

function syncSidebarPinLabel() {
  if (!sidebarPinLabelEl) return;
  const sidebarAttr = document.documentElement.getAttribute('data-bs-sidebar') || '';
  const isFolded = sidebarAttr.startsWith('folded');
  sidebarPinLabelEl.textContent = isFolded ? 'Pin sidebar' : 'Unpin sidebar';
}

function closeMobileSidebar() {
  if (!sidebarMenuEl || !sidebarMenuEl.classList.contains('show')) {
    return;
  }
  const Collapse = window.bootstrap?.Collapse;
  if (Collapse) {
    Collapse.getOrCreateInstance(sidebarMenuEl, { toggle: false }).hide();
  } else {
    sidebarMenuEl.classList.remove('show');
  }
}

async function fetchCalls(overrides = {}) {
  const win = overrides.window || state.window;
  const limit = Number(overrides.limit ?? REQUESTS_PAGE_SIZE);
  const rawOffset = overrides.offset !== undefined ? overrides.offset : state.query.offset;
  const offset = Math.max(0, Number.parseInt(rawOffset || '0', 10) || 0);
  const status = overrides.status !== undefined ? overrides.status : state.query.status;
  const method = overrides.method !== undefined ? overrides.method : state.query.method;
  const workerId = overrides.worker !== undefined
    ? overrides.worker
    : (overrides.worker_id !== undefined ? overrides.worker_id : state.query.worker);

  const params = new URLSearchParams({
    window: win,
    limit: String(limit),
    offset: String(offset),
  });
  if (status) params.set('status', String(status));
  if (method) params.set('method', String(method));
  if (workerId) params.set('worker_id', String(workerId));

  const signal = overrides.signal || routeAbortController.signal;
  const payload = await fetchJson(`/api/calls?${params.toString()}`, signal);
  if (overrides.updateState !== false) {
    state.calls = payload;
  }
  return payload;
}

async function fetchHeartbeat() {
  if (heartbeatController) {
    heartbeatController.abort();
  }
  const controller = new AbortController();
  heartbeatController = controller;

  try {
    const data = await fetchJson('/api/data', controller.signal);
    if (controller.signal.aborted) return;
    state.data = data;
    state.lastUpdated = new Date();
    state.connectionError = null;
    updateConnectionBanner();
    updateChromeFromData(data);
    notifyActivePage();
  } catch (err) {
    if (isAbortError(err)) return;
    state.connectionError = err?.message || 'Failed to fetch /api/data';
    updateConnectionBanner();
  } finally {
    if (heartbeatController === controller) {
      heartbeatController = null;
    }
  }
}

async function fetchActivePage() {
  if (pageFetchController) {
    pageFetchController.abort();
  }
  const controller = makeLinkedController(routeAbortController.signal);
  pageFetchController = controller;
  const { signal } = controller;
  const win = encodeURIComponent(state.window);

  try {
    switch (state.route) {
      case '/': {
        const [summary, failures] = await Promise.all([
          fetchJson(`/api/summary?window=${win}`, signal),
          fetchJson(`/api/calls?window=${win}&status=ERROR&limit=5`, signal),
        ]);
        if (signal.aborted) return;
        state.summary = summary;
        state.recentFailures = failures;
        break;
      }
      case '/workers': {
        const [metrics, tasks] = await Promise.all([
          fetchJson(`/api/metrics/workers?window=${win}`, signal),
          fetchJson(`/api/metrics/tasks?window=${win}`, signal),
        ]);
        if (signal.aborted) return;
        state.metrics = metrics;
        state.tasks = tasks;
        break;
      }
      case '/methods': {
        const summary = await fetchJson(`/api/summary?window=${win}`, signal);
        if (signal.aborted) return;
        state.summary = summary;
        break;
      }
      case '/requests': {
        await fetchCalls({ signal });
        if (signal.aborted) return;
        break;
      }
      default:
        break;
    }
    notifyActivePage();
  } catch (err) {
    if (isAbortError(err)) return;
    // Heartbeat owns the global connection banner; keep last good page data intact.
  } finally {
    if (pageFetchController === controller) {
      pageFetchController = null;
    }
  }
}

async function refresh() {
  if (refreshNowBtn) {
    refreshNowBtn.classList.add('btn-refresh-spinning');
  }
  try {
    await Promise.all([fetchHeartbeat(), fetchActivePage()]);
  } finally {
    if (refreshNowBtn) {
      refreshNowBtn.classList.remove('btn-refresh-spinning');
    }
  }
}

function stopTimers() {
  if (heartbeatTimer !== null) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (pageTimer !== null) {
    clearInterval(pageTimer);
    pageTimer = null;
  }
}

function startTimers() {
  stopTimers();
  if (!state.autoRefresh || document.hidden) {
    return;
  }
  heartbeatTimer = setInterval(() => {
    fetchHeartbeat();
  }, HEARTBEAT_INTERVAL_MS);

  const pageMs = PAGE_POLL_INTERVAL_MS[state.route] || 5000;
  pageTimer = setInterval(() => {
    fetchActivePage();
  }, pageMs);
}

const drawer = createDrawer({
  navigate: (route, params, opts) => router.navigate(route, params, opts),
  onClose: () => {
    openedDrawerRequestId = null;
    if (state.query.request) {
      router.replaceQuery({ request: null }, { push: false });
    }
  },
});

function openRequest(requestId, opts = {}) {
  if (!requestId) return;
  const id = String(requestId);
  if (state.query.request !== id) {
    router.replaceQuery({ request: id }, { push: true });
  }
  if (openedDrawerRequestId !== id || !drawer.isOpen()) {
    openedDrawerRequestId = id;
    drawer.open(id, opts);
  }
}

function syncDrawerWithQuery() {
  const reqId = state.query.request || null;
  if (reqId) {
    if (openedDrawerRequestId !== reqId || !drawer.isOpen()) {
      openedDrawerRequestId = reqId;
      drawer.open(reqId);
    }
  } else if (drawer.isOpen()) {
    openedDrawerRequestId = null;
    drawer.close({ silent: true });
  }
}

function handleRouteChange(next, prev) {
  const routeChanged = !prev || prev.route !== next.route;
  const windowChanged = !prev || prev.window !== next.window;
  const requestsQueryChanged = next.route === '/requests'
    && prev
    && prev.route === '/requests'
    && (
      prev.query.status !== next.query.status
      || prev.query.method !== next.query.method
      || prev.query.worker !== next.query.worker
      || prev.query.offset !== next.query.offset
    );

  state.route = next.route;
  state.window = next.window;
  state.query = next.query;

  updateNavChrome();

  if (routeChanged || windowChanged) {
    routeAbortController.abort();
    routeAbortController = new AbortController();
    if (pageFetchController) {
      pageFetchController.abort();
      pageFetchController = null;
    }
  }

  if (routeChanged) {
    if (activePage && typeof activePage.unmount === 'function') {
      activePage.unmount();
    }
    if (pageRoot) {
      pageRoot.replaceChildren();
    }
    activePage = PAGES[state.route] || overviewPage;
    if (pageRoot && activePage && typeof activePage.mount === 'function') {
      activePage.mount(pageRoot, buildContext());
    }
    startTimers();
    if (!document.hidden) {
      if (!prev) {
        fetchHeartbeat();
      }
      fetchActivePage();
    }
  } else if (windowChanged || requestsQueryChanged) {
    notifyActivePage();
    startTimers();
    if (!document.hidden) {
      fetchActivePage();
    }
  } else {
    notifyActivePage();
  }

  syncDrawerWithQuery();
}

const router = createRouter({ onChange: handleRouteChange });

// Delegated navigation, Request Details drawer, and copy triggers
document.addEventListener('click', (event) => {
  const copyEl = event.target.closest('[data-copy]');
  if (copyEl) {
    event.preventDefault();
    copyText(copyEl.dataset.copy, copyEl.dataset.copyLabel || 'Copied to clipboard');
    return;
  }

  const reqEl = event.target.closest('[data-open-req]');
  if (reqEl) {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) {
      return;
    }
    event.preventDefault();
    openRequest(reqEl.dataset.openReq, { triggerEl: reqEl });
    return;
  }

  const navEl = event.target.closest('[data-nav]');
  if (navEl) {
    if (
      event.metaKey
      || event.ctrlKey
      || event.shiftKey
      || event.altKey
      || event.button !== 0
      || navEl.getAttribute('target') === '_blank'
    ) {
      return;
    }
    event.preventDefault();
    const rawTarget = navEl.getAttribute('data-nav') || navEl.getAttribute('href') || '/';
    const parsed = new URL(rawTarget, window.location.origin);
    const targetRoute = normalizeRoute(parsed.pathname);
    const targetParams = parseQuery(parsed.search);
    closeMobileSidebar();
    router.navigate(targetRoute, targetParams, { replace: false, preserveWindow: true });
  }
});

// Window selector buttons
document.getElementById('window-selector')?.addEventListener('click', (event) => {
  const btn = event.target.closest('button[data-window]');
  if (!btn) return;
  router.setWindow(btn.dataset.window);
});

// Auto-refresh toggle
autoRefreshInput?.addEventListener('change', () => {
  state.autoRefresh = Boolean(autoRefreshInput.checked);
  if (state.autoRefresh) {
    startTimers();
    if (!document.hidden) {
      refresh();
    }
  } else {
    stopTimers();
  }
});

// Manual refresh & connection retry
refreshNowBtn?.addEventListener('click', () => {
  refresh();
});

connectionRetryBtn?.addEventListener('click', () => {
  refresh();
});

// 2-state Light/Dark theme toggle (no page reload)
themeToggleBtn?.addEventListener('click', () => {
  const currentAttr = document.documentElement.getAttribute('data-bs-theme');
  const resolved = currentAttr === 'dark' || currentAttr === 'light'
    ? currentAttr
    : (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  const next = resolved === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-bs-theme', next);
  try {
    localStorage.setItem('tabler-theme', next);
  } catch {
    // Ignore storage failures
  }
  announce(`Switched to ${next} theme`);
});

// Sidebar pin/unpin label sync
document.addEventListener('tabler:sidebar-folded', syncSidebarPinLabel);
syncSidebarPinLabel();

// Visibility safeguard: pause when hidden, resume immediately when visible
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    stopTimers();
    if (heartbeatController) {
      heartbeatController.abort();
      heartbeatController = null;
    }
    if (pageFetchController) {
      pageFetchController.abort();
      pageFetchController = null;
    }
  } else {
    startTimers();
    if (state.autoRefresh) {
      refresh();
    }
  }
});

// Boot router & initial view
router.start();
