/**
 * Shared Tabbed Right Offcanvas Request Details drawer controller.
 * Contract: createDrawer({ navigate, onClose }) -> { open(requestId, opts), close(opts), isOpen() }
 * Pure helpers (`extractErrorSummary`, `extractRawMethod`, `formatJsonPretty`) are
 * DOM-independent and cleanly importable under `node --test`.
 */

import {
  computeElapsedSeconds,
  copyText,
  escapeHtml,
  extractRawMethod,
  formatBytes,
  formatCallTimestamp,
  formatDisplayMethod,
  formatDuration,
  isAbortError,
  resolveTablerComponent,
  statusBadgeClass,
} from './format.js';

export { extractRawMethod };

const ERROR_PREVIEW_MAX_LEN = 240;
const COPY_FEEDBACK_MS = 1500;

const TABS = Object.freeze([
  { key: 'summary', label: 'Summary' },
  { key: 'args', label: 'Args' },
  { key: 'results', label: 'Results' },
  { key: 'raw', label: 'Raw JSON' },
]);

const COPY_LABELS = Object.freeze({
  args: 'Copied Args JSON',
  results: 'Copied Results JSON',
  raw: 'Copied Raw JSON',
});

function truncatePreview(text, maxLen = ERROR_PREVIEW_MAX_LEN) {
  const normalized = String(text ?? '').replace(/\s+/g, ' ').trim();
  if (normalized.length <= maxLen) {
    return normalized;
  }
  return `${normalized.slice(0, Math.max(1, maxLen - 1)).trimEnd()}…`;
}

/**
 * Safely pretty-print any value as 2-space indented JSON.
 */
export function formatJsonPretty(value) {
  try {
    const serialized = JSON.stringify(value === undefined ? null : value, null, 2);
    return serialized === undefined ? 'null' : serialized;
  } catch {
    return String(value);
  }
}

function normalizeErrorEntry(entry) {
  if (entry === null || entry === undefined) {
    return null;
  }

  if (typeof entry === 'string') {
    const trimmed = truncatePreview(entry);
    return trimmed ? { type: null, message: trimmed, code: null } : null;
  }

  if (typeof entry === 'object' && !Array.isArray(entry)) {
    if (entry._truncated && typeof entry.preview === 'string') {
      const preview = truncatePreview(entry.preview);
      return preview ? { type: null, message: preview, code: null } : null;
    }

    const type = entry.type !== undefined && entry.type !== null && String(entry.type).trim()
      ? String(entry.type).trim()
      : null;
    const code = entry.code !== undefined && entry.code !== null && String(entry.code).trim()
      ? String(entry.code).trim()
      : null;

    let message = '';
    if (entry.message !== undefined && entry.message !== null && String(entry.message).trim()) {
      message = truncatePreview(entry.message);
    } else if (entry.error !== undefined && entry.error !== null && String(entry.error).trim()) {
      message = truncatePreview(entry.error);
    }

    if (type || message || code) {
      return { type, message, code };
    }

    try {
      const jsonPreview = truncatePreview(JSON.stringify(entry));
      return jsonPreview ? { type: null, message: jsonPreview, code: null } : null;
    } catch {
      return null;
    }
  }

  const fallback = truncatePreview(String(entry));
  return fallback ? { type: null, message: fallback, code: null } : null;
}

/**
 * Extract a compact error summary `{ type, message, code }` from a Request details payload
 * when `status === 'ERROR'`. Returns `null` for non-ERROR payloads.
 */
export function extractErrorSummary(data) {
  if (!data || typeof data !== 'object') {
    return null;
  }
  if (String(data.status || '').toUpperCase() !== 'ERROR') {
    return null;
  }

  const results = data.results !== undefined ? data.results : data.result;

  if (Array.isArray(results)) {
    for (let i = results.length - 1; i >= 0; i -= 1) {
      const extracted = normalizeErrorEntry(results[i]);
      if (extracted) {
        return extracted;
      }
    }
  } else {
    const extracted = normalizeErrorEntry(results);
    if (extracted) {
      return extracted;
    }
  }

  if (data.error !== undefined) {
    const extracted = normalizeErrorEntry(data.error);
    if (extracted) {
      return extracted;
    }
  }

  return {
    type: null,
    message: 'Request failed with status ERROR.',
    code: null,
  };
}

function getOffcanvasCtor() {
  return resolveTablerComponent('Offcanvas');
}

function formatDurationCell(data) {
  if (data?.duration !== null && data?.duration !== undefined && data?.duration !== '') {
    return escapeHtml(formatDuration(data.duration));
  }
  if (String(data?.status || '').toUpperCase() === 'ACTIVE') {
    const elapsed = computeElapsedSeconds(data?.start_time);
    if (elapsed !== null) {
      return `${escapeHtml(formatDuration(elapsed))} <span class="text-secondary">(active)</span>`;
    }
  }
  return escapeHtml(formatDuration(null));
}

function renderErrorBanner(data) {
  const err = extractErrorSummary(data);
  if (!err) {
    return '';
  }

  const headerParts = [];
  if (err.type) {
    headerParts.push(`<strong class="font-mono">${escapeHtml(err.type)}</strong>`);
  }
  if (err.code) {
    headerParts.push(`<span class="badge bg-red-lt font-mono">${escapeHtml(err.code)}</span>`);
  }

  return `
    <div class="alert alert-danger py-2 px-3 mb-3" role="alert">
      <div class="d-flex align-items-start gap-2">
        <svg class="icon alert-icon flex-shrink-0 mt-1" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
             aria-hidden="true" focusable="false">
          <path d="M12 9v4"/>
          <path d="M10.363 3.591l-8.106 13.534a1.914 1.914 0 0 0 1.636 2.871h16.214a1.914 1.914 0 0 0 1.636 -2.87l-8.106 -13.536a1.914 1.914 0 0 0 -3.274 0z"/>
          <path d="M12 16h.01"/>
        </svg>
        <div class="min-w-0 small">
          ${headerParts.length > 0 ? `<div class="d-flex align-items-center flex-wrap gap-1">${headerParts.join('')}</div>` : ''}
          ${err.message ? `<div class="text-break${headerParts.length > 0 ? ' mt-1' : ''}">${escapeHtml(err.message)}</div>` : ''}
        </div>
      </div>
    </div>
  `;
}

function renderSummaryPanel(data) {
  const status = data?.status || 'UNKNOWN';
  const rawMethod = extractRawMethod(data);
  const displayMethod = formatDisplayMethod(data, rawMethod);
  const workerId = typeof data?.worker_id === 'string' ? data.worker_id.trim() : '';
  const clientId = typeof data?.client_id === 'string' && data.client_id.trim()
    ? data.client_id.trim()
    : '—';
  const timestamp = formatCallTimestamp(data);
  const durationHtml = formatDurationCell(data);
  const sentBytes = formatBytes(data?.sent_bytes ?? 0);
  const updatesCount = Number.isFinite(Number(data?.updates)) ? String(Number(data.updates)) : '0';

  const methodHtml = rawMethod
    ? `<a href="/requests?method=${encodeURIComponent(rawMethod)}"
          class="font-mono link-primary text-decoration-none text-break"
          data-drawer-nav="method"
          data-drawer-nav-value="${escapeHtml(rawMethod)}"
          title="Filter Requests by Method ${escapeHtml(rawMethod)}">${escapeHtml(displayMethod)}</a>`
    : '<span class="text-secondary">—</span>';

  const workerHtml = workerId
    ? `<a href="/requests?worker=${encodeURIComponent(workerId)}"
          class="font-mono link-primary text-decoration-none text-break"
          data-drawer-nav="worker"
          data-drawer-nav-value="${escapeHtml(workerId)}"
          title="Filter Requests by Worker ${escapeHtml(workerId)}">${escapeHtml(workerId)}</a>`
    : '<span class="text-secondary">—</span>';

  const rows = [
    ['Status', `<span class="${statusBadgeClass(status)}">${escapeHtml(status)}</span>`],
    ['Method', methodHtml],
    ['Worker', workerHtml],
    ['Client', `<span class="font-mono text-break">${escapeHtml(clientId)}</span>`],
    ['Timestamp', `<span class="font-mono">${escapeHtml(timestamp)}</span>`],
    ['Duration', `<span class="font-mono">${durationHtml}</span>`],
    ['Sent Bytes', `<span class="font-mono">${escapeHtml(sentBytes)}</span>`],
    ['Updates', `<span class="font-mono">${escapeHtml(updatesCount)}</span>`],
  ];

  const rowsHtml = rows.map(([label, valueHtml], index) => {
    const borderClass = index < rows.length - 1 ? ' border-bottom' : '';
    return `
      <div class="row g-2 py-2 align-items-baseline${borderClass}">
        <dt class="col-4 text-secondary fw-normal">${escapeHtml(label)}</dt>
        <dd class="col-8 mb-0 min-w-0">${valueHtml}</dd>
      </div>
    `;
  }).join('');

  return `${renderErrorBanner(data)}<dl class="mb-0 small">${rowsHtml}</dl>`;
}

function renderJsonPanel(tabKey, subtitle, label, jsonText) {
  return `
    <div class="d-flex align-items-center justify-content-between gap-2 mb-2">
      <span class="text-secondary small">${escapeHtml(subtitle)}</span>
      <button type="button" class="btn btn-sm btn-outline-secondary"
              data-drawer-copy="${escapeHtml(tabKey)}"
              aria-label="Copy ${escapeHtml(label)} JSON">Copy JSON</button>
    </div>
    <pre class="monitor-json-pre"><code>${escapeHtml(jsonText)}</code></pre>
  `;
}

export function createDrawer({
  navigate = () => {},
  onClose = () => {},
  fetchImpl = undefined,
} = {}) {
  const drawerEl = typeof document !== 'undefined'
    ? document.getElementById('request-details-drawer')
    : null;
  const idEl = typeof document !== 'undefined'
    ? document.getElementById('request-details-id')
    : null;
  const copyIdBtn = typeof document !== 'undefined'
    ? document.getElementById('request-details-copy-id')
    : null;
  const bodyEl = typeof document !== 'undefined'
    ? document.getElementById('request-details-body')
    : null;

  const doFetch = fetchImpl || (typeof globalThis !== 'undefined' ? globalThis.fetch : null);

  let currentRequestId = null;
  let closing = false;
  let openEpoch = 0;
  let activeTab = 'summary';
  let returnFocusEl = null;
  let fetchController = null;
  let jsonPayloads = { args: '{}', results: '[]', raw: '{}' };
  const pendingHideEpochs = [];
  const feedbackTimers = new Map();

  function clearFeedbackTimers() {
    for (const timer of feedbackTimers.values()) {
      clearTimeout(timer);
    }
    feedbackTimers.clear();
  }

  function flashButtonLabel(button, copiedText, defaultText) {
    if (!button) return;
    const prevTimer = feedbackTimers.get(button);
    if (prevTimer) {
      clearTimeout(prevTimer);
    }
    button.textContent = copiedText;
    const timer = setTimeout(() => {
      feedbackTimers.delete(button);
      if (button.isConnected) {
        button.textContent = defaultText;
      }
    }, COPY_FEEDBACK_MS);
    feedbackTimers.set(button, timer);
  }

  function abortInFlight() {
    if (fetchController) {
      fetchController.abort();
      fetchController = null;
    }
  }

  function activateTab(tabKey, { focusTab = false } = {}) {
    if (!bodyEl) return;
    if (!TABS.some((t) => t.key === tabKey)) return;
    activeTab = tabKey;

    bodyEl.querySelectorAll('[data-drawer-tab]').forEach((tabBtn) => {
      const isSelected = tabBtn.dataset.drawerTab === tabKey;
      tabBtn.classList.toggle('active', isSelected);
      tabBtn.setAttribute('aria-selected', String(isSelected));
      tabBtn.setAttribute('tabindex', isSelected ? '0' : '-1');
      if (isSelected && focusTab) {
        tabBtn.focus();
      }
    });

    bodyEl.querySelectorAll('[data-drawer-panel]').forEach((panel) => {
      const isSelected = panel.dataset.drawerPanel === tabKey;
      panel.hidden = !isSelected;
      panel.classList.toggle('d-none', !isSelected);
    });
  }

  function renderLoading() {
    if (!bodyEl) return;
    bodyEl.innerHTML = `
      <div class="d-flex align-items-center justify-content-center gap-2 text-secondary py-4" role="status" aria-live="polite">
        <div class="spinner-border spinner-border-sm" aria-hidden="true"></div>
        <span>Loading...</span>
      </div>
    `;
  }

  function renderNotFound(requestId, message) {
    if (!bodyEl) return;
    bodyEl.innerHTML = `
      <div class="alert alert-warning mb-0" role="alert">
        <div class="fw-semibold">${escapeHtml(message || 'Request not found')}</div>
        <div class="small text-secondary mt-1">
          No active or historical record found for Request <code class="font-mono text-break">${escapeHtml(requestId)}</code>.
        </div>
      </div>
    `;
  }

  function renderFetchError(message) {
    if (!bodyEl) return;
    bodyEl.innerHTML = `
      <div class="alert alert-danger mb-0" role="alert">
        <div class="d-flex align-items-start justify-content-between gap-2">
          <div class="min-w-0">
            <div class="fw-semibold">Failed to load Request details</div>
            <div class="small mt-1 text-break">${escapeHtml(message || 'Unable to reach Monitor API.')}</div>
          </div>
          <button type="button" class="btn btn-sm btn-danger flex-shrink-0" data-drawer-retry>Retry</button>
        </div>
      </div>
    `;
  }

  function renderDetails(data) {
    if (!bodyEl) return;

    const argsValue = data?.kwargs !== undefined ? data.kwargs : {};
    const resultsValue = data?.results !== undefined ? data.results : [];
    const argsJson = formatJsonPretty(argsValue);
    const resultsJson = formatJsonPretty(resultsValue);
    const rawJson = formatJsonPretty(data);

    jsonPayloads = {
      args: argsJson,
      results: resultsJson,
      raw: rawJson,
    };

    const resultsSubtitle = Array.isArray(resultsValue)
      ? `Results (${resultsValue.length} ${resultsValue.length === 1 ? 'chunk' : 'chunks'})`
      : 'Results payload';

    const tabItemsHtml = TABS.map(({ key, label }) => {
      const isSelected = key === activeTab;
      return `
        <li class="nav-item" role="presentation">
          <button type="button"
                  class="nav-link${isSelected ? ' active' : ''}"
                  id="req-tab-${key}"
                  role="tab"
                  data-drawer-tab="${key}"
                  aria-selected="${isSelected ? 'true' : 'false'}"
                  aria-controls="req-panel-${key}"
                  tabindex="${isSelected ? '0' : '-1'}">${escapeHtml(label)}</button>
        </li>
      `;
    }).join('');

    bodyEl.innerHTML = `
      <ul class="nav nav-tabs nav-fill mb-3" role="tablist" aria-label="Request details sections">
        ${tabItemsHtml}
      </ul>
      <div id="req-panel-summary" role="tabpanel" aria-labelledby="req-tab-summary"
           tabindex="0" data-drawer-panel="summary"${activeTab === 'summary' ? '' : ' hidden class="d-none"'}>
        ${renderSummaryPanel(data)}
      </div>
      <div id="req-panel-args" role="tabpanel" aria-labelledby="req-tab-args"
           tabindex="0" data-drawer-panel="args"${activeTab === 'args' ? '' : ' hidden class="d-none"'}>
        ${renderJsonPanel('args', 'Keyword arguments', 'Args', argsJson)}
      </div>
      <div id="req-panel-results" role="tabpanel" aria-labelledby="req-tab-results"
           tabindex="0" data-drawer-panel="results"${activeTab === 'results' ? '' : ' hidden class="d-none"'}>
        ${renderJsonPanel('results', resultsSubtitle, 'Results', resultsJson)}
      </div>
      <div id="req-panel-raw" role="tabpanel" aria-labelledby="req-tab-raw"
           tabindex="0" data-drawer-panel="raw"${activeTab === 'raw' ? '' : ' hidden class="d-none"'}>
        ${renderJsonPanel('raw', 'Full /api/details payload', 'Raw', rawJson)}
      </div>
    `;
  }

  async function loadRequest(requestId, epoch = openEpoch) {
    abortInFlight();
    clearFeedbackTimers();
    renderLoading();

    if (typeof doFetch !== 'function') {
      renderFetchError('Fetch API is unavailable.');
      return;
    }

    const controller = new AbortController();
    fetchController = controller;

    try {
      const response = await doFetch(`/api/details/${encodeURIComponent(requestId)}`, {
        signal: controller.signal,
        headers: { Accept: 'application/json' },
      });

      if (controller.signal.aborted || openEpoch !== epoch || currentRequestId !== requestId) {
        return;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} while loading Request details`);
      }

      const payload = await response.json();

      if (controller.signal.aborted || openEpoch !== epoch || currentRequestId !== requestId) {
        return;
      }

      if (!payload || typeof payload !== 'object') {
        throw new Error('Invalid Request details response');
      }

      if (typeof payload.error === 'string' && !payload.request_id) {
        renderNotFound(requestId, payload.error);
        return;
      }

      renderDetails(payload);
    } catch (err) {
      if (isAbortError(err) || controller.signal.aborted || openEpoch !== epoch || currentRequestId !== requestId) {
        return;
      }
      renderFetchError(err?.message || 'Network error while fetching Request details.');
    } finally {
      if (fetchController === controller) {
        fetchController = null;
      }
    }
  }

  function finalizeHidden({ silent = false } = {}) {
    abortInFlight();
    clearFeedbackTimers();

    closing = false;
    currentRequestId = null;
    activeTab = 'summary';
    jsonPayloads = { args: '{}', results: '[]', raw: '{}' };

    if (idEl) {
      idEl.textContent = '';
      idEl.removeAttribute('title');
    }
    if (copyIdBtn) {
      copyIdBtn.classList.add('d-none');
      copyIdBtn.textContent = 'Copy ID';
    }

    const focusTarget = returnFocusEl;
    returnFocusEl = null;
    if (focusTarget && typeof focusTarget.focus === 'function' && focusTarget.isConnected) {
      focusTarget.focus();
    }

    if (!silent) {
      onClose();
    }
  }

  function handleHidden() {
    if (pendingHideEpochs.length > 0) {
      const pending = pendingHideEpochs.shift();
      if (pending.epoch !== openEpoch) {
        return;
      }
      finalizeHidden({ silent: pending.silent });
      return;
    }

    if (!closing && currentRequestId === null) {
      return;
    }

    finalizeHidden({ silent: false });
  }

  if (drawerEl) {
    drawerEl.addEventListener('hidden.bs.offcanvas', handleHidden);
  }

  if (copyIdBtn) {
    copyIdBtn.addEventListener('click', async () => {
      if (!currentRequestId) return;
      const copied = await copyText(currentRequestId, 'Copied Request ID');
      if (copied) {
        flashButtonLabel(copyIdBtn, 'Copied', 'Copy ID');
      }
    });
  }

  if (bodyEl) {
    bodyEl.addEventListener('click', async (event) => {
      const tabBtn = event.target.closest('[data-drawer-tab]');
      if (tabBtn) {
        event.preventDefault();
        activateTab(tabBtn.dataset.drawerTab);
        return;
      }

      const copyBtn = event.target.closest('[data-drawer-copy]');
      if (copyBtn) {
        event.preventDefault();
        const key = copyBtn.dataset.drawerCopy;
        if (Object.prototype.hasOwnProperty.call(jsonPayloads, key)) {
          const copied = await copyText(jsonPayloads[key], COPY_LABELS[key] || 'Copied JSON');
          if (copied) {
            flashButtonLabel(copyBtn, 'Copied', 'Copy JSON');
          }
        }
        return;
      }

      const retryBtn = event.target.closest('[data-drawer-retry]');
      if (retryBtn) {
        event.preventDefault();
        if (currentRequestId) {
          loadRequest(currentRequestId, openEpoch);
        }
        return;
      }

      const navLink = event.target.closest('[data-drawer-nav]');
      if (navLink) {
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) {
          return;
        }
        event.preventDefault();
        const kind = navLink.dataset.drawerNav;
        const value = navLink.dataset.drawerNavValue || '';
        const params = kind === 'method' ? { method: value } : { worker: value };
        close({ silent: true, restoreFocus: false });
        navigate('/requests', params, { replace: false, preserveWindow: true });
      }
    });

    bodyEl.addEventListener('keydown', (event) => {
      const tabBtn = event.target.closest('[role="tab"][data-drawer-tab]');
      if (!tabBtn) return;

      const idx = TABS.findIndex((t) => t.key === tabBtn.dataset.drawerTab);
      if (idx === -1) return;

      let nextIdx = -1;
      if (event.key === 'ArrowRight') {
        nextIdx = (idx + 1) % TABS.length;
      } else if (event.key === 'ArrowLeft') {
        nextIdx = (idx - 1 + TABS.length) % TABS.length;
      } else if (event.key === 'Home') {
        nextIdx = 0;
      } else if (event.key === 'End') {
        nextIdx = TABS.length - 1;
      }

      if (nextIdx !== -1) {
        event.preventDefault();
        activateTab(TABS[nextIdx].key, { focusTab: true });
      }
    });
  }

  function open(requestId, { triggerEl = null } = {}) {
    if (!requestId) return;
    const id = String(requestId);

    openEpoch += 1;
    const epoch = openEpoch;
    closing = false;

    if (triggerEl && typeof triggerEl.focus === 'function') {
      returnFocusEl = triggerEl;
    } else if (!returnFocusEl && typeof document !== 'undefined') {
      const active = document.activeElement;
      if (active && active !== document.body && (!drawerEl || !drawerEl.contains(active))) {
        returnFocusEl = active;
      }
    }

    currentRequestId = id;
    activeTab = 'summary';

    if (idEl) {
      idEl.textContent = id;
      idEl.title = id;
    }
    if (copyIdBtn) {
      copyIdBtn.classList.remove('d-none');
      copyIdBtn.textContent = 'Copy ID';
    }

    const Offcanvas = getOffcanvasCtor();
    if (drawerEl && Offcanvas) {
      Offcanvas.getOrCreateInstance(drawerEl).show();
    }

    loadRequest(id, epoch);
  }

  function close({ silent = false, restoreFocus = true } = {}) {
    abortInFlight();
    if (!currentRequestId) return;

    const epoch = openEpoch;
    const isSilent = Boolean(silent);
    currentRequestId = null;
    closing = true;
    if (!restoreFocus) {
      returnFocusEl = null;
    }

    const Offcanvas = getOffcanvasCtor();
    const isVisibleInDom = Boolean(
      drawerEl
      && (drawerEl.classList.contains('show') || drawerEl.classList.contains('showing')),
    );
    if (drawerEl && Offcanvas && isVisibleInDom) {
      pendingHideEpochs.push({ epoch, silent: isSilent });
      Offcanvas.getOrCreateInstance(drawerEl).hide();
      return;
    }

    finalizeHidden({ silent: isSilent });
  }

  function isOpen() {
    return currentRequestId !== null && !closing;
  }

  return { open, close, isOpen };
}
