/**
 * Shared formatting, HTML escaping, status styling, clipboard, and Tabler helpers.
 * Pure helpers are DOM-independent and cleanly importable under `node --test`.
 */

export const REQUESTS_PAGE_SIZE = 15;

const VALID_STATUS_FILTERS = new Set(['DONE', 'ERROR']);

const HTML_ESCAPE_MAP = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};

/**
 * Coerce a value to a finite number, falling back when NaN/Infinity.
 */
export function toFiniteNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

/**
 * Deterministic case-insensitive string comparator with case-sensitive tie-break.
 */
export function compareIgnoreCase(a, b) {
  const aStr = String(a ?? '');
  const bStr = String(b ?? '');
  const aLower = aStr.toLowerCase();
  const bLower = bStr.toLowerCase();
  if (aLower < bLower) return -1;
  if (aLower > bLower) return 1;
  if (aStr < bStr) return -1;
  if (aStr > bStr) return 1;
  return 0;
}

/**
 * Extract the canonical raw method name (without trailing `(...)` signature)
 * for exact-match `/requests?method=<m>` filtering.
 */
export function extractRawMethod(call) {
  if (!call || typeof call !== 'object') {
    return '';
  }
  if (typeof call.raw_method === 'string' && call.raw_method.trim()) {
    return call.raw_method.trim();
  }
  if (typeof call.method === 'string' && call.method.trim()) {
    return call.method.replace(/\(.*$/, '').trim();
  }
  return '';
}

/**
 * Format a call's display method name, stripping redundant empty `()` suffix.
 */
export function formatDisplayMethod(call, fallback = '—') {
  const raw = extractRawMethod(call);
  const full = typeof call?.method === 'string' ? call.method.trim() : '';
  if (!full) {
    return raw || fallback;
  }
  if (raw && full === `${raw}()`) {
    return raw;
  }
  return full;
}

/**
 * Format a call timestamp from `date_time` or epoch `start_time` (seconds).
 */
export function formatCallTimestamp(call) {
  if (typeof call?.date_time === 'string' && call.date_time.trim()) {
    return call.date_time.trim();
  }
  const start = Number(call?.start_time);
  if (Number.isFinite(start) && start > 0) {
    return new Date(start * 1000).toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
  }
  return '—';
}

/**
 * Compute non-negative elapsed seconds from an epoch `startTime` in seconds.
 * Returns `null` if `startTime` is missing or invalid.
 */
export function computeElapsedSeconds(startTime, nowSeconds = Date.now() / 1000) {
  const start = Number(startTime);
  if (!Number.isFinite(start) || start <= 0) {
    return null;
  }
  return Math.max(0, nowSeconds - start);
}

/**
 * Normalize a status filter query parameter to `'DONE'`, `'ERROR'`, or `''` (All).
 */
export function normalizeStatusParam(raw) {
  const val = String(raw ?? '').trim().toUpperCase();
  return VALID_STATUS_FILTERS.has(val) ? val : '';
}

/**
 * Check whether an error represents an aborted fetch/request.
 */
export function isAbortError(err) {
  return Boolean(err && (err.name === 'AbortError' || err.code === 20));
}

/**
 * Resolve a Tabler v1.5.0 / Bootstrap JS component constructor (e.g. `Collapse`, `Offcanvas`).
 * Tabler's UMD bundle exports `window.tabler.<Name>` and `window.tabler.bootstrap.<Name>`,
 * without setting `window.bootstrap`.
 */
export function resolveTablerComponent(name, win = undefined) {
  const target = win !== undefined
    ? win
    : (typeof globalThis !== 'undefined' ? globalThis.window : undefined);
  if (!name || !target) {
    return null;
  }
  return target.tabler?.[name] || target.tabler?.bootstrap?.[name] || target.bootstrap?.[name] || null;
}

/**
 * Format a duration in seconds into a concise human-readable string.
 * Examples: null -> "-", 0 -> "0s", 0.0004 -> "<1ms", 0.25 -> "250ms",
 * 1.5 -> "1.50s", 5 -> "5s", 65 -> "1m 5s", 3661 -> "1h 1m 1s".
 */
export function formatDuration(seconds) {
  if (seconds === null || seconds === undefined || seconds === '') {
    return '-';
  }
  const num = Number(seconds);
  if (!Number.isFinite(num)) {
    return '-';
  }
  const abs = Math.abs(num);
  if (abs === 0) {
    return '0s';
  }
  if (abs < 1) {
    const ms = abs * 1000;
    return ms < 1 ? '<1ms' : `${Math.round(ms)}ms`;
  }
  if (abs < 60) {
    if (Math.abs(abs - Math.round(abs)) < 1e-9) {
      return `${Math.round(abs)}s`;
    }
    return abs < 10 ? `${abs.toFixed(2)}s` : `${abs.toFixed(1)}s`;
  }

  let remaining = Math.floor(abs);
  const days = Math.floor(remaining / 86400);
  remaining %= 86400;
  const hours = Math.floor(remaining / 3600);
  remaining %= 3600;
  const minutes = Math.floor(remaining / 60);
  const secs = remaining % 60;

  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (secs > 0 || parts.length === 0) parts.push(`${secs}s`);
  return parts.join(' ');
}

/**
 * Format a byte count into B / KB / MB / GB.
 */
export function formatBytes(bytes) {
  if (bytes === null || bytes === undefined || bytes === '') {
    return '0 B';
  }
  const num = Number(bytes);
  if (!Number.isFinite(num)) {
    return '0 B';
  }
  const val = Math.max(0, num);
  if (val < 1024) return `${Math.round(val)} B`;
  if (val < 1024 * 1024) return `${(val / 1024).toFixed(1)} KB`;
  if (val < 1024 * 1024 * 1024) return `${(val / (1024 * 1024)).toFixed(2)} MB`;
  return `${(val / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/**
 * Escape HTML special characters for safe interpolation.
 */
export function escapeHtml(value) {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value).replace(/[&<>"']/g, (ch) => HTML_ESCAPE_MAP[ch]);
}

/**
 * Map a Request status string to Tabler badge classes.
 */
export function statusBadgeClass(status) {
  switch (String(status || '').toUpperCase()) {
    case 'ACTIVE':
      return 'badge bg-blue-lt';
    case 'DONE':
      return 'badge bg-green-lt';
    case 'ERROR':
      return 'badge bg-red-lt';
    default:
      return 'badge bg-secondary-lt';
  }
}

/**
 * Announce a polite status message to screen readers via #live-announcer.
 */
export function announce(message) {
  if (typeof document === 'undefined') {
    return;
  }
  const el = document.getElementById('live-announcer');
  if (!el) {
    return;
  }
  const text = String(message || '');
  el.textContent = '';
  if (typeof requestAnimationFrame === 'function') {
    requestAnimationFrame(() => {
      el.textContent = text;
    });
  } else {
    el.textContent = text;
  }
}

/**
 * Copy text to clipboard with fallback for non-secure contexts and announce feedback.
 */
export async function copyText(text, label = 'Copied to clipboard') {
  const value = String(text ?? '');
  let copied = false;

  if (typeof navigator !== 'undefined' && navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
    try {
      await navigator.clipboard.writeText(value);
      copied = true;
    } catch {
      copied = false;
    }
  }

  if (!copied && typeof document !== 'undefined' && document.body) {
    const ta = document.createElement('textarea');
    ta.value = value;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    ta.style.pointerEvents = 'none';
    document.body.appendChild(ta);
    ta.select();
    try {
      copied = document.execCommand('copy');
    } catch {
      copied = false;
    }
    ta.remove();
  }

  announce(copied ? label : 'Copy failed');
  return copied;
}
