/**
 * Shared formatting, HTML escaping, status styling, and clipboard helpers.
 * Pure helpers are DOM-independent and cleanly importable under `node --test`.
 */

const HTML_ESCAPE_MAP = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};

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
