/**
 * Shared Right Offcanvas Request Details drawer controller (stub for Issue #18).
 * Contract: createDrawer({ navigate, onClose }) -> { open(requestId, opts), close(opts), isOpen() }
 */

import { escapeHtml } from './format.js';

export function createDrawer({ navigate = () => {}, onClose = () => {} } = {}) {
  const drawerEl = typeof document !== 'undefined'
    ? document.getElementById('request-details-drawer')
    : null;
  const idEl = typeof document !== 'undefined'
    ? document.getElementById('request-details-id')
    : null;
  const bodyEl = typeof document !== 'undefined'
    ? document.getElementById('request-details-body')
    : null;

  let currentRequestId = null;
  let returnFocusEl = null;
  let suppressCloseCallback = false;

  if (drawerEl) {
    drawerEl.addEventListener('hidden.bs.offcanvas', () => {
      const shouldNotify = !suppressCloseCallback;
      suppressCloseCallback = false;
      currentRequestId = null;

      if (returnFocusEl && typeof returnFocusEl.focus === 'function' && returnFocusEl.isConnected) {
        returnFocusEl.focus();
      }
      returnFocusEl = null;

      if (shouldNotify) {
        onClose();
      }
    });
  }

  function open(requestId, { triggerEl = null } = {}) {
    if (!requestId) return;
    currentRequestId = String(requestId);
    returnFocusEl = triggerEl || (typeof document !== 'undefined' ? document.activeElement : null);

    if (idEl) {
      idEl.textContent = currentRequestId;
    }
    if (bodyEl) {
      bodyEl.innerHTML = `<p class="text-secondary small">Request <code>${escapeHtml(currentRequestId)}</code> details viewer will be populated in Monitor v2 [2/6].</p>`;
    }

    const Offcanvas = typeof window !== 'undefined' ? window.bootstrap?.Offcanvas : null;
    if (drawerEl && Offcanvas) {
      Offcanvas.getOrCreateInstance(drawerEl).show();
    }
  }

  function close({ silent = false } = {}) {
    if (!currentRequestId) return;
    suppressCloseCallback = Boolean(silent);
    const Offcanvas = typeof window !== 'undefined' ? window.bootstrap?.Offcanvas : null;
    if (drawerEl && Offcanvas) {
      Offcanvas.getOrCreateInstance(drawerEl).hide();
    } else {
      currentRequestId = null;
      if (!silent) {
        onClose();
      }
    }
  }

  function isOpen() {
    return currentRequestId !== null;
  }

  // Keep reference to navigate for the full #18 implementation signature
  void navigate;

  return { open, close, isOpen };
}
