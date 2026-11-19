/**
 * Requests page controller (stub for Issue #22 — Requests C).
 * Exports lifecycle hooks: mount(container, ctx), update(ctx), unmount().
 */

import { escapeHtml } from '../format.js';

let root = null;

function render(ctx) {
  if (!root) return;
  const activeCount = (ctx.state.data?.active_calls || []).length;
  const totalCount = ctx.state.calls?.total ?? 0;
  const win = ctx.state.window;
  root.innerHTML = `
    <div class="page-header d-print-none mb-3">
      <div class="row align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Requests</h1>
        </div>
        <div class="col-auto text-secondary small">Window: <span class="font-mono">${escapeHtml(win)}</span></div>
      </div>
    </div>
    <div class="card">
      <div class="card-body text-secondary small">
        Active Requests: <strong class="font-mono">${escapeHtml(activeCount)}</strong> · Historical Requests in window: <strong class="font-mono">${escapeHtml(totalCount)}</strong>
      </div>
    </div>
  `;
}

export function mount(container, ctx) {
  root = container;
  render(ctx);
}

export function update(ctx) {
  render(ctx);
}

export function unmount() {
  root = null;
}
