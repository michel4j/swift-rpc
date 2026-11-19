/**
 * Methods page controller (stub for Issue #21 — Methods C).
 * Exports lifecycle hooks: mount(container, ctx), update(ctx), unmount().
 */

import { escapeHtml } from '../format.js';

let root = null;

function render(ctx) {
  if (!root) return;
  const methodsCount = (ctx.state.summary?.methods || []).length;
  const win = ctx.state.window;
  root.innerHTML = `
    <div class="page-header d-print-none mb-3">
      <div class="row align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Methods</h1>
        </div>
        <div class="col-auto text-secondary small">Window: <span class="font-mono">${escapeHtml(win)}</span></div>
      </div>
    </div>
    <div class="card">
      <div class="card-body text-secondary small">
        Recorded Methods in window: <strong class="font-mono">${escapeHtml(methodsCount)}</strong>
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
