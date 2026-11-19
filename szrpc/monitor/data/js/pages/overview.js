/**
 * Overview page controller (stub for Issue #19 — Overview A).
 * Exports lifecycle hooks: mount(container, ctx), update(ctx), unmount().
 */

import { escapeHtml, formatDuration } from '../format.js';

let root = null;

function render(ctx) {
  if (!root) return;
  const stats = ctx.state.data?.stats;
  const win = ctx.state.window;
  root.innerHTML = `
    <div class="page-header d-print-none mb-3">
      <div class="row align-items-center">
        <div class="col">
          <div class="page-pretitle">Monitor</div>
          <h1 class="page-title">Overview</h1>
        </div>
        <div class="col-auto text-secondary small">Window: <span class="font-mono">${escapeHtml(win)}</span></div>
      </div>
    </div>
    <div class="card">
      <div class="card-body">
        <div class="text-secondary small">
          ${stats
            ? `Uptime <strong class="font-mono">${escapeHtml(formatDuration(Math.floor(stats.uptime || 0)))}</strong> · Requests <strong class="font-mono">${escapeHtml(stats.total_requests)}</strong> · Failures <strong class="font-mono">${escapeHtml(stats.total_errors)}</strong> · Active Workers <strong class="font-mono">${escapeHtml(stats.active_workers_count)}</strong>`
            : 'Loading Monitor data…'}
        </div>
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
