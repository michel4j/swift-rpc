import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  REQUESTS_PAGE_SIZE as FORMAT_REQUESTS_PAGE_SIZE,
  compareIgnoreCase,
  escapeHtml,
  formatBytes,
  formatCallTimestamp,
  formatDisplayMethod,
  formatDuration,
  isAbortError,
  normalizeStatusParam as formatNormalizeStatusParam,
  resolveTablerComponent,
  statusBadgeClass,
  toFiniteNumber,
} from '../../szrpc/monitor/data/js/format.js';
import {
  DEFAULT_WINDOW,
  WINDOW_STORAGE_KEY,
  buildUrl,
  createRouter,
  normalizeRoute,
  parseQuery,
  resolveWindow,
} from '../../szrpc/monitor/data/js/router.js';
import {
  createDrawer,
  extractErrorSummary,
  extractRawMethod,
  formatJsonPretty,
} from '../../szrpc/monitor/data/js/drawer.js';
import {
  computeElapsedSeconds,
  computeErrorRatePct,
  computeQueueDepthStatus,
  computeQueueWaitStatus,
  extractBrokerEndpoints,
  selectTopMethods,
} from '../../szrpc/monitor/data/js/pages/overview.js';
import { normalizeStatusFilter } from '../../szrpc/monitor/data/js/pages/workers.js';
import { partitionWorkers } from '../../szrpc/monitor/data/sparklines.js';
import {
  computeMethodStats,
  computeWhiskerGeometry,
  errorRateBadgeClass,
  filterAndSortMethods,
} from '../../szrpc/monitor/data/js/pages/methods.js';
import {
  REQUESTS_PAGE_SIZE,
  collectKnownMethodsAndWorkers,
  computePaginationRange,
  filterActiveCalls,
  filterHistoricalCalls,
  normalizeStatusParam,
} from '../../szrpc/monitor/data/js/pages/requests.js';

describe('format.js', () => {
  it('formats durations across sub-ms, ms, seconds, minutes, hours, and days', () => {
    assert.equal(formatDuration(null), '-');
    assert.equal(formatDuration(undefined), '-');
    assert.equal(formatDuration(''), '-');
    assert.equal(formatDuration(Number.NaN), '-');
    assert.equal(formatDuration(0), '0s');
    assert.equal(formatDuration(0.0004), '<1ms');
    assert.equal(formatDuration(0.25), '250ms');
    assert.equal(formatDuration(1.5), '1.50s');
    assert.equal(formatDuration(5), '5s');
    assert.equal(formatDuration(12.34), '12.3s');
    assert.equal(formatDuration(65), '1m 5s');
    assert.equal(formatDuration(3661), '1h 1m 1s');
    assert.equal(formatDuration(90061), '1d 1h 1m 1s');
    assert.equal(formatDuration(-0.5), '500ms');
  });

  it('formats byte counts cleanly', () => {
    assert.equal(formatBytes(null), '0 B');
    assert.equal(formatBytes(undefined), '0 B');
    assert.equal(formatBytes(''), '0 B');
    assert.equal(formatBytes(Number.NaN), '0 B');
    assert.equal(formatBytes(-10), '0 B');
    assert.equal(formatBytes(512), '512 B');
    assert.equal(formatBytes(1024), '1.0 KB');
    assert.equal(formatBytes(1536), '1.5 KB');
    assert.equal(formatBytes(1024 * 1024), '1.00 MB');
    assert.equal(formatBytes(2.5 * 1024 * 1024 * 1024), '2.50 GB');
  });

  it('escapes HTML special characters', () => {
    assert.equal(escapeHtml(null), '');
    assert.equal(escapeHtml(undefined), '');
    assert.equal(escapeHtml(42), '42');
    assert.equal(
      escapeHtml(`<script a="1" b='2'>&</script>`),
      '&lt;script a=&quot;1&quot; b=&#39;2&#39;&gt;&amp;&lt;/script&gt;',
    );
  });

  it('maps status values to Tabler badge classes', () => {
    assert.equal(statusBadgeClass('ACTIVE'), 'badge bg-blue-lt');
    assert.equal(statusBadgeClass('DONE'), 'badge bg-green-lt');
    assert.equal(statusBadgeClass('ERROR'), 'badge bg-red-lt');
    assert.equal(statusBadgeClass('error'), 'badge bg-red-lt');
    assert.equal(statusBadgeClass('UNKNOWN'), 'badge bg-secondary-lt');
    assert.equal(statusBadgeClass(null), 'badge bg-secondary-lt');
  });

  it('resolves Tabler v1.5.0 components when window.bootstrap is undefined', () => {
    class FakeCollapse {}
    class FakeOffcanvas {}
    class FakeNestedModal {}
    class FakeBootstrapTooltip {}

    const prevWindow = globalThis.window;
    try {
      globalThis.window = {
        tabler: {
          Collapse: FakeCollapse,
          Offcanvas: FakeOffcanvas,
          bootstrap: { Modal: FakeNestedModal },
        },
        bootstrap: undefined,
      };
      assert.equal(resolveTablerComponent('Collapse'), FakeCollapse);
      assert.equal(resolveTablerComponent('Offcanvas'), FakeOffcanvas);
      assert.equal(resolveTablerComponent('Modal'), FakeNestedModal);
      assert.equal(resolveTablerComponent('Missing'), null);

      globalThis.window = { bootstrap: { Tooltip: FakeBootstrapTooltip } };
      assert.equal(resolveTablerComponent('Tooltip'), FakeBootstrapTooltip);
      assert.equal(resolveTablerComponent('Offcanvas'), null);

      delete globalThis.window;
      assert.equal(resolveTablerComponent('Collapse'), null);
    } finally {
      if (prevWindow === undefined) {
        delete globalThis.window;
      } else {
        globalThis.window = prevWindow;
      }
    }
  });

  it('normalizes status parameters and shared call helpers', () => {
    assert.equal(FORMAT_REQUESTS_PAGE_SIZE, 15);
    assert.equal(formatNormalizeStatusParam('error'), 'ERROR');
    assert.equal(formatNormalizeStatusParam('ACTIVE'), '');
    assert.equal(formatNormalizeStatusParam('done'), 'DONE');
    assert.equal(formatNormalizeStatusParam('  ERROR '), 'ERROR');
    assert.equal(formatNormalizeStatusParam('unknown'), '');
    assert.equal(formatNormalizeStatusParam(null), '');

    assert.equal(toFiniteNumber('42.5', 0), 42.5);
    assert.equal(toFiniteNumber('NaN', 7), 7);
    assert.equal(compareIgnoreCase('Alpha', 'beta'), -1);
    assert.equal(compareIgnoreCase('alpha', 'Alpha'), 1);
    assert.equal(formatDisplayMethod({ raw_method: 'echo', method: 'echo()' }), 'echo');
    assert.equal(formatDisplayMethod({ method: 'echo(x=1)' }), 'echo(x=1)');
    assert.equal(formatCallTimestamp({ date_time: '2026-01-01 00:00:00' }), '2026-01-01 00:00:00');
    assert.equal(formatCallTimestamp({ start_time: 0 }), '—');
    assert.equal(isAbortError({ name: 'AbortError' }), true);
    assert.equal(isAbortError({ code: 20 }), true);
    assert.equal(isAbortError(new Error('network')), false);
  });
});

describe('router.js', () => {
  it('normalizes routes and strips trailing slashes', () => {
    assert.equal(normalizeRoute('/'), '/');
    assert.equal(normalizeRoute('/workers'), '/workers');
    assert.equal(normalizeRoute('/methods/'), '/methods');
    assert.equal(normalizeRoute('/requests///'), '/requests');
    assert.equal(normalizeRoute('/unknown'), '/');
    assert.equal(normalizeRoute(''), '/');
    assert.equal(normalizeRoute(null), '/');
  });

  it('parses query strings and skips empty values', () => {
    assert.deepEqual(parseQuery('?window=1h&method=foo&empty='), {
      window: '1h',
      method: 'foo',
    });
    assert.deepEqual(parseQuery(new URLSearchParams({ a: '1', b: '' })), { a: '1' });
    assert.deepEqual(parseQuery(''), {});
  });

  it('resolves window priority: query > storage > default', () => {
    const store = new Map([[WINDOW_STORAGE_KEY, '12h']]);
    const mockStorage = {
      getItem: (k) => store.get(k) ?? null,
      setItem: (k, v) => store.set(k, v),
    };

    assert.equal(resolveWindow('?window=7d', mockStorage), '7d');
    assert.equal(resolveWindow('?window=invalid', mockStorage), '12h');
    assert.equal(resolveWindow('', mockStorage), '12h');
    store.set(WINDOW_STORAGE_KEY, 'bad');
    assert.equal(resolveWindow('', mockStorage), DEFAULT_WINDOW);
  });

  it('builds normalized URLs omitting null/undefined/empty params', () => {
    assert.equal(buildUrl('/requests/', { window: '1h', method: 'echo', worker: '', offset: null }), '/requests?window=1h&method=echo');
    assert.equal(buildUrl('/bad', {}), '/');
  });

  it('manages pushState, replaceState, setWindow offset reset, and popstate', () => {
    const historyCalls = [];
    const listeners = new Map();
    const storageMap = new Map();
    const mockStorage = {
      getItem: (k) => storageMap.get(k) ?? null,
      setItem: (k, v) => storageMap.set(k, v),
    };
    const mockWin = {
      location: { pathname: '/requests', search: '?window=1h&offset=50&method=echo' },
      history: {
        pushState: (_s, _t, url) => {
          historyCalls.push(['push', url]);
          const u = new URL(url, 'http://localhost');
          mockWin.location.pathname = u.pathname;
          mockWin.location.search = u.search;
        },
        replaceState: (_s, _t, url) => {
          historyCalls.push(['replace', url]);
          const u = new URL(url, 'http://localhost');
          mockWin.location.pathname = u.pathname;
          mockWin.location.search = u.search;
        },
      },
      addEventListener: (ev, fn) => listeners.set(ev, fn),
      removeEventListener: (ev) => listeners.delete(ev),
    };

    const changes = [];
    const router = createRouter({
      onChange: (next, prev) => changes.push({ next, prev }),
      storage: mockStorage,
      win: mockWin,
    });

    router.start();
    assert.equal(router.getState().route, '/requests');
    assert.equal(router.getState().window, '1h');
    assert.equal(router.getState().query.offset, '50');

    router.setWindow('12h');
    assert.equal(router.getState().window, '12h');
    assert.equal(router.getState().query.offset, undefined);
    assert.equal(historyCalls.at(-1)[0], 'replace');

    router.navigate('/methods', {});
    assert.equal(router.getState().route, '/methods');
    assert.equal(router.getState().window, '12h');
    assert.equal(router.getState().query.method, undefined);
    assert.equal(historyCalls.at(-1)[0], 'push');

    router.replaceQuery({ request: 'req-42' }, { push: true });
    assert.equal(router.getState().query.request, 'req-42');
    assert.equal(historyCalls.at(-1)[0], 'push');

    mockWin.location.pathname = '/workers';
    mockWin.location.search = '?window=1d&status=active';
    listeners.get('popstate')();
    assert.equal(router.getState().route, '/workers');
    assert.equal(router.getState().window, '1d');
    assert.equal(router.getState().query.status, 'active');

    router.stop();
    assert.equal(listeners.has('popstate'), false);
  });
});

describe('drawer.js', () => {
  it('extracts canonical raw method names', () => {
    assert.equal(extractRawMethod({ raw_method: 'ping_pong', method: 'ping_pong(msg="hi")' }), 'ping_pong');
    assert.equal(extractRawMethod({ method: 'compute(x=1, y=2)' }), 'compute');
    assert.equal(extractRawMethod({ method: 'plain' }), 'plain');
    assert.equal(extractRawMethod(null), '');
  });

  it('extracts error summaries for ERROR payloads and returns null otherwise', () => {
    assert.equal(extractErrorSummary({ status: 'DONE', results: ['ok'] }), null);
    assert.equal(extractErrorSummary({ status: 'ACTIVE', results: [] }), null);
    assert.equal(extractErrorSummary(null), null);

    assert.deepEqual(
      extractErrorSummary({
        status: 'ERROR',
        results: ['chunk-1', { type: 'ValueError', message: 'bad input', code: 'E400' }],
      }),
      { type: 'ValueError', message: 'bad input', code: 'E400' },
    );

    assert.deepEqual(
      extractErrorSummary({
        status: 'ERROR',
        results: [{ _truncated: true, preview: 'truncated error trace' }],
      }),
      { type: null, message: 'truncated error trace', code: null },
    );

    assert.deepEqual(
      extractErrorSummary({ status: 'ERROR', results: ['Worker lost connection'] }),
      { type: null, message: 'Worker lost connection', code: null },
    );

    assert.deepEqual(
      extractErrorSummary({ status: 'ERROR', results: [] }),
      { type: null, message: 'Request failed with status ERROR.', code: null },
    );
  });

  it('pretty-prints JSON safely', () => {
    assert.equal(formatJsonPretty({ a: 1 }), '{\n  "a": 1\n}');
    assert.equal(formatJsonPretty(undefined), 'null');
  });

  it('handles drilldown silent close and delayed hidden.bs.offcanvas without firing onClose or clobbering a newer open()', async () => {
    const makeClassList = () => {
      const classes = new Set();
      return {
        add: (...names) => names.forEach((n) => classes.add(n)),
        remove: (...names) => names.forEach((n) => classes.delete(n)),
        contains: (name) => classes.has(name),
        toggle: (name, force) => {
          if (force === undefined) {
            if (classes.has(name)) { classes.delete(name); return false; }
            classes.add(name); return true;
          }
          if (force) classes.add(name); else classes.delete(name);
          return Boolean(force);
        },
      };
    };

    const makeEl = () => {
      const listeners = new Map();
      const attrs = new Map();
      const el = {
        classList: makeClassList(),
        textContent: '',
        innerHTML: '',
        title: '',
        isConnected: true,
        contains: () => false,
        setAttribute: (k, v) => attrs.set(k, v),
        removeAttribute: (k) => { attrs.delete(k); if (k === 'title') el.title = ''; },
        addEventListener: (ev, fn) => {
          if (!listeners.has(ev)) listeners.set(ev, []);
          listeners.get(ev).push(fn);
        },
        dispatch: (ev) => {
          for (const fn of (listeners.get(ev) || [])) fn({ type: ev });
        },
      };
      return el;
    };

    const drawerEl = makeEl();
    const idEl = makeEl();
    const copyIdBtn = makeEl();
    const bodyEl = makeEl();
    const elements = {
      'request-details-drawer': drawerEl,
      'request-details-id': idEl,
      'request-details-copy-id': copyIdBtn,
      'request-details-body': bodyEl,
    };

    let hideCalls = 0;
    const pendingHiddenCallbacks = [];
    class FakeOffcanvas {
      static getOrCreateInstance(el) {
        if (!el._fakeInstance) {
          el._fakeInstance = {
            show() {
              el.classList.add('show');
            },
            hide() {
              hideCalls += 1;
              pendingHiddenCallbacks.push(() => {
                el.classList.remove('show');
                el.dispatch('hidden.bs.offcanvas');
              });
            },
          };
        }
        return el._fakeInstance;
      }
    }

    const prevDocument = globalThis.document;
    const prevWindow = globalThis.window;
    try {
      globalThis.document = {
        body: {},
        activeElement: null,
        getElementById: (id) => elements[id] || null,
      };
      globalThis.window = { tabler: { Offcanvas: FakeOffcanvas } };

      let closeCount = 0;
      const focused = [];
      const trigger1 = { isConnected: true, focus: () => focused.push('t1') };
      const trigger2 = { isConnected: true, focus: () => focused.push('t2') };
      const fetchImpl = async (url) => ({
        ok: true,
        json: async () => ({
          request_id: decodeURIComponent(url.split('/').pop()),
          status: 'DONE',
          method: 'echo()',
          kwargs: {},
          results: [],
        }),
      });

      const drawer = createDrawer({
        onClose: () => { closeCount += 1; },
        fetchImpl,
      });

      // Case 1: drilldown close({ silent: true, restoreFocus: false }) followed by delayed hidden.bs.offcanvas
      drawer.open('req-1', { triggerEl: trigger1 });
      assert.equal(drawer.isOpen(), true);
      assert.equal(idEl.textContent, 'req-1');

      drawer.close({ silent: true, restoreFocus: false });
      assert.equal(drawer.isOpen(), false);
      assert.equal(hideCalls, 1);

      // Calling close() again while transition is in flight is a no-op
      drawer.close({ silent: false });
      assert.equal(hideCalls, 1);

      // Delayed transition finishes
      pendingHiddenCallbacks.shift()();
      assert.equal(closeCount, 0);
      assert.deepEqual(focused, []);
      assert.equal(idEl.textContent, '');

      // Case 2: silent close followed immediately by open('req-2') BEFORE delayed hidden.bs.offcanvas fires
      drawer.open('req-1', { triggerEl: trigger1 });
      drawer.close({ silent: true, restoreFocus: false });
      assert.equal(drawer.isOpen(), false);

      drawer.open('req-2', { triggerEl: trigger2 });
      assert.equal(drawer.isOpen(), true);
      assert.equal(idEl.textContent, 'req-2');

      // Stale hidden.bs.offcanvas from req-1 arrives after req-2 opened
      pendingHiddenCallbacks.shift()();
      assert.equal(closeCount, 0);
      assert.equal(drawer.isOpen(), true);
      assert.equal(idEl.textContent, 'req-2');
      assert.deepEqual(focused, []);

      // Subsequent genuine user dismiss of req-2 fires onClose() and restores focus to trigger2
      drawerEl.classList.remove('show');
      drawerEl.dispatch('hidden.bs.offcanvas');
      assert.equal(closeCount, 1);
      assert.equal(drawer.isOpen(), false);
      assert.deepEqual(focused, ['t2']);
    } finally {
      if (prevDocument === undefined) delete globalThis.document; else globalThis.document = prevDocument;
      if (prevWindow === undefined) delete globalThis.window; else globalThis.window = prevWindow;
    }
  });
});

describe('pages/overview.js', () => {
  it('computes clamped error rate percentage', () => {
    assert.equal(computeErrorRatePct(0, 0), 0);
    assert.equal(computeErrorRatePct(0, 5), 0);
    assert.equal(computeErrorRatePct(200, 10), 5);
    assert.equal(computeErrorRatePct(10, 50), 100);
  });

  it('selects top methods sorted by count desc, errors desc, name asc without mutating input', () => {
    const input = [
      { method: 'beta', count: 10, errors: 1 },
      { method: 'alpha', count: 10, errors: 1 },
      { method: 'gamma', count: 10, errors: 3 },
      { method: 'top', count: 25, errors: 0 },
    ];
    const snapshot = input.map((x) => x.method);
    const out = selectTopMethods(input, 3);
    assert.deepEqual(out.map((x) => x.method), ['top', 'gamma', 'alpha']);
    assert.deepEqual(input.map((x) => x.method), snapshot);
    assert.deepEqual(selectTopMethods(null, 5), []);
  });

  it('extracts non-empty Broker endpoints in canonical order', () => {
    assert.deepEqual(
      extractBrokerEndpoints({
        frontend: 'tcp://*:9990',
        backend: '  ',
        publish: 'tcp://*:9992',
        subscribe: null,
      }),
      [
        { key: 'frontend', label: 'Frontend', url: 'tcp://*:9990' },
        { key: 'publish', label: 'Publish', url: 'tcp://*:9992' },
      ],
    );
    assert.deepEqual(extractBrokerEndpoints(null), []);
  });

  it('computes non-negative elapsed seconds', () => {
    assert.equal(computeElapsedSeconds(100, 112.5), 12.5);
    assert.equal(computeElapsedSeconds(200, 150), 0);
    assert.equal(computeElapsedSeconds(null, 150), null);
    assert.equal(computeElapsedSeconds(0, 150), null);
  });

  it('computes broker queue depth status thresholds', () => {
    assert.deepEqual(computeQueueDepthStatus(0, 0), {
      label: 'Idle',
      dotClass: 'status-dot bg-success',
      badgeClass: 'badge bg-success-lt',
    });
    assert.deepEqual(computeQueueDepthStatus(3, 1.2), {
      label: 'Queued',
      dotClass: 'status-dot status-dot-animated bg-warning',
      badgeClass: 'badge bg-warning-lt',
    });
    assert.deepEqual(computeQueueDepthStatus(2, 5.0), {
      label: 'High',
      dotClass: 'status-dot status-dot-animated bg-danger',
      badgeClass: 'badge bg-danger-lt',
    });
  });

  it('computes broker queue wait p95 status thresholds', () => {
    assert.deepEqual(computeQueueWaitStatus(0.01), {
      label: 'Healthy',
      badgeClass: 'badge bg-success-lt',
    });
    assert.deepEqual(computeQueueWaitStatus(0.05), {
      label: 'Elevated',
      badgeClass: 'badge bg-warning-lt',
    });
    assert.deepEqual(computeQueueWaitStatus(0.5), {
      label: 'High',
      badgeClass: 'badge bg-danger-lt',
    });
  });
});

describe('pages/workers.js & sparklines.js', () => {
  it('normalizes worker status filter', () => {
    assert.equal(normalizeStatusFilter('active'), 'active');
    assert.equal(normalizeStatusFilter('OFFLINE'), 'offline');
    assert.equal(normalizeStatusFilter('bogus'), 'all');
    assert.equal(normalizeStatusFilter(null), 'all');
  });

  it('partitions workers into sorted active first then sorted offline', () => {
    const res = partitionWorkers({
      metricsPayload: { series: { 'w-b': [], 'w-c': [] } },
      tasksPayload: { tasks: { 'w-a': [], 'w-d': [] } },
      activeWorkers: new Set(['w-c', 'w-a']),
    });
    assert.deepEqual(res.active, ['w-a', 'w-c']);
    assert.deepEqual(res.offline, ['w-b', 'w-d']);
    assert.deepEqual(res.ordered, ['w-a', 'w-c', 'w-b', 'w-d']);
  });
});

describe('pages/methods.js', () => {
  const sampleMethods = [
    {
      method: 'fast',
      count: 100,
      errors: 0,
      min_duration: 0.01,
      avg_duration: 0.05,
      p95_duration: 0.1,
      max_duration: 0.2,
      total_bytes: 1000,
    },
    {
      method: 'flaky_job',
      count: 20,
      errors: 4,
      min_duration: 0.1,
      avg_duration: 0.4,
      p95_duration: 0.9,
      max_duration: 1.0,
      total_bytes: 5000,
    },
    {
      method: 'medium_job',
      count: 50,
      errors: 1,
      min_duration: 0.05,
      avg_duration: 0.2,
      p95_duration: 0.5,
      max_duration: 0.8,
      total_bytes: 2500,
    },
  ];

  it('aggregates method stats across window', () => {
    const stats = computeMethodStats(sampleMethods);
    assert.equal(stats.totalCalls, 170);
    assert.equal(stats.totalErrors, 5);
    assert.ok(Math.abs(stats.errorRatePct - (5 / 170) * 100) < 1e-9);
    assert.equal(stats.worstP95, 0.9);
    assert.equal(stats.totalBytes, 8500);

    assert.deepEqual(computeMethodStats([]), {
      totalCalls: 0,
      totalErrors: 0,
      errorRatePct: 0,
      worstP95: 0,
      totalBytes: 0,
    });
  });

  it('assigns error rate badge thresholds (0 green, <=5 yellow, >5 red)', () => {
    assert.equal(errorRateBadgeClass(0), 'bg-green-lt');
    assert.equal(errorRateBadgeClass(0.01), 'bg-yellow-lt');
    assert.equal(errorRateBadgeClass(5), 'bg-yellow-lt');
    assert.equal(errorRateBadgeClass(5.01), 'bg-red-lt');
  });

  it('filters by substring and sorts by any valid column without mutating input', () => {
    const byCountDesc = filterAndSortMethods(sampleMethods);
    assert.deepEqual(byCountDesc.map((m) => m.method), ['fast', 'medium_job', 'flaky_job']);

    const byErrorDesc = filterAndSortMethods(sampleMethods, { sortCol: 'error_rate', sortDir: 'desc' });
    assert.deepEqual(byErrorDesc.map((m) => m.method), ['flaky_job', 'medium_job', 'fast']);

    const byNameAsc = filterAndSortMethods(sampleMethods, { sortCol: 'method', sortDir: 'asc' });
    assert.deepEqual(byNameAsc.map((m) => m.method), ['fast', 'flaky_job', 'medium_job']);

    const filtered = filterAndSortMethods(sampleMethods, { query: 'JOB', sortCol: 'p95_duration', sortDir: 'asc' });
    assert.deepEqual(filtered.map((m) => m.method), ['medium_job', 'flaky_job']);
  });

  it('computes whisker geometry scaled to globalMaxDuration', () => {
    const geom = computeWhiskerGeometry(
      { min_duration: 0.2, avg_duration: 0.5, p95_duration: 0.9, max_duration: 1.0 },
      2.0,
    );
    assert.deepEqual(geom, { minPct: 10, rangeWidthPct: 40, avgPct: 25, p95Pct: 45 });

    assert.deepEqual(computeWhiskerGeometry({ min_duration: 0.1 }, 0), {
      minPct: 0,
      rangeWidthPct: 0,
      avgPct: 0,
      p95Pct: 0,
    });
  });
});

describe('pages/requests.js', () => {
  it('normalizes status query parameters', () => {
    assert.equal(normalizeStatusParam('DONE'), 'DONE');
    assert.equal(normalizeStatusParam('error'), 'ERROR');
    assert.equal(normalizeStatusParam('ACTIVE'), '');
    assert.equal(normalizeStatusParam('all'), '');
    assert.equal(normalizeStatusParam(null), '');
  });

  it('filters active calls by exact method and worker match', () => {
    const active = [
      { request_id: 'r1', raw_method: 'compute', method: 'compute(x=1)', worker_id: 'w-1' },
      { request_id: 'r2', method: 'compute_fast()', worker_id: 'w-1' },
      { request_id: 'r3', raw_method: 'compute', method: 'compute()', worker_id: 'w-2' },
    ];

    assert.deepEqual(filterActiveCalls(active, {}).map((c) => c.request_id), ['r1', 'r2', 'r3']);
    assert.deepEqual(filterActiveCalls(active, { method: 'compute' }).map((c) => c.request_id), ['r1', 'r3']);
    assert.deepEqual(filterActiveCalls(active, { worker: 'w-1' }).map((c) => c.request_id), ['r1', 'r2']);
    assert.deepEqual(
      filterActiveCalls(active, { method: 'compute', worker: 'w-2' }).map((c) => c.request_id),
      ['r3'],
    );
    assert.deepEqual(filterActiveCalls(active, { method: 'Compute' }), []);
  });

  it('excludes flushed ACTIVE rows in All view and enforces status filter when set', () => {
    const rows = [
      { request_id: 'a1', status: 'ACTIVE' },
      { request_id: 'd1', status: 'DONE' },
      { request_id: 'e1', status: 'ERROR' },
    ];
    assert.deepEqual(filterHistoricalCalls(rows, '').map((r) => r.request_id), ['d1', 'e1']);
    assert.deepEqual(filterHistoricalCalls(rows, 'DONE').map((r) => r.request_id), ['d1']);
    assert.deepEqual(filterHistoricalCalls(rows, 'ERROR').map((r) => r.request_id), ['e1']);
  });

  it('collects deduplicated sorted known methods and workers from all sources', () => {
    const res = collectKnownMethodsAndWorkers({
      data: {
        methods: ['ping_pong', 'echo'],
        workers: ['w-2'],
        worker_methods: { 'w-1': ['echo', 'stream'] },
        worker_metrics: { 'w-3': {} },
        active_calls: [{ raw_method: 'slow_op', worker_id: 'w-2' }],
      },
      calls: {
        calls: [{ method: 'archive(id=1)', worker_id: 'w-old' }],
      },
      extraMethods: ['zeta'],
      extraWorkers: ['w-0'],
    });
    assert.deepEqual(res.methods, ['archive', 'echo', 'ping_pong', 'slow_op', 'stream', 'zeta']);
    assert.deepEqual(res.workers, ['w-0', 'w-1', 'w-2', 'w-3', 'w-old']);
  });

  it('computes pagination bounds and button states accurately', () => {
    assert.equal(REQUESTS_PAGE_SIZE, 15);

    assert.deepEqual(
      computePaginationRange({ total: 0, offset: 0, limit: 15, displayedCount: 0 }),
      { start: 0, end: 0, total: 0, hasPrev: false, hasNext: false, prevOffset: 0, nextOffset: 15 },
    );

    assert.deepEqual(
      computePaginationRange({ total: 60, offset: 0, limit: 15, displayedCount: 15 }),
      { start: 1, end: 15, total: 60, hasPrev: false, hasNext: true, prevOffset: 0, nextOffset: 15 },
    );

    assert.deepEqual(
      computePaginationRange({ total: 60, offset: 15, limit: 15, displayedCount: 15 }),
      { start: 16, end: 30, total: 60, hasPrev: true, hasNext: true, prevOffset: 0, nextOffset: 30 },
    );

    assert.deepEqual(
      computePaginationRange({ total: 60, offset: 50, limit: 15, displayedCount: 10 }),
      { start: 51, end: 60, total: 60, hasPrev: true, hasNext: false, prevOffset: 35, nextOffset: 65 },
    );

    assert.deepEqual(
      computePaginationRange({ total: 25, offset: 0, limit: 15, displayedCount: 15 }),
      { start: 1, end: 15, total: 25, hasPrev: false, hasNext: true, prevOffset: 0, nextOffset: 15 },
    );
  });
});
