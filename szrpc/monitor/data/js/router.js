/**
 * History API router and query-string URL state helpers.
 * Pure URL/route helpers are DOM-independent and cleanly importable under `node --test`.
 */

export const VALID_ROUTES = Object.freeze(['/', '/workers', '/methods', '/requests']);
export const VALID_WINDOWS = Object.freeze(['15m', '1h', '12h', '1d', '7d']);
export const DEFAULT_WINDOW = '15m';
export const WINDOW_STORAGE_KEY = 'szrpc-monitor-window';

/**
 * Normalize a pathname to one of VALID_ROUTES (defaults to '/').
 */
export function normalizeRoute(pathname) {
  if (typeof pathname !== 'string' || !pathname) {
    return '/';
  }
  const clean = pathname.length > 1 ? pathname.replace(/\/+$/, '') : pathname;
  return VALID_ROUTES.includes(clean) ? clean : '/';
}

/**
 * Parse a query string (with or without leading '?') or URLSearchParams into a plain object.
 * Empty values are omitted.
 */
export function parseQuery(search = '') {
  const params = search instanceof URLSearchParams
    ? search
    : new URLSearchParams(typeof search === 'string' ? search : '');
  const result = {};
  for (const [key, value] of params.entries()) {
    if (value !== '') {
      result[key] = value;
    }
  }
  return result;
}

function readStoredWindow(storage) {
  const store = storage !== undefined
    ? storage
    : (typeof globalThis !== 'undefined' ? globalThis.localStorage : null);
  if (!store || typeof store.getItem !== 'function') {
    return null;
  }
  try {
    return store.getItem(WINDOW_STORAGE_KEY);
  } catch {
    return null;
  }
}

function writeStoredWindow(windowKey, storage) {
  const store = storage !== undefined
    ? storage
    : (typeof globalThis !== 'undefined' ? globalThis.localStorage : null);
  if (!store || typeof store.setItem !== 'function') {
    return;
  }
  try {
    store.setItem(WINDOW_STORAGE_KEY, windowKey);
  } catch {
    // Ignore storage quota / privacy mode failures
  }
}

/**
 * Resolve the active time window from query string/object, localStorage fallback, or default '15m'.
 */
export function resolveWindow(searchOrQuery = '', storage = undefined) {
  const query = typeof searchOrQuery === 'string' || searchOrQuery instanceof URLSearchParams
    ? parseQuery(searchOrQuery)
    : (searchOrQuery || {});

  if (query.window && VALID_WINDOWS.includes(query.window)) {
    return query.window;
  }
  const stored = readStoredWindow(storage);
  if (stored && VALID_WINDOWS.includes(stored)) {
    return stored;
  }
  return DEFAULT_WINDOW;
}

/**
 * Build a normalized route URL with non-empty query parameters.
 */
export function buildUrl(route, params = {}) {
  const path = normalizeRoute(route);
  const searchParams = new URLSearchParams();
  if (params && typeof params === 'object') {
    for (const [key, value] of Object.entries(params)) {
      if (value !== null && value !== undefined && value !== '') {
        searchParams.set(key, String(value));
      }
    }
  }
  const qs = searchParams.toString();
  return qs ? `${path}?${qs}` : path;
}

/**
 * Create a History API router synchronized with `window.location` and `popstate`.
 */
export function createRouter({ onChange = () => {}, storage = undefined, win = undefined } = {}) {
  const targetWin = win || (typeof globalThis !== 'undefined' ? globalThis.window : null);

  function readLocationState() {
    const pathname = targetWin?.location?.pathname || '/';
    const search = targetWin?.location?.search || '';
    const query = parseQuery(search);
    const route = normalizeRoute(pathname);
    const windowKey = resolveWindow(query, storage);
    query.window = windowKey;
    return { route, window: windowKey, query };
  }

  let current = readLocationState();
  let popstateHandler = null;

  function snapshot(st = current) {
    return { route: st.route, window: st.window, query: { ...st.query } };
  }

  function applyState(nextState, { replace = false, notify = true } = {}) {
    const prev = snapshot(current);
    const url = buildUrl(nextState.route, nextState.query);
    const currentUrl = targetWin?.location
      ? `${targetWin.location.pathname}${targetWin.location.search}`
      : null;

    writeStoredWindow(nextState.window, storage);
    current = {
      route: nextState.route,
      window: nextState.window,
      query: { ...nextState.query },
    };

    if (targetWin?.history && url !== currentUrl) {
      if (replace) {
        targetWin.history.replaceState(null, '', url);
      } else {
        targetWin.history.pushState(null, '', url);
      }
    }

    if (notify) {
      onChange(snapshot(current), prev);
    }
  }

  function navigate(route, params = {}, { replace = false, preserveWindow = true } = {}) {
    const normalizedRoute = normalizeRoute(route);
    const cleanParams = {};
    if (params && typeof params === 'object') {
      for (const [key, value] of Object.entries(params)) {
        if (value !== null && value !== undefined && value !== '') {
          cleanParams[key] = String(value);
        }
      }
    }

    const nextWindow = cleanParams.window && VALID_WINDOWS.includes(cleanParams.window)
      ? cleanParams.window
      : (preserveWindow ? current.window : DEFAULT_WINDOW);

    const { window: _ignoredWindow, ...restParams } = cleanParams;
    const query = { window: nextWindow, ...restParams };
    applyState({ route: normalizedRoute, window: nextWindow, query }, { replace });
  }

  function replaceQuery(patch = {}, { push = false } = {}) {
    const nextQuery = { ...current.query };
    if (patch && typeof patch === 'object') {
      for (const [key, value] of Object.entries(patch)) {
        if (value === null || value === undefined || value === '') {
          delete nextQuery[key];
        } else {
          nextQuery[key] = String(value);
        }
      }
    }

    const nextWindow = nextQuery.window && VALID_WINDOWS.includes(nextQuery.window)
      ? nextQuery.window
      : current.window;
    nextQuery.window = nextWindow;

    applyState(
      { route: current.route, window: nextWindow, query: nextQuery },
      { replace: !push },
    );
  }

  function setWindow(windowKey) {
    if (!VALID_WINDOWS.includes(windowKey)) {
      return;
    }
    replaceQuery({ window: windowKey, offset: null }, { push: false });
  }

  function start() {
    current = readLocationState();
    writeStoredWindow(current.window, storage);

    const normalizedUrl = buildUrl(current.route, current.query);
    const rawUrl = targetWin?.location
      ? `${targetWin.location.pathname}${targetWin.location.search}`
      : null;
    if (targetWin?.history && rawUrl !== normalizedUrl) {
      targetWin.history.replaceState(null, '', normalizedUrl);
    }

    if (targetWin && typeof targetWin.addEventListener === 'function') {
      popstateHandler = () => {
        const prev = snapshot(current);
        current = readLocationState();
        writeStoredWindow(current.window, storage);
        onChange(snapshot(current), prev);
      };
      targetWin.addEventListener('popstate', popstateHandler);
    }

    onChange(snapshot(current), null);
  }

  function stop() {
    if (targetWin && popstateHandler && typeof targetWin.removeEventListener === 'function') {
      targetWin.removeEventListener('popstate', popstateHandler);
      popstateHandler = null;
    }
  }

  return {
    getState: () => snapshot(current),
    navigate,
    replaceQuery,
    setWindow,
    start,
    stop,
  };
}
