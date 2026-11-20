# Monitor Dashboard v2 Specification

**Status:** Finalized via [Map: Monitor Dashboard v2 (Multi-page Tabler Redesign)](https://github.com/michel4j/swift-rpc/issues/8)  
**Target branch:** `feat/monitor-dashboard-v2`  
**Primary research note:** [`docs/research/tabler-folded-hover-audit.md`](../research/tabler-folded-hover-audit.md) (`91df0d8`)  
**Static scaffold commit:** `36d1954` ([Ticket #10](https://github.com/michel4j/swift-rpc/issues/10))

---

## 1. Goals, Vocabulary & Non-Goals

### Goals
Redesign the `szrpc` Monitor web dashboard (`szrpc/monitor/`) from a single-page CDN-hosted Bootstrap page into a **self-hosted, multi-page Tabler dashboard** using the `layout-folded-hover` vertical sidebar design, zero external runtime CDNs, zero new Python dependencies, `<1 MiB` total static wheel assets, and full Light/Dark theme support over existing `/api/*` endpoints.

### Domain Vocabulary (`GLOSSARY.md`)
All UI headings, table columns, route names, and labels MUST use canonical `GLOSSARY.md` terms (replacing legacy v1 `Task`/`Job` copy):

| Canonical Term | Avoid | UI Usage |
| --- | --- | --- |
| **Monitor** | Dashboard singleton | Sidebar section header, footer, service introspection |
| **Broker** | Proxy, coordinator | Endpoints card (`frontend`, `backend`, `publish`, `subscribe`) |
| **Service** | Handler, controller | Service description & version in sticky top bar |
| **Worker** | Runner, node | `/workers` page, Worker cards, Worker filter |
| **Request** | Task, job, command | `/requests` page, `Request ID`, `Active Requests`, `Request Details` drawer |
| **Method** | Task Type | `/methods` page, Method column, Method tags & filters |

### Out of Scope
- New Monitor/Broker runtime capabilities (SSE/WebSocket push, Request cancellation from UI, Broadcast Events inspector, authentication/RBAC).
- Breaking changes to existing `/api/*` JSON contracts or `SqliteMonitorStore` schema migrations.
- Third-party charting libraries (specifically rejecting Tabler's `apexcharts` — 876 KiB raw and non-MIT Community License).

---

## 2. Static Assets, Wheel Packaging & FastAPI Routing

Already landed in `36d1954` ([Ticket #9](https://github.com/michel4j/swift-rpc/issues/9), [Ticket #10](https://github.com/michel4j/swift-rpc/issues/10)):

### Vendored Assets (`szrpc/monitor/data/vendor/`)
3 MIT-licensed Tabler v1.5.0 files (0 external `url(...)` references, 0 `@import`, 0 `@font-face`, system font stacks, inline SVG icons only):
- `vendor/tabler.min.css` — 691,465 B raw (79,499 B gzip)
- `vendor/tabler.min.js` — 85,439 B raw (25,194 B gzip)
- `vendor/tabler-theme.min.js` — 1,340 B raw (687 B gzip)

**Packaging constraint:** Never place vendored assets under a `dist/` subdirectory inside `szrpc/monitor/data/` — `.gitignore:16` has an unanchored `dist/` rule honored by Hatchling (`ignore_vcs = False`). Keeping assets under `vendor/`, `js/`, `css/` packages automatically under `pyproject.toml` (`packages = ["szrpc"]`). Total `szrpc/monitor/data/` raw footprint MUST remain `< 1 MiB` (1,048,576 B).

### FastAPI Routes (`szrpc/monitor/__init__.py`)
- `StaticFiles(directory=DATA_DIR)` mounted at `/static` (serves `/static/vendor/*`, `/static/js/*`, `/static/css/*`, `/static/sparklines.js`; missing `.map` files return 404).
- Explicit History API SPA shell routes: `SPA_ROUTES = ("/", "/workers", "/methods", "/requests")`, each returning `FileResponse(DATA_DIR / "monitor.html")`. Unknown `/api/*` paths return genuine 404s.
- Existing JSON APIs unchanged: `/api/data`, `/api/details/{request_id}`, `/api/summary`, `/api/metrics/workers`, `/api/metrics/tasks`, `/api/calls`.
- **Legacy cleanup ([Ticket #15](https://github.com/michel4j/swift-rpc/issues/15)):** Remove the top-level `/sparklines.js` FastAPI route once v2 `monitor.html` migrates to `/static/sparklines.js` (performed in build ticket #22).

---

## 3. Shell Layout & Global Chrome

Resolved in [Ticket #11](https://github.com/michel4j/swift-rpc/issues/11):

### Tabler `layout-folded-hover` Shell
- `<html lang="en" data-bs-navbar-position="vertical" data-bs-sidebar="folded-hover">`
- Load `/static/vendor/tabler-theme.min.js` **synchronously** immediately after `<body>` open (pre-paint theme init + OS `prefers-color-scheme` sync). Load `/static/vendor/tabler.min.js` with `defer`.
- `<aside class="navbar navbar-vertical navbar-expand-lg">`:
  - Mobile `<button class="navbar-toggler" data-bs-toggle="collapse" data-bs-target="#sidebar-menu">` (auto-closes collapse menu on nav link click).
  - Brand: Swift RPC inline SVG icon (4rem folded rail shows icon; hover/pinned shows icon + `Swift RPC` title).
  - 4 primary nav links with inline Tabler SVG icons and `aria-current="page"` on active route:
    - **Overview** (`/`)
    - **Workers** (`/workers`) — live active Workers count badge (hidden while folded to 4rem rail, visible when expanded/pinned)
    - **Methods** (`/methods`)
    - **Requests** (`/requests`) — live active Requests count badge when `> 0` (hidden while folded)
  - `.navbar-footer`: built-in pin/unpin toggle (`data-bs-toggle="sidebar-folded"`, persisted in `localStorage['tabler-sidebar']`).

### Sticky Top Utility Bar & Footer (Inside `.page-wrapper`)
> **CSS caveat (`tabler.css:15337`):** Direct children `.page > header.navbar.navbar-expand-*` are hidden when `data-bs-navbar-position="vertical"` is set. The sticky top utility bar MUST live **inside `.page-wrapper`** (above `<main id="content">`).

- **Left:** Service `description` + `version` badge + Broker endpoints dropdown popover button (`frontend`/`backend`/`publish`/`subscribe`).
- **Right:**
  - Time-window button group: `15m` / `1h` / `12h` / `1d` / `7d` (exposing all 5 backend `WINDOWS`).
  - Auto-refresh switch + compact **Refresh now** icon button (works when Auto-refresh is paused or on demand).
  - **2-state Light/Dark `<button>` toggle** (`.hide-theme-dark` / `.hide-theme-light` icons; starts following OS `"auto"`, click flips resolved `data-bs-theme` and persists `localStorage['tabler-theme']` without page reload).
- **Stale/connection-lost banner:** Inside `.page-wrapper` above `<main>`, `role="alert"`, hidden by default; shown on `/api/data` failure with `Last updated HH:MM:SS` and `Retry` button, keeping last good data visible and auto-clearing on recovery.
- **Footer:** Tabler `.footer.footer-transparent` preserving external links (`Docs`, `Source`, `Releases`, `Issues`) + updated timestamp.

---

## 4. Client Architecture, Routing & Polling Lifecycle

Resolved in [Ticket #11](https://github.com/michel4j/swift-rpc/issues/11):

### Zero-Build ES Module Layout (`szrpc/monitor/data/`)
```text
szrpc/monitor/data/
├── monitor.html              # Thin Tabler folded-hover shell + Offcanvas drawer markup
├── sparklines.js             # Tabler-themed pure inline SVG sparkline renderer
├── css/
│   └── monitor.css           # Sticky topbar, folded-rail rules, sparklines, whiskers
├── js/
│   ├── app.js                # Entrypoint: shared state, poller, shell chrome, view lifecycle
│   ├── router.js             # History API router & query-param URL state helpers
│   ├── format.js             # Duration/byte/HTML escaping/status badge/clipboard helpers
│   ├── drawer.js             # Shared 4-tab Right Offcanvas Request Details controller
│   └── pages/
│       ├── overview.js       # Overview A view controller (mount/update/unmount)
│       ├── workers.js        # Workers C view controller (mount/update/unmount)
│       ├── methods.js        # Methods C view controller + pure sort/filter helpers
│       └── requests.js       # Requests C view controller (mount/update/unmount)
└── vendor/
    ├── tabler.min.css
    ├── tabler.min.js
    └── tabler-theme.min.js
```

### Routing & URL State
- Base routes: `/`, `/workers`, `/methods`, `/requests`. URL query string is the single source of truth for page state:
  - `?window=15m|1h|12h|1d|7d` — carried across sidebar navigation, persisted in `localStorage` (default `15m`).
  - Page filters & deep links: `/workers?status=all|active|offline`, `/requests?status=DONE|ERROR&method=<m>&worker=<w>`, `?request=<id>` (opens Request Drawer on Overview or Requests). Fresh sidebar clicks clear page-specific filters while retaining `?window=`.
- **History semantics:**
  - `history.pushState`: sidebar navigation, cross-page drilldowns, opening Request Details (`?request=<id>`).
  - `history.replaceState`: changing `window`, typing/changing filters, paginating, closing Request Details drawer.
- **View lifecycle:** `app.js` mounts exactly one active page controller (`mount`, `update`, `unmount`) into `<main id="content">`.

### Polling & Safeguards
- **Global live heartbeat:** `GET /api/data` every **1s** (KPIs, active Requests, active Workers, sidebar badges).
- **Active-page windowed fetches only** (immediate on route mount or `window` switch, then on interval):
  - **Overview (`/`):** `/api/summary?window=<w>` + `/api/calls?window=<w>&status=ERROR&limit=5` every **5s**
  - **Workers (`/workers`):** `/api/metrics/workers?window=<w>` + `/api/metrics/tasks?window=<w>` every **5s**
  - **Methods (`/methods`):** `/api/summary?window=<w>` every **5s**
  - **Requests (`/requests`):** `/api/calls?window=<w>&limit=25&offset=<n>&...` every **2s**
- **Safeguards:** Pause all polling when `document.hidden`, resume immediately on `visibilitychange`; cancel in-flight requests on route/window switch via `AbortController`.

---

## 5. Charting & Visual Strategy

Resolved in [Ticket #12](https://github.com/michel4j/swift-rpc/issues/12):
- **Pure inline SVG only** (`sparklines.js` + lightweight CSS/SVG helpers). `apexcharts` rejected due to size (876 KiB raw) and incompatible non-MIT Community License.
- Migrate `sparklines.js` palette & status colors from Bootstrap `--bs-*` / hardcoded hex to Tabler CSS custom properties (`--tblr-primary`, `--tblr-success`, `--tblr-danger`, `--tblr-warning`, `--tblr-purple`, `--tblr-teal`, `--tblr-cyan`, `--tblr-secondary`) for automatic Light/Dark mode adaptation.
- Retain lightweight native SVG `<title>` hover tooltips and inline numeric readouts in sparkline headers.

---

## 6. Per-Page Specifications

Validated via live 3-variant prototype ([Ticket #13](https://github.com/michel4j/swift-rpc/issues/13)) and locked in [Ticket #14](https://github.com/michel4j/swift-rpc/issues/14): **Overview A + Workers C + Methods C + Requests C**.

### 6.1 Overview (`/` — Overview A)
- **Top row — 4 KPI stat cards:** `Uptime`, `Requests` (lifetime total + selected-window subtotal), `Failures` (lifetime count + error-rate progress bar & `%`), `Active Workers` (count + link to `/workers`).
- **Left column:**
  - **Broker Endpoints card:** Tabler datagrid of non-empty endpoints (`frontend`, `backend`, `publish`, `subscribe`) with one-click copy icon per URL.
  - **Top Methods card:** Top **5** methods by `count desc` (`Method`, `Calls`, `Errors`, `p95`); clicking a method navigates to `/requests?method=<m>`; header action links to `/methods`.
- **Right column:**
  - **Active Requests card:** Live 1s list from `/api/data` with ticking elapsed durations; clicking opens Request Details Drawer (`?request=<id>`); compact empty state (`No active Requests`) when 0.
  - **Recent Failures card:** 5s `/api/calls?window=<w>&status=ERROR&limit=5` (survives restarts, honors window); clicking opens Request Details Drawer; header action links to `/requests?status=ERROR`.

### 6.2 Workers (`/workers` — Workers C)
- **Header filter pills:** `All` (default) / `Active` / `Offline` with live counts, synced via `?status=all|active|offline` (`replaceState`).
- **Card grid:** Responsive 3-column grid (`grid-template-columns: repeat(auto-fill, minmax(300px, 1fr))`) rendering Tabler-styled `sparklines.js` cards (CPU, Mem, Net Rx/Tx, Active Requests, Completed duration column/needle bars, advertised Methods).
- **Ordering & status accents:** Active workers sorted alphabetically first with `card-status-top` success (`--tblr-success`) accent; Offline workers seen in the selected window sorted alphabetically below with muted secondary (`--tblr-secondary`) accent.
- **Method drilldown:** Clicking any advertised method badge navigates to `/requests?method=<m>&worker=<w>` (`pushState`).
- **Offscreen performance:** Preserve `content-visibility: auto` and `contentvisibilityautostatechange` render skip guard on the Workers grid.

### 6.3 Methods (`/methods` — Methods C)
- **Summary strip:** 4 compact KPI cards (`Window Calls`, `Error Rate`, `Worst p95`, `Total Payload`).
- **Card header:** Title, compact whisker legend (`min–max`, `avg`, `p95`), and client-side substring filter input (`Filter method...`, zero extra API calls).
- **Table (`.table-responsive`):**
  - Default sort: `Calls desc`. Sortable columns (`Method`, `Calls`, `Error %`, `Avg`, `p95`, `Payload`) using `<button type="button">` inside `<th aria-sort="ascending|descending|none">`.
  - `Error %` badge thresholds: `0%` green (`bg-green-lt`), `>0%–5%` yellow (`bg-yellow-lt`), `>5%` red (`bg-red-lt`).
  - Inline latency distribution whisker scaled `0..max(max_duration)` across visible methods (`[min..max]` band + primary `avg` tick + warning `p95` tick) with full `min / avg / p95 / max` tooltip.
  - Clicking a method navigates to `/requests?method=<m>` (`pushState`).

### 6.4 Requests (`/requests` — Requests C)
- **Top card — Live Active Requests (`card-status-top bg-primary`):** Fed by 1s `/api/data` with ticking elapsed times; respects Method/Worker filters; hidden when 0 matching active Requests.
- **Bottom card — Historical Requests:** Fed by 2s `/api/calls` with page size **25**, `Prev`/`Next` pagination, and `Showing X-Y of Z (<window>)` counter.
  - Status filter: `All` / `DONE` / `ERROR` (client filters out flushed `ACTIVE` rows in `All` view so live Requests are not duplicated between cards).
  - Exact-match Method & Worker filters: Because `store.py:486-489` matches `method` and `worker_id` by exact equality, use `<input list="...">` HTML datalists populated from `service_info.methods` and known `workers`, triggering on `change`/Enter or 250ms debounce (`replaceState`), plus a `Clear` button whenever filtered.

### 6.5 Shared Tabbed Right Offcanvas Drawer (`#request-details-drawer`)
- Shared across Overview and Requests; `width: min(560px, 100vw)`.
- Opens on any Request ID click (`pushState` `?request=<id>`); clears `request` param via `replaceState` on close; auto-opens on initial load if `?request=<id>` is present.
- Fetches `/api/details/{request_id}` and renders 4 tabs:
  - **Summary:** Key-value metadata list (`Status`, `Method`, `Worker`, `Client`, `Timestamp`, `Duration`, `Sent Bytes`, `Updates`), clickable `Method` (`-> /requests?method=<m>`) and `Worker` (`-> /requests?worker=<w>`) links auto-closing the drawer, plus a compact red error alert callout at top when `status === 'ERROR'` (extracting structured error `type`/`message` or string preview).
  - **Args:** Pretty-printed `kwargs` JSON + Copy JSON button.
  - **Results:** Pretty-printed `results` JSON + Copy JSON button.
  - **Raw JSON:** Full payload JSON + Copy JSON button.
- Header includes Request ID + Copy ID button. All copy actions announce via `aria-live="polite"`.

---

## 7. Accessibility, Responsive Breakpoints & Testing

Resolved in [Ticket #15](https://github.com/michel4j/swift-rpc/issues/15):

### Accessibility
- Skip link (`Skip to main content` -> `#content`), semantic `<aside>`, `<nav aria-label="Monitor navigation">`, `<main id="content">`, `<footer>`, and `aria-current="page"` on active nav link.
- Folded-hover sidebar expands on keyboard focus via Tabler's native `:has(:focus-visible)`; Offcanvas handles `Escape`, focus trap, and returns focus to triggering Request link on close.
- Sortable Methods headers use `<button>` inside `<th aria-sort="...">`; `role="alert"` on connection banner; `aria-live="polite"` on copy feedback; respect `prefers-reduced-motion: reduce` and keep SVG sparklines non-animated.

### Responsive Breakpoints
- `>=992px` (`lg`): fixed 4rem folded-hover rail expanding to 16rem overlay on hover/focus or pin.
- `<992px`: Tabler `navbar-toggler` collapse (auto-closes on nav click); sticky topbar wraps cleanly into 2 compact rows without horizontal overflow.
- Standard `.table-responsive` horizontal scroll on narrow screens (drop v1's `@container` stacked-block table CSS); Workers grid `minmax(300px, 1fr)`; Offcanvas `min(560px, 100vw)`.

### Test Strategy (Zero New Python Dependencies)
- **Python (`uv run pytest`, no `httpx`):**
  - Static assets verified via `StaticFiles.get_response` (`/static/vendor/*`, `/static/js/*`, `/static/css/monitor.css`, `/static/sparklines.js`; 404 on `.map`, unknown `/api/*`, and removed `/sparklines.js`).
  - All 4 SPA routes return `monitor.html`; smoke assertions verify `data-bs-sidebar="folded-hover"`, `/static/js/app.js`, and zero external CDN URLs.
  - Raw size of `szrpc/monitor/data/` asserted `< 1 MiB` (verified against `uv build --wheel`).
- **Frontend JS (`node --test`):**
  - Zero-dependency unit tests in `tests/js/monitor.test.mjs` covering `format.js`, `router.js`, and Methods sort/filter/whisker helpers; invoked via pytest subprocess when `node` is on `PATH` and cleanly skipped (`pytest.skip`) in Python-only environments.

---

## 8. Implementation Plan (`ready-for-agent` Tickets)

All build tickets carry `ready-for-agent` + `enhancement` labels, native GitHub blocking edges, and target `feat/monitor-dashboard-v2`:

| Order | Ticket | Blocked By |
| :---: | --- | --- |
| 1 | [Monitor v2 [1/6]: Tabler folded-hover shell, ES module router, poller, and shared helpers](https://github.com/michel4j/swift-rpc/issues/17) | _None (ready now)_ |
| 2 | [Monitor v2 [2/6]: Shared Tabbed Right Offcanvas Request Details drawer](https://github.com/michel4j/swift-rpc/issues/18) | #17 |
| 3 | [Monitor v2 [3/6]: Overview page (Overview A)](https://github.com/michel4j/swift-rpc/issues/19) | #17, #18 |
| 4 | [Monitor v2 [4/6]: Workers page (Workers C) and Tabler-themed sparklines.js](https://github.com/michel4j/swift-rpc/issues/20) | #17 |
| 5 | [Monitor v2 [5/6]: Methods page (Methods C) with latency whiskers and sorting](https://github.com/michel4j/swift-rpc/issues/21) | #17 |
| 6 | [Monitor v2 [6/6]: Requests page (Requests C), drop legacy /sparklines.js, and finalize v2 tests](https://github.com/michel4j/swift-rpc/issues/22) | #18, #19, #20, #21 |
