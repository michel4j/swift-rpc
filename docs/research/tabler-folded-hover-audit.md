# Tabler `layout-folded-hover` Asset, Theme, Routing & Wheel Packaging Audit

Research note for [Issue #9](https://github.com/michel4j/swift-rpc/issues/9) (part of [Map #8](https://github.com/michel4j/swift-rpc/issues/8)). Every fact below is verified against primary sources in this repository (`tmp/tabler-dashboard/`, `pyproject.toml`, `.gitignore`, `szrpc/monitor/__init__.py`, `tests/test_monitor.py`, and installed `hatchling` 1.33.0 / `fastapi` 0.163.2).

## Executive Summary

- **Exact minimal vendored set (3 files, Tabler v1.5.0, MIT-licensed):**
  1. `tabler.min.css` — 691,465 B raw (79,499 B gzip-9)
  2. `tabler.min.js` — 85,439 B raw (25,194 B gzip-9)
  3. `tabler-theme.min.js` — 1,340 B raw (687 B gzip-9)
- **Footprint vs. `<1 MB` budget:** **778,244 B (760.0 KiB / 0.742 MiB)** uncompressed (**105,380 B / 102.9 KiB** gzip-9). Combined with the existing `monitor.html` + `sparklines.js` (47,163 B raw), total static assets under `szrpc/monitor/data/` are **825,407 B (806.1 KiB / 0.787 MiB)** — comfortably under the `<1 MB` uncompressed budget.
- **Zero external runtime dependencies:** `tabler.min.css` has **0** external `url(...)` references (all 28 `url(...)` calls are inline `data:image/svg+xml` URIs), **0** `@import` rules, and **0** `@font-face` rules (uses system sans-serif and monospace stacks). All icons are inline SVGs.
- **Wheel packaging (`hatchling`):** `pyproject.toml:45-46` (`packages = ["szrpc"]`) recursively packages nested static subdirectories under `szrpc/monitor/data/` automatically with **no `pyproject.toml` changes** — **provided** vendored paths do **not** use a `dist/` segment, because `.gitignore:16` has an unanchored `dist/` rule that Hatchling honors by default (`ignore_vcs = False`), silently excluding `szrpc/monitor/data/dist/**` from wheels and sdists. Use `szrpc/monitor/data/vendor/` instead.
- **Fact for Ticket #12 (ApexCharts):** `tmp/tabler-dashboard/dist/libs/apexcharts/dist/apexcharts.min.js` is **897,003 B (876.0 KiB)** raw (**252,913 B** gzip-9) — nearly 2x the ~500 KB assumption in #12, pushing raw static assets to **1,675,247 B (1.60 MiB)** — and is **not MIT-licensed** (`LICENSE` is a `<$2M/yr` revenue-capped Community License prohibiting redistribution in toolkits/SDKs/platforms, incompatible with `szrpc`'s MIT license).

---

## 1. Exact File List to Vendor for Ticket #10

Copy these **3 files** verbatim (do **not** copy `.map` source maps or use a `dist/` subdirectory):

| Source (`tmp/tabler-dashboard/...`) | Target (`szrpc/monitor/data/...`) | Raw bytes | Raw KiB | Gzip-9 bytes | License | Purpose |
| --- | --- | ---: | ---: | ---: | --- | --- |
| `dist/css/tabler.min.css` | `vendor/tabler.min.css` | 691,465 | 675.3 | 79,499 | MIT | Full Tabler v1.5.0 + Bootstrap 5 styles, dark mode (`[data-bs-theme=dark]`), `.hide-theme-*`, folded-hover sidebar CSS, mobile navbar CSS |
| `dist/js/tabler.min.js` | `vendor/tabler.min.js` | 85,439 | 83.4 | 25,194 | MIT | Bootstrap 5 JS components + Popper.js v2.11.8 + Tabler `sidebar.ts` folded-hover/pin handlers + tooltip/popover/tab/toast init |
| `dist/js/tabler-theme.min.js` | `vendor/tabler-theme.min.js` | 1,340 | 1.3 | 687 | MIT | Synchronous pre-paint theme/layout attribute initializer (`localStorage`, URL params, `prefers-color-scheme`) |
| **Core Tabler subtotal** | | **778,244** | **760.0** | **105,380** | **MIT** | **Under `<1 MB` budget (74.2% of 1 MiB)** |

### Existing `szrpc/monitor/data/` Files (Baseline Reference)

| File | Raw bytes | Raw KiB | Gzip-9 bytes |
| --- | ---: | ---: | ---: |
| `szrpc/monitor/data/monitor.html` | 31,595 | 30.9 | 7,033 |
| `szrpc/monitor/data/sparklines.js` | 15,568 | 15.2 | 3,983 |
| **Core Tabler + Existing total** | **825,407** | **806.1** | **116,396** |

### Excluded Tabler Assets (Why Each Is Not Needed)

| File in `tmp/tabler-dashboard/` | Raw bytes | Gzip-9 bytes | Reason excluded |
| --- | ---: | ---: | --- |
| `dist/css/tabler-themes.min.css` | 4,965 | 1,185 | Only adds optional color/radius/font switcher presets (`[data-bs-theme-base=...]`, `[data-bs-theme-primary=...]`, `[data-bs-theme-radius=...]`, `[data-bs-theme-font=...]`, `tabler-themes.css:20-244`). Standard light/dark mode (`[data-bs-theme=dark]`) is already inside `tabler.min.css`. |
| `dist/css/tabler-vendors.min.css` | 12,330 | 2,831 | Styles third-party plugins only: noUiSlider, Litepicker, Tom Select, ApexCharts (`L207-283`), jsVectorMap, Dropzone, fslightbox, Plyr, HugeRTE, Star Rating, Coloris, Typed, FullCalendar (`tabler-vendors.css:20-553`). Only needed if ApexCharts or another listed plugin is adopted. |
| `dist/css/tabler-props.min.css` | 22,494 | 4,330 | CSS `@property` custom-property declarations not linked by `layout-folded-hover.html`. |
| `dist/css/tabler-{flags,socials,payments,marketing}.min.css` | 114,786 | — | Country flags, social buttons, payment badges, marketing landing styles; unused by Monitor dashboard. |
| `preview/css/demo.css` | ~2,700 | — | Demo-site syntax highlighting (`.highlight`) and icon-grid preview styles only (`demo.css:1-112`). |
| `dist/css/*.rtl.*`, `dist/js/*.esm.*`, `**/*.map` | >2.5 MB | — | RTL variants, ESM duplicates, and source maps (`tabler.min.css.map` alone is 1,856,337 B; `tabler.min.js.map` is 347,287 B). Note: minified files contain trailing `sourceMappingURL=...` comments; with `/static` mounted via `StaticFiles`, DevTools `.map` requests return a clean 404 (not SPA HTML). |
| `dist/libs/jsvectormap/**` | — | — | World map widget on the demo page only. |

---

## 2. `layout-folded-hover.html` DOM Structure & CSS/JS Hooks

Primary sources: `tmp/tabler-dashboard/layout-folded-hover.html`, `tmp/tabler-dashboard/dist/css/tabler.css`, `tmp/tabler-dashboard/dist/js/tabler.js`, `tmp/tabler-dashboard/dist/js/tabler-theme.js`.

### 2.1 Top-Level Shell Skeleton

```html
<!doctype html>
<html lang="en" data-bs-navbar-position="vertical" data-bs-sidebar="folded-hover">
  <head>
    <link href="/static/vendor/tabler.min.css" rel="stylesheet" />
  </head>
  <body>
    <script src="/static/vendor/tabler-theme.min.js"></script>
    <div class="page">
      <aside class="navbar navbar-vertical navbar-expand-lg">
        <div class="container-fluid">
          <!-- Mobile toggler (<992px) -->
          <button class="navbar-toggler" type="button"
                  data-bs-toggle="collapse" data-bs-target="#sidebar-menu"
                  aria-controls="sidebar-menu" aria-expanded="false"
                  aria-label="Toggle sidebar navigation">
            <span class="navbar-toggler-icon"></span>
          </button>
          <!-- Brand -->
          <div class="navbar-brand navbar-brand-autodark">...</div>
          <!-- Optional pinned bottom slot (order: 1; margin-top: auto) -->
          <div class="navbar-footer">...</div>
          <!-- Collapsible nav -->
          <nav class="collapse navbar-collapse" id="sidebar-menu" aria-label="Sidebar">
            <ul class="navbar-nav pt-lg-3">
              <li class="nav-section-title">Monitor</li>
              <li class="nav-item active">
                <a class="nav-link" href="/">
                  <span class="nav-link-icon"><svg class="icon" ...></svg></span>
                  <span class="nav-link-title">Overview</span>
                </a>
              </li>
            </ul>
            <div class="navbar-side">...</div>
          </nav>
        </div>
      </aside>

      <div class="page-wrapper">
        <div class="page-header d-print-none">
          <div class="container-xl">...</div>
        </div>
        <main id="content" class="page-body">
          <div class="container-xl">...</div>
        </main>
        <footer class="footer footer-transparent d-print-none">
          <div class="container-xl">...</div>
        </footer>
      </div>
    </div>
    <script src="/static/vendor/tabler.min.js" defer></script>
  </body>
</html>
```

Key line citations in `tmp/tabler-dashboard/layout-folded-hover.html`:
- `L2`: `<html lang="en" data-bs-navbar-position="vertical" data-bs-sidebar="folded-hover">`
- `L21`: `<link href="./dist/css/tabler.css" rel="stylesheet" />` (mandatory CSS)
- `L42`: `<script src="./dist/js/tabler-theme.js"></script>` (synchronous right after `<body>` open)
- `L45`: `<div class="page">`
- `L48-502`: `<aside class="navbar navbar-vertical navbar-expand-lg">` containing `.navbar-toggler` (`L51-53`), `.navbar-brand.navbar-brand-autodark` (`L57-67`), `.navbar-footer` (`L71-105`), `<nav class="collapse navbar-collapse" id="sidebar-menu">` (`L108-500`), `.navbar-side` (`L483-499`).
- `L508-1613`: Horizontal `<header class="navbar navbar-expand-md d-print-none">` + `<div class="navbar-expand-md">` (demo-only dual chrome — see caveat below!).
- `L1617-4049`: `<div class="page-wrapper">` wrapping `.page-header` (`L1620-1655`), `<main id="content" class="page-body">` (`L1659-4001`), and `<footer class="footer footer-transparent d-print-none">` (`L4006-4046`).
- `L4978`: `<script src="./dist/js/tabler.js" defer></script>` (mandatory JS).

### 2.2 Critical CSS Caveat: `data-bs-navbar-position` & `<header>` Visibility

In `tmp/tabler-dashboard/dist/css/tabler.css:15329-15339`:

```css
html:not([data-bs-navbar-position=vertical]) .page:has(> [class*=navbar-expand]:not(.navbar-vertical)) > .navbar-vertical {
  display: none;
}
html[data-bs-navbar-position=vertical] .page:has(> .navbar-vertical) > [class*=navbar-expand]:not(.navbar-vertical) {
  display: none;
}
```

- In the Tabler demo (`layout-folded-hover.html`), `<header class="navbar navbar-expand-md">` (`L508`) and `<div class="navbar-expand-md">` (`L1206`) are **hidden** by `tabler.css:15337` because `data-bs-navbar-position="vertical"` is set on `<html>` and they are direct children (`>`) of `.page`. Tabler emits both solely so its live customizer can toggle horizontal vs. vertical mode.
- **Design implication for Monitor Dashboard v2:**
  - If the dashboard has a **pure vertical sidebar + `.page-header`** (no top `<header>` bar), omit the demo `<header>` entirely. Keeping `data-bs-navbar-position="vertical"` on `<html>` is harmless (and keeps `tabler-theme.min.js` consistent).
  - If the dashboard **wants** a top utility `<header class="navbar ...">` (e.g., for Broker status, window selector, Auto-refresh, theme toggle) **alongside** the folded-hover sidebar, either:
    1. Place `<header class="navbar ...">` **inside `<div class="page-wrapper">`** (not a direct child of `.page`), where `.page-wrapper` already supplies `margin-inline-start: var(--tblr-sidebar-width)`, **or**
    2. Keep `<header class="navbar d-print-none">` as a direct child of `.page` **without** any `navbar-expand-*` class — `tabler.css:14673-14676` (`.navbar-expand-lg.navbar-vertical ~ .navbar { margin-inline-start: var(--tblr-sidebar-width); }`) still offsets it while neither `L15329` nor `L15337` hides it.

### 2.3 Folded-Hover Sidebar Behavior (CSS + JS — Both in Core Bundle)

- **CSS (`tabler.css`):**
  - Sidebar width variables (`L13952-13953`): `--tblr-sidebar-width: 16rem; --tblr-sidebar-folded-width: 4rem;`
  - Folded width override (`L15316-15327`): `html[data-bs-sidebar^=folded]` (or `.navbar-vertical:is(.navbar-folded, .navbar-folded-hover)`) sets `--tblr-sidebar-width: var(--tblr-sidebar-folded-width)` (`4rem`), which offsets `.page-wrapper` via `.navbar-expand-lg.navbar-vertical ~ .page-wrapper { margin-inline-start: var(--tblr-sidebar-width); }` (`L14673-14676`).
  - Desktop breakpoint for `navbar-expand-lg` (`@media (min-width: 992px)`, `L15939-16237`):
    - Fixed full-height rail (`position: fixed; inset-inline-start: 0; top: 0; bottom: 0; z-index: 1030; width: var(--tblr-sidebar-width)`).
    - While not hovered and without keyboard focus (`:not(:hover):not(:has(:focus-visible))`): hides `.nav-link-title` (`width: 0; opacity: 0`, `L16153-16158`), collapses `.nav-section-title` to a subtle 20 px horizontal divider (`L16162-16179`), sets `--tblr-nav-link-icon-margin-end: 0` (`L16147-16149`).
    - On hover or keyboard focus (`:hover` / `:has(:focus-visible)`, `L16233-16236`): expands smoothly (`transition: transform 0.3s, width 0.3s`, disabled under `prefers-reduced-motion: reduce`, `L16213-16231`) to `width: 16rem` as an overlay with `var(--tblr-shadow-lg)` (content does not reflow).
- **JS (`tabler.js:4521-4567`, `js/src/sidebar.ts`):**
  - Closes open sidebar dropdowns on outside click (`L4535-4540`) and on `mouseleave` from `.navbar-vertical` in `folded-hover` mode (`L4541-4545`).
  - On `DOMContentLoaded`, removes `.show` from pre-expanded `.navbar-vertical .dropdown-menu.show` when folded (`L4546-4555`).
  - Built-in optional pin/fold toggle: any button with `data-bs-toggle="sidebar-folded"` toggles `html[data-bs-sidebar="folded-hover"]` vs. removed, persists `localStorage['tabler-sidebar']` (`"folded-hover"` / `"default"`), updates `aria-pressed`, and dispatches `tabler:sidebar-folded` (`L4556-4567`).
- **Mobile collapse (`< 992px` for `navbar-expand-lg`):**
  - Below `992px`, `.navbar-vertical.navbar-expand-lg` is not fixed and not folded; `<button class="navbar-toggler" data-bs-toggle="collapse" data-bs-target="#sidebar-menu">` toggles `<nav class="collapse navbar-collapse" id="sidebar-menu">` via Bootstrap `Collapse` (`tabler.js:2448-2591`).

### 2.4 Light/Dark Theme Hooks & `tabler-theme.min.js`

Primary sources: `tmp/tabler-dashboard/dist/js/tabler-theme.js:18-61`, `tabler.css:173, 17335-17366`, `layout-folded-hover.html:42, 553-567`.

- **Managed keys (`tabler-theme.js:18-29`):** `theme` (`"auto"`), `theme-base`, `theme-font`, `theme-primary`, `theme-radius`, `layout`, `navbar`, `navbar-position` (`"horizontal"`), `navbar-theme`, `sidebar` (`"default"`).
- **Resolution order on page load (`tabler-theme.js:39-54`):**
  1. URL query param (`?<key>=<val>`) -> also writes `localStorage.setItem('tabler-' + key, val)`
  2. `localStorage.getItem('tabler-' + key)`
  3. Server-rendered `<html data-bs-<key>="...">` attribute
  4. `themeDefaults[key]`
  - For `theme === "auto"`, resolves via `window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'` and writes `data-bs-theme` on `<html>`.
  - Attaches `prefersDark.addEventListener('change', ...)` (`L55-61`) so OS dark/light switches update `data-bs-theme` live when `localStorage['tabler-theme']` is unset or `"auto"`.
- **Pre-paint placement:** Must be loaded synchronously (no `defer`/`async`) at the top of `<body>` (or in `<head>`) to avoid light-theme flash (`tabler-theme.js:32-36`).
- **CSS visibility helpers (`tabler.css:17335-17366`):**
  - `.hide-theme-light` is `display: none !important` unless `:root` is dark (`data-bs-theme="dark"`).
  - `.hide-theme-dark` is `display: none !important` when `:root` is dark.
  - `.navbar-brand-autodark .navbar-brand-image` applies `filter: brightness(0) invert(1)` in dark mode (`L17362-17366`) — omit `navbar-brand-autodark` if using a custom theme-aware SVG logo using `currentColor`/CSS vars.
- **SPA caveat:** The demo uses `<a href="?theme=dark">` / `<a href="?theme=light">` (`layout-folded-hover.html:553,560`), which forces a full page reload and discards SPA query filters/polling state. In Monitor Dashboard v2, use `<button type="button">` toggles (styled with `.hide-theme-dark` / `.hide-theme-light`) that update `document.documentElement.setAttribute('data-bs-theme', next)` and `localStorage.setItem('tabler-theme', next)` in-place without reloading, while keeping `tabler-theme.min.js` for pre-paint init + OS theme sync.

### 2.5 External Resource & Icon Verification (`tabler.min.css`)

- `url(...)` scan of `tmp/tabler-dashboard/dist/css/tabler.min.css`: **28 matches**, **28 inline `data:image/svg+xml` URIs**, **0 relative/external asset URLs**.
- `@import`: **0**. `@font-face`: **0**.
- Font stacks (`tabler.css:117-118`):
  - `--tblr-font-sans-serif: system-ui, -apple-system, BlinkMacSystemFont, San Francisco, Segoe UI, Roboto, Helvetica Neue, sans-serif;`
  - `--tblr-font-monospace: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace;`
- Icons: `layout-folded-hover.html` embeds inline `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" class="icon">...</svg>` directly — zero icon font files required.
- Bootstrap JS components bundled in `tabler.min.js` (`tabler.js:4501-4675`): `Alert`, `Button`, `Carousel`, `Collapse`, `Dropdown`, `Modal`, `Offcanvas`, `Popover`, `ScrollSpy`, `Tab`, `Toast`, `Tooltip`, plus `@popperjs/core@2.11.8` and `tabler` helpers (`getColor`, `hexToRgba`, `prefix`), exposed on `window.bootstrap` / `window.tabler` (UMD).

---

## 3. Wheel Packaging Behavior (`pyproject.toml` + `hatchling`)

Primary sources: `pyproject.toml:35-46`, `.gitignore:14-19`, `hatchling` 1.33.0 (`WheelBuilder`, `BuilderConfig.exclude_spec`, `BuilderConfig.load_vcs_exclusion_patterns`).

### 3.1 Automatic Recursive Inclusion

`pyproject.toml:45-46`:
```toml
[tool.hatch.build.targets.wheel]
packages = ["szrpc"]
```

Verified directly via `WheelBuilder(root).recurse_included_files()`:
- Hatchling walks **all files and nested subdirectories** under `szrpc/` regardless of whether subdirectories contain `__init__.py` (currently includes `szrpc/monitor/data/monitor.html` and `szrpc/monitor/data/sparklines.js`).
- **No changes to `pyproject.toml` are required** to package `szrpc/monitor/data/vendor/...` or future `szrpc/monitor/data/js/...` subdirectories in either wheels or sdists.

### 3.2 Critical `.gitignore` Exclusion Trap (`dist/`)

`.gitignore:14-19` contains unanchored directory patterns:
```gitignore
etc/
build/
dist/
/*.egg-info
/*.egg
tmp/
```

Because Hatchling sets `ignore_vcs = False` by default (`BuilderConfig.exclude_spec` loads `.gitignore` via `load_vcs_exclusion_patterns()`), verified against the real workspace root (`/home/michel/Projects/swift-rpc`):
- `wb.config.path_is_excluded("szrpc/monitor/data/dist/css/tabler.min.css")` -> **`True`** (and `git check-ignore -v` matches `.gitignore:16:dist/`).
- `wb.config.path_is_excluded("szrpc/monitor/data/vendor/css/tabler.min.css")` -> **`False`**.

**Rule for Ticket #10 (and any future vendoring):** Never create `dist/`, `build/`, `etc/`, or `tmp/` subdirectories under `szrpc/monitor/data/`. Place vendored assets directly under `szrpc/monitor/data/vendor/` (e.g., `vendor/tabler.min.css`, `vendor/tabler.min.js`, `vendor/tabler-theme.min.js`).

---

## 4. FastAPI Mounting & Test Compatibility Notes

Primary sources: `szrpc/monitor/__init__.py:298-371`, `tests/test_monitor.py:37-54, 130-152`, installed `fastapi` 0.163.2 / `starlette` 1.10.0 (`httpx` is **not** installed in `dev` dependencies).

### 4.1 Existing Routes & Test Assertions

- `create_app(monitor)` (`szrpc/monitor/__init__.py:298-371`) currently defines:
  - JSON APIs: `/api/data`, `/api/details/{request_id}`, `/api/summary`, `/api/metrics/workers`, `/api/metrics/tasks`, `/api/calls`
  - Assets: `/` -> `FileResponse(DATA_DIR / "monitor.html")`, `/sparklines.js` -> `FileResponse(DATA_DIR / "sparklines.js", media_type="text/javascript")`
- Both `test_monitor_fastapi_routes` (`tests/test_monitor.py:47-53`) and `test_sqlite_store_persistence_and_windowed_queries` (`tests/test_monitor.py:132-151`) invoke route handlers directly without `httpx`:
  ```python
  routes = {r.path: r.endpoint for r in app.routes if hasattr(r, 'endpoint')}
  ```
- `test_sqlite_store_persistence_and_windowed_queries:140-151` specifically asserts:
  - `routes['/']()` returns a `FileResponse` whose `.path` contains `id="worker-sparklines-card"`, `id="worker-sparklines"`, `id="duration-table-body"`, `from './sparklines.js'`, and not `chart.js`.
  - `routes['/sparklines.js']()` returns a `FileResponse` with `media_type == 'text/javascript'` containing `export function updateWorkerSparklines`.

### 4.2 Implications for Ticket #10 (`StaticFiles` + History API Routes)

1. **`StaticFiles` mount is non-breaking for `app.routes` tests:**
   - `app.mount("/static", StaticFiles(directory=DATA_DIR), name="static")` adds a Starlette `Mount` whose `hasattr(r, "endpoint")` is **`False`** (verified against `fastapi` 0.163.2). Existing `{r.path: r.endpoint for r in app.routes if hasattr(r, 'endpoint')}` comprehensions ignore it cleanly.
   - Serving `/static/vendor/tabler.min.css`, `/static/vendor/tabler.min.js`, `/static/vendor/tabler-theme.min.js` (and future `/static/sparklines.js`, `/static/js/...`) via `StaticFiles` also guarantees missing files like `/static/vendor/tabler.min.css.map` return a real HTTP 404 rather than falling through to SPA HTML.
   - Keep `/sparklines.js` registered for backward compatibility until `monitor.html` and `tests/test_monitor.py:148-151` migrate to `/static/sparklines.js`.
2. **History API SPA shell routes (prefer explicit allowlist over unguarded catch-all):**
   - **Recommended:** Register explicit routes for the 4 top-level pages (`@app.get("/")`, `@app.get("/workers")`, `@app.get("/methods")`, `@app.get("/requests")`, plus any sub-route pattern added later) all serving `FileResponse(DATA_DIR / "monitor.html")`:
     - Directly testable via `routes['/workers']()` without adding `httpx` to `dev` dependencies.
     - Unknown `/api/*`, `/static/*`, or random paths return genuine 404 JSON instead of silently serving HTML.
     - Never shadows FastAPI's built-in `/openapi.json`, `/docs`, or `/redoc`.
   - **If a catch-all `@app.get("/{full_path:path}")` is used instead:** Register it **after** all `/api/*`, `/sparklines.js`, and `app.mount("/static", ...)`, and explicitly raise `HTTPException(status_code=404)` when `full_path == "api"` or `full_path.startswith(("api/", "static/"))`.

---

## 5. Charting Fact Sheet for Ticket #12 (`apexcharts`)

Primary sources: `tmp/tabler-dashboard/dist/libs/apexcharts/dist/apexcharts.min.js`, `tmp/tabler-dashboard/dist/libs/apexcharts/LICENSE`, `tmp/tabler-dashboard/dist/css/tabler-vendors.css:207-283`, `pyproject.toml:7`.

| Metric | Value | Notes |
| --- | --- | --- |
| `apexcharts.min.js` raw size | **897,003 B (876.0 KiB)** | Nearly 2x the ~500 KB estimate in Ticket #12 |
| `apexcharts.min.js` gzip-9 size | **252,913 B (247.0 KiB)** | |
| Core Tabler (3 files) + `apexcharts.min.js` raw | **1,675,247 B (1.598 MiB)** | Exceeds `<1 MB` uncompressed budget by +675 KB (+68%) |
| + `tabler-vendors.min.css` (ApexCharts Tabler theme overrides, `L207-283`) | **1,687,577 B (1.609 MiB)** | Adds 12,330 B raw / 2,831 B gzip-9 (or ~2 KB if only ApexCharts CSS lines are extracted) |
| **License (`dist/libs/apexcharts/LICENSE:1-45`)** | **ApexCharts Dual / Community License (NOT MIT)** | Free Community License is restricted to entities with **`<$2M USD` annual revenue**, forbids sublicensing under different terms (`L21`), and Commercial License forbids redistribution in toolkits, SDKs, or platforms (`L42`) — **incompatible with `szrpc`'s MIT license** (`pyproject.toml:7`). |
