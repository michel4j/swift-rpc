const SVG_NS = 'http://www.w3.org/2000/svg';

export const DEFAULT_PALETTE = [
    'var(--tblr-primary, #066fd1)',
    'var(--tblr-success, #2fb344)',
    'var(--tblr-danger, #d63939)',
    'var(--tblr-warning, #f59f00)',
    'var(--tblr-purple, #ae3ec9)',
    'var(--tblr-teal, #0ca678)',
    'var(--tblr-cyan, #17a2b8)',
    'var(--tblr-secondary, #6b7280)',
];

export const STATUS_COLORS = {
    DONE: 'var(--tblr-success, #2fb344)',
    ERROR: 'var(--tblr-danger, #d63939)',
};

function compareWorkerIds(a, b) {
    const cmp = a.localeCompare(b);
    if (cmp !== 0) return cmp;
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

function defaultFormatDuration(seconds) {
    if (seconds === null || seconds === undefined) return '-';
    const s = Math.abs(Number(seconds));
    if (s > 0 && s < 1) return `${(s * 1000).toFixed(0)}ms`;
    if (s < 60) return `${s.toFixed(s < 10 ? 2 : 1)}s`;
    return `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
}

function createSvgShell(width, height, ariaLabel, extraClass = '') {
    const svg = document.createElementNS(SVG_NS, 'svg');
    svg.setAttribute('class', extraClass ? `sparkline-svg ${extraClass}` : 'sparkline-svg');
    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    svg.setAttribute('preserveAspectRatio', 'none');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', ariaLabel);

    const title = document.createElementNS(SVG_NS, 'title');
    title.textContent = ariaLabel;
    svg.appendChild(title);
    return svg;
}

function appendEmptyBaseline(svg, width, height, padX) {
    const baseline = document.createElementNS(SVG_NS, 'line');
    baseline.setAttribute('x1', String(padX));
    baseline.setAttribute('y1', String(height / 2));
    baseline.setAttribute('x2', String(width - padX));
    baseline.setAttribute('y2', String(height / 2));
    baseline.setAttribute('stroke', 'currentColor');
    baseline.setAttribute('stroke-opacity', '0.2');
    baseline.setAttribute('stroke-dasharray', '2 2');
    svg.appendChild(baseline);
}

/**
 * Partition known Workers in the current window into sorted Active and Offline lists.
 * Returns `{ active, offline, ordered, all }` where `ordered` places Active Workers
 * alphabetically first followed by Offline Workers alphabetically below.
 */
export function partitionWorkers({
    metricsPayload = null,
    tasksPayload = {tasks: {}},
    activeWorkers = new Set(),
} = {}) {
    const series = (metricsPayload && metricsPayload.series) || {};
    const tasksByWorker = (tasksPayload && tasksPayload.tasks) || {};
    const activeSet = activeWorkers instanceof Set ? activeWorkers : new Set(activeWorkers || []);

    const allSet = new Set([
        ...Object.keys(series),
        ...Object.keys(tasksByWorker),
        ...activeSet,
    ]);

    const active = [];
    const offline = [];
    for (const workerId of allSet) {
        if (activeSet.has(workerId)) {
            active.push(workerId);
        } else {
            offline.push(workerId);
        }
    }

    active.sort(compareWorkerIds);
    offline.sort(compareWorkerIds);

    const all = Array.from(allSet).sort(compareWorkerIds);
    const ordered = [...active, ...offline];

    return {active, offline, ordered, all};
}

/**
 * Build an accessible inline SVG sparkline for one or more metric series.
 */
export function buildSparklineSvg(seriesSpecs, {ariaLabel, zeroBase = false, minSpan = 1} = {}) {
    const width = 120;
    const height = 28;
    const padX = 2;
    const padY = 3;

    const svg = createSvgShell(width, height, ariaLabel);
    const allVals = seriesSpecs.flatMap(s => s.values);
    if (allVals.length === 0) {
        appendEmptyBaseline(svg, width, height, padX);
        return svg;
    }

    let minVal;
    let maxVal;
    if (zeroBase) {
        minVal = 0;
        maxVal = Math.max(minSpan, ...allVals);
    } else {
        const rawMin = Math.min(...allVals);
        const rawMax = Math.max(...allVals);
        if (rawMax === rawMin) {
            minVal = Math.max(0, rawMin - 1);
            maxVal = rawMax + 1;
        } else {
            const pad = (rawMax - rawMin) * 0.1;
            minVal = Math.max(0, rawMin - pad);
            maxVal = rawMax + pad;
        }
    }

    const span = maxVal - minVal || 1;
    const plotW = width - 2 * padX;
    const plotH = height - 2 * padY;
    const yFor = (v) => +(height - padY - ((v - minVal) / span) * plotH).toFixed(2);

    seriesSpecs.forEach(spec => {
        const vals = spec.values;
        if (vals.length === 0) return;

        const coords = vals.length === 1
            ? [[padX, yFor(vals[0])], [width - padX, yFor(vals[0])]]
            : vals.map((v, i) => [+(padX + (i / (vals.length - 1)) * plotW).toFixed(2), yFor(v)]);

        const pointsAttr = coords.map(([x, y]) => `${x},${y}`).join(' ');

        if (spec.fill) {
            const area = document.createElementNS(SVG_NS, 'polygon');
            const baseY = height - padY;
            area.setAttribute('points', `${coords[0][0]},${baseY} ${pointsAttr} ${coords[coords.length - 1][0]},${baseY}`);
            area.setAttribute('fill', spec.color);
            area.setAttribute('fill-opacity', '0.14');
            svg.appendChild(area);
        }

        const line = document.createElementNS(SVG_NS, 'polyline');
        line.setAttribute('fill', 'none');
        line.setAttribute('stroke', spec.color);
        line.setAttribute('stroke-width', spec.dashed ? '1.25' : '1.5');
        line.setAttribute('stroke-linecap', 'round');
        line.setAttribute('stroke-linejoin', 'round');
        line.setAttribute('vector-effect', 'non-scaling-stroke');
        if (spec.dashed) {
            line.setAttribute('stroke-dasharray', '3 2');
            line.setAttribute('stroke-opacity', '0.75');
        }
        line.setAttribute('points', pointsAttr);
        svg.appendChild(line);

        const [lastX, lastY] = coords[coords.length - 1];
        const dot = document.createElementNS(SVG_NS, 'circle');
        dot.setAttribute('cx', String(lastX));
        dot.setAttribute('cy', String(lastY));
        dot.setAttribute('r', '2');
        dot.setAttribute('fill', spec.color);
        svg.appendChild(dot);
    });

    return svg;
}

/**
 * Build an accessible inline SVG column/needle sparkline for completed Requests.
 * Each bar (or vertical line when dense) encodes Request duration as height and status as color.
 */
export function buildColumnSparklineSvg(tasks, {ariaLabel, formatDuration = defaultFormatDuration} = {}) {
    const width = 240;
    const height = 32;
    const padX = 2;
    const padY = 2;

    const svg = createSvgShell(width, height, ariaLabel, 'sparkline-svg-tall');
    if (!tasks || tasks.length === 0) {
        appendEmptyBaseline(svg, width, height, padX);
        return svg;
    }

    const plotW = width - 2 * padX;
    const plotH = height - 2 * padY;
    const baseY = height - padY;
    const durations = tasks.map(t => Math.max(0, Number(t.duration || 0)));
    const maxDur = Math.max(...durations, 1e-6);

    const step = Math.min(12, plotW / tasks.length);
    const gap = step >= 3 ? Math.max(1, step * 0.22) : 0;
    const barW = step - gap;
    const useLines = barW < 2;

    tasks.forEach((task, i) => {
        const dur = durations[i];
        const barH = +Math.max(2, (dur / maxDur) * plotH).toFixed(2);
        const yTop = +(baseY - barH).toFixed(2);
        const color = STATUS_COLORS[task.status] || STATUS_COLORS.DONE;
        const tipText = `${task.method || 'request'} (${task.status || 'DONE'}, ${formatDuration(dur)})`;

        let shape;
        if (useLines) {
            shape = document.createElementNS(SVG_NS, 'line');
            const x = +(padX + (i + 0.5) * step).toFixed(2);
            shape.setAttribute('x1', String(x));
            shape.setAttribute('x2', String(x));
            shape.setAttribute('y1', String(baseY));
            shape.setAttribute('y2', String(yTop));
            shape.setAttribute('stroke', color);
            shape.setAttribute('stroke-width', '1.25');
            shape.setAttribute('vector-effect', 'non-scaling-stroke');
        } else {
            shape = document.createElementNS(SVG_NS, 'rect');
            shape.setAttribute('x', String(+(padX + i * step).toFixed(2)));
            shape.setAttribute('y', String(yTop));
            shape.setAttribute('width', String(+barW.toFixed(2)));
            shape.setAttribute('height', String(barH));
            shape.setAttribute('rx', '0.75');
            shape.setAttribute('fill', color);
        }

        const tip = document.createElementNS(SVG_NS, 'title');
        tip.textContent = tipText;
        shape.appendChild(tip);
        svg.appendChild(shape);
    });

    return svg;
}

/**
 * Render per-Worker Tabler-themed sparkline cards into `container`.
 * Active Workers are ordered alphabetically first, followed by Offline Workers below.
 */
export function updateWorkerSparklines({
    container,
    metricsPayload,
    tasksPayload = {tasks: {}},
    activeWorkers = new Set(),
    workerMetrics = {},
    workerMethods = {},
    palette = DEFAULT_PALETTE,
    formatDuration = defaultFormatDuration,
    offscreen = false,
    statusFilter = 'all',
    onMethodClick = null,
    emptyMessage = null,
}) {
    if (offscreen || !container) return;

    const series = (metricsPayload && metricsPayload.series) || {};
    const tasksByWorker = (tasksPayload && tasksPayload.tasks) || {};
    const activeSet = activeWorkers instanceof Set ? activeWorkers : new Set(activeWorkers || []);
    const {active, offline, ordered, all} = partitionWorkers({
        metricsPayload,
        tasksPayload,
        activeWorkers: activeSet,
    });

    const colorByWorker = new Map(all.map((w, idx) => [w, palette[idx % palette.length]]));

    let workers;
    if (statusFilter === 'active') {
        workers = active;
    } else if (statusFilter === 'offline') {
        workers = offline;
    } else {
        workers = ordered;
    }

    if (workers.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'text-secondary small';
        empty.style.gridColumn = '1 / -1';
        empty.textContent = emptyMessage || 'No Worker telemetry recorded in this window.';
        container.replaceChildren(empty);
        return;
    }

    const cards = workers.map((workerId, fallbackIdx) => {
        let pts = series[workerId] || [];
        if (pts.length === 0 && workerMetrics[workerId]) {
            pts = [workerMetrics[workerId]];
        }
        const workerTasks = tasksByWorker[workerId] || [];
        const color = colorByWorker.get(workerId) || palette[fallbackIdx % palette.length];
        const isActive = activeSet.has(workerId);

        const article = document.createElement('article');
        article.className = isActive ? 'card worker-spark-card' : 'card worker-spark-card opacity-75';
        article.dataset.workerId = workerId;
        article.dataset.workerStatus = isActive ? 'active' : 'offline';

        const statusAccent = document.createElement('div');
        statusAccent.className = isActive
            ? 'card-status-top bg-success'
            : 'card-status-top bg-secondary opacity-50';
        statusAccent.setAttribute('aria-hidden', 'true');
        article.appendChild(statusAccent);

        const header = document.createElement('header');
        header.className = 'worker-spark-header';

        const identity = document.createElement('div');
        identity.className = 'worker-spark-identity';

        const dot = document.createElement('span');
        dot.className = 'worker-spark-dot';
        dot.style.backgroundColor = color;
        dot.setAttribute('aria-hidden', 'true');

        const title = document.createElement('h2');
        title.className = 'worker-spark-title';
        title.textContent = workerId;
        title.title = workerId;

        identity.append(dot, title);

        const statusBadge = document.createElement('span');
        statusBadge.className = isActive ? 'badge bg-green-lt' : 'badge bg-secondary-lt';
        statusBadge.textContent = isActive ? 'Active' : 'Offline';

        header.append(identity, statusBadge);
        article.appendChild(header);

        const cpuVals = pts.map(pt => Number(pt.cpu_pct || 0));
        const memVals = pts.map(pt => +((Number(pt.mem_rss || 0)) / (1024 * 1024)).toFixed(2));
        const rxVals = pts.map(pt => +((Number(pt.net_rx_bps || 0)) / 1024).toFixed(2));
        const txVals = pts.map(pt => +((Number(pt.net_tx_bps || 0)) / 1024).toFixed(2));
        const activeReqVals = pts.map(pt => Number(pt.active_tasks || 0));

        const latest = (arr, fmt) => arr.length ? fmt(arr[arr.length - 1]) : '-';
        const cpuText = latest(cpuVals, v => `${v.toFixed(1)}%`);
        const memText = latest(memVals, v => `${v.toFixed(1)} MB`);
        const netText = pts.length
            ? `${rxVals[rxVals.length - 1].toFixed(1)} / ${txVals[txVals.length - 1].toFixed(1)} KB/s`
            : '-';
        const activeReqText = latest(activeReqVals, v => `${Math.round(v)}`);

        const metricsGrid = document.createElement('div');
        metricsGrid.className = 'spark-metrics-grid';

        const metricRows = [
            {
                label: 'CPU',
                value: cpuText,
                aria: `${workerId} CPU sparkline, latest ${cpuText}`,
                specs: [{values: cpuVals, color, fill: true}],
                opts: {zeroBase: true, minSpan: 1},
            },
            {
                label: 'Mem',
                value: memText,
                aria: `${workerId} memory sparkline, latest ${memText}`,
                specs: [{values: memVals, color, fill: true}],
                opts: {zeroBase: false},
            },
            {
                label: 'Net',
                value: netText,
                aria: `${workerId} network Rx and Tx sparkline, latest ${netText}`,
                specs: [
                    {values: rxVals, color, fill: true},
                    {values: txVals, color, dashed: true},
                ],
                opts: {zeroBase: true, minSpan: 0.1},
            },
            {
                label: 'Active Requests',
                value: activeReqText,
                aria: `${workerId} active Requests sparkline, latest ${activeReqText}`,
                specs: [{values: activeReqVals, color, fill: true}],
                opts: {zeroBase: true, minSpan: 1},
            },
        ];

        metricRows.forEach(({label, value, aria, specs, opts}) => {
            const cell = document.createElement('div');
            cell.className = 'spark-metric';

            const head = document.createElement('div');
            head.className = 'spark-metric-head';

            const lbl = document.createElement('span');
            lbl.className = 'spark-metric-label';
            lbl.textContent = label;

            const val = document.createElement('span');
            val.className = 'spark-metric-value';
            val.textContent = value;

            head.append(lbl, val);
            cell.append(head, buildSparklineSvg(specs, {ariaLabel: aria, ...opts}));
            metricsGrid.appendChild(cell);
        });

        const errCount = workerTasks.filter(t => t.status === 'ERROR').length;
        const doneCount = workerTasks.length - errCount;
        const maxDur = workerTasks.length
            ? Math.max(...workerTasks.map(t => Number(t.duration || 0)))
            : null;
        const completedSummary = workerTasks.length
            ? `${workerTasks.length}${errCount ? ` (${errCount} err)` : ''} · max ${formatDuration(maxDur)}`
            : '-';

        const completedCell = document.createElement('div');
        completedCell.className = 'spark-metric spark-metric-full';

        const completedHead = document.createElement('div');
        completedHead.className = 'spark-metric-head';

        const completedLbl = document.createElement('span');
        completedLbl.className = 'spark-metric-label';
        completedLbl.textContent = 'Completed';

        const completedVal = document.createElement('span');
        completedVal.className = 'spark-metric-value';
        completedVal.textContent = completedSummary;

        completedHead.append(completedLbl, completedVal);
        completedCell.append(
            completedHead,
            buildColumnSparklineSvg(workerTasks, {
                ariaLabel: `${workerId} completed Requests: ${doneCount} succeeded, ${errCount} failed${maxDur !== null ? `, max ${formatDuration(maxDur)}` : ''}`,
                formatDuration,
            }),
        );
        metricsGrid.appendChild(completedCell);

        article.appendChild(metricsGrid);

        const methods = Array.isArray(workerMethods[workerId]) ? workerMethods[workerId] : [];
        const footer = document.createElement('footer');
        footer.className = 'worker-spark-footer';

        const methodsLbl = document.createElement('span');
        methodsLbl.className = 'worker-methods-label';
        methodsLbl.textContent = 'Methods';
        footer.appendChild(methodsLbl);

        if (methods.length > 0) {
            const list = document.createElement('ul');
            list.className = 'worker-methods-list';
            list.setAttribute('role', 'list');
            methods.forEach(methodName => {
                const item = document.createElement('li');
                if (typeof onMethodClick === 'function') {
                    const btn = document.createElement('button');
                    btn.type = 'button';
                    btn.className = 'worker-method-tag';
                    btn.textContent = methodName;
                    btn.dataset.worker = workerId;
                    btn.dataset.method = methodName;
                    btn.title = `View ${methodName} Requests on ${workerId}`;
                    btn.setAttribute('aria-label', `Filter Requests by method ${methodName} on Worker ${workerId}`);
                    btn.addEventListener('click', () => onMethodClick(workerId, methodName));
                    item.appendChild(btn);
                } else {
                    const tag = document.createElement('span');
                    tag.className = 'worker-method-tag';
                    tag.textContent = methodName;
                    item.appendChild(tag);
                }
                list.appendChild(item);
            });
            footer.appendChild(list);
        } else {
            const emptyMethods = document.createElement('span');
            emptyMethods.className = 'worker-methods-empty';
            emptyMethods.textContent = '-';
            footer.appendChild(emptyMethods);
        }

        article.appendChild(footer);
        return article;
    });

    container.replaceChildren(...cards);
}
