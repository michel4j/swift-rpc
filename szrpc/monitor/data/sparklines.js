const SVG_NS = 'http://www.w3.org/2000/svg';
const DEFAULT_PALETTE = ['#0d6efd', '#198754', '#dc3545', '#fd7e14', '#6f42c1', '#20c997', '#0dcaf0', '#6c757d'];

/**
 * Build an accessible inline SVG sparkline for one or more metric series.
 */
export function buildSparklineSvg(seriesSpecs, {ariaLabel, zeroBase = false, minSpan = 1} = {}) {
    const width = 120;
    const height = 28;
    const padX = 2;
    const padY = 3;

    const svg = document.createElementNS(SVG_NS, 'svg');
    svg.setAttribute('class', 'sparkline-svg');
    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    svg.setAttribute('preserveAspectRatio', 'none');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', ariaLabel);

    const title = document.createElementNS(SVG_NS, 'title');
    title.textContent = ariaLabel;
    svg.appendChild(title);

    const allVals = seriesSpecs.flatMap(s => s.values);
    if (allVals.length === 0) {
        const baseline = document.createElementNS(SVG_NS, 'line');
        baseline.setAttribute('x1', String(padX));
        baseline.setAttribute('y1', String(height / 2));
        baseline.setAttribute('x2', String(width - padX));
        baseline.setAttribute('y2', String(height / 2));
        baseline.setAttribute('stroke', 'currentColor');
        baseline.setAttribute('stroke-opacity', '0.2');
        baseline.setAttribute('stroke-dasharray', '2 2');
        svg.appendChild(baseline);
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
 * Render per-worker sparkline cards into `container`.
 */
export function updateWorkerSparklines({
    container,
    metricsPayload,
    activeWorkers = new Set(),
    workerMetrics = {},
    palette = DEFAULT_PALETTE,
    offscreen = false,
}) {
    if (offscreen || !container) return;

    const series = (metricsPayload && metricsPayload.series) || {};
    const seriesWorkers = Object.keys(series).sort();
    const colorByWorker = new Map(seriesWorkers.map((w, idx) => [w, palette[idx % palette.length]]));

    const allWorkersSet = new Set([...seriesWorkers, ...activeWorkers]);
    const workers = Array.from(allWorkersSet).sort();

    if (workers.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'text-secondary small';
        empty.textContent = 'No worker telemetry recorded in this window.';
        container.replaceChildren(empty);
        return;
    }

    const cards = workers.map((workerId, fallbackIdx) => {
        let pts = series[workerId] || [];
        if (pts.length === 0 && workerMetrics[workerId]) {
            pts = [workerMetrics[workerId]];
        }
        const color = colorByWorker.get(workerId) || palette[fallbackIdx % palette.length];
        const isActive = activeWorkers.has(workerId);

        const article = document.createElement('article');
        article.className = 'worker-spark-card';

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
        statusBadge.className = isActive
            ? 'badge rounded-pill text-bg-success'
            : 'badge rounded-pill bg-secondary-subtle text-secondary-emphasis';
        statusBadge.textContent = isActive ? 'Active' : 'Offline';

        header.append(identity, statusBadge);
        article.appendChild(header);

        const cpuVals = pts.map(pt => Number(pt.cpu_pct || 0));
        const memVals = pts.map(pt => +((Number(pt.mem_rss || 0)) / (1024 * 1024)).toFixed(2));
        const rxVals = pts.map(pt => +((Number(pt.net_rx_bps || 0)) / 1024).toFixed(2));
        const txVals = pts.map(pt => +((Number(pt.net_tx_bps || 0)) / 1024).toFixed(2));
        const taskVals = pts.map(pt => Number(pt.active_tasks || 0));

        const latest = (arr, fmt) => arr.length ? fmt(arr[arr.length - 1]) : '-';
        const cpuText = latest(cpuVals, v => `${v.toFixed(1)}%`);
        const memText = latest(memVals, v => `${v.toFixed(1)} MB`);
        const netText = pts.length
            ? `${rxVals[rxVals.length - 1].toFixed(1)} / ${txVals[txVals.length - 1].toFixed(1)} KB/s`
            : '-';
        const tasksText = latest(taskVals, v => `${Math.round(v)}`);

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
                label: 'Tasks',
                value: tasksText,
                aria: `${workerId} active tasks sparkline, latest ${tasksText}`,
                specs: [{values: taskVals, color, fill: true}],
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

        article.appendChild(metricsGrid);
        return article;
    });

    container.replaceChildren(...cards);
}
