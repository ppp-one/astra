// History page: plots polled device data and the schedule history.
// The server reduces each series (time buckets for numbers, changes only for
// states), so this page only draws what it gets. Needs d3, Plot and weather.js.

const MARGIN_LEFT = 60;
const MARGIN_RIGHT = 20;
const MAX_CONCURRENT_REQUESTS = 3;
const AUTO_REFRESH_MS = 60 * 1000;
const STORAGE_KEY = "astra-history";

const TWILIGHT_COLORS = {
    day: "rgba(150,187,201,0.12)",
    civil: "rgba(150,187,201,0.12)",
    nautical: "rgba(111,143,154,0.12)",
    astronomical: "rgba(88,106,112,0.12)",
    night: "rgba(47,69,77,0.12)",
};

const ACTION_COLORS = {
    object: "#3b82f6",
    calibration: "#a855f7",
    flats: "#eab308",
    autofocus: "#14b8a6",
    calibrate_guiding: "#06b6d4",
    pointing_model: "#f97316",
    open: "#22c55e",
    close: "#ef4444",
    cool_camera: "#60a5fa",
    complete_headers: "#9ca3af",
};

const MARKER_COLORS = {
    loaded: "#60a5fa",
    schedule_started: "#22c55e",
    schedule_stopped: "#f97316",
};

const SAFETY_BOOLS = new Set(["IsSafe", "WeatherSafe"]);

const DEVICE_ORDER = [
    "ObservingConditions",
    "SafetyMonitor",
    "WeatherSafe",
    "Dome",
    "Telescope",
    "Camera",
    "FilterWheel",
    "Focuser",
];

const WEATHER_ORDER = [
    "RelativeSkyTemp",
    "RainRate",
    "WindSpeed",
    "WindGust",
    "Humidity",
    "SkyTemperature",
    "Temperature",
    "DewPoint",
    "SkyBrightness",
    "Pressure",
];

const DEFAULT_SERIES = new Set([
    "SafetyMonitor|IsSafe",
    "WeatherSafe|WeatherSafe",
    "Dome|ShutterStatus",
    "Telescope|Altitude",
    "Camera|CCDTemperature",
    "Focuser|Position",
]);

const app = document.getElementById("history-app");
const RETENTION_MS = Number(app.dataset.retentionDays || 3) * 24 * 3600 * 1000;

const state = {
    parameters: [],
    selected: new Set(),
    range: { start: null, end: null, hours: 24 },
    // Data of the current range. A new range gets a new object, so late
    // responses for an old range are dropped.
    load: null,
    xScale: null,
    crosshairVisible: false,
    autoRefreshTimer: null,
};

// ---------- small helpers ----------

function seriesKey(p) {
    return `${p.device_type}|${p.device_name}|${p.device_command}`;
}

function esc(text) {
    return String(text ?? "").replace(/[&<>"']/g, (c) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]);
}

function domId(key) {
    return "series-" + key.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function formatTime(ms) {
    return new Date(ms).toLocaleString([], {
        month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
}

function formatNumber(value) {
    if (value === null || value === undefined || Number.isNaN(value)) return "—";
    const abs = Math.abs(value);
    if (abs >= 1000) return value.toFixed(0);
    if (abs >= 100) return value.toFixed(1);
    return value.toFixed(2);
}

function setStatus(text) {
    document.getElementById("status").textContent = text;
}

function loadSettings() {
    try {
        return JSON.parse(localStorage.getItem(STORAGE_KEY)) || {};
    } catch {
        return {};
    }
}

function saveSettings() {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify({
            selected: [...state.selected],
            hours: state.range.hours,
            autoRefresh: document.getElementById("auto-refresh").checked,
        }));
    } catch {
        // Storage can be blocked; the page works without it
    }
}

async function runLimited(items, limit, fn) {
    const queue = [...items];
    const workers = Array.from({ length: Math.min(limit, queue.length) }, async () => {
        while (queue.length) await fn(queue.shift());
    });
    await Promise.all(workers);
}

function plotWidth() {
    return Math.max(320, document.getElementById("plots").clientWidth);
}

function maxPoints() {
    // About one bucket per pixel is enough detail
    return Math.min(1500, Math.max(200, plotWidth() - MARGIN_LEFT - MARGIN_RIGHT));
}

function deviceRank(p) {
    const i = DEVICE_ORDER.indexOf(p.device_type);
    return i === -1 ? DEVICE_ORDER.length : i;
}

function commandRank(p) {
    const i = WEATHER_ORDER.indexOf(p.device_command);
    return i === -1 ? WEATHER_ORDER.length : i;
}

function compareParameters(a, b) {
    return deviceRank(a) - deviceRank(b)
        || a.device_type.localeCompare(b.device_type)
        || a.device_name.localeCompare(b.device_name)
        || commandRank(a) - commandRank(b)
        || a.device_command.localeCompare(b.device_command);
}

function isDefault(p) {
    return p.device_type === "ObservingConditions"
        || DEFAULT_SERIES.has(`${p.device_type}|${p.device_command}`);
}

function selectedParameters() {
    return state.parameters.filter((p) => state.selected.has(seriesKey(p)));
}

function seriesColor(p) {
    const color = color_palette(p.device_command);
    if (!color.startsWith("rgba(128, 128, 128")) return color;
    const i = state.parameters.indexOf(p);
    return d3.schemeTableau10[(i < 0 ? 0 : i) % 10];
}

// ---------- time range ----------

function toLocalInput(date) {
    const offset = date.getTimezoneOffset() * 60000;
    return new Date(date.getTime() - offset).toISOString().slice(0, 16);
}

function syncRangeControls() {
    document.getElementById("range-start").value = toLocalInput(state.range.start);
    document.getElementById("range-end").value = toLocalInput(state.range.end);
    document.querySelectorAll(".preset").forEach((button) => {
        button.classList.toggle("active", Number(button.dataset.hours) === state.range.hours);
    });
}

function setPreset(hours) {
    const end = new Date();
    state.range = { start: new Date(end.getTime() - hours * 3600 * 1000), end, hours };
    syncRangeControls();
}

function applyCustomRange() {
    const startValue = document.getElementById("range-start").value;
    const endValue = document.getElementById("range-end").value;
    const now = Date.now();
    // A datetime-local value without an offset is read as local time
    let start = startValue ? new Date(startValue) : new Date(now - 24 * 3600 * 1000);
    let end = endValue ? new Date(endValue) : new Date(now);
    if (end.getTime() > now) end = new Date(now);
    if (start.getTime() < now - RETENTION_MS) start = new Date(now - RETENTION_MS);
    if (!(start < end)) {
        setStatus("The start time must be before the end time, within the last days kept.");
        return;
    }
    state.range = { start, end, hours: null };
    syncRangeControls();
    saveSettings();
    loadAll();
}

function updateXScale() {
    state.xScale = d3.scaleTime()
        .domain([state.range.start, state.range.end])
        .range([MARGIN_LEFT, plotWidth() - MARGIN_RIGHT]);
}

function clipToDomain(x1, x2) {
    const lo = state.range.start.getTime();
    const hi = state.range.end.getTime();
    const a = Math.max(x1, lo);
    const b = Math.min(x2, hi);
    return b > a ? [a, b] : null;
}

function twilightMarks() {
    const periods = state.load?.schedule?.twilight_periods || [];
    const rects = [];
    for (const period of periods) {
        const clipped = clipToDomain(new Date(period.start).getTime(), new Date(period.end).getTime());
        if (clipped) rects.push({ x1: new Date(clipped[0]), x2: new Date(clipped[1]), phase: period.phase });
    }
    return rects.length
        ? [Plot.rect(rects, { x1: "x1", x2: "x2", fill: (d) => TWILIGHT_COLORS[d.phase] })]
        : [];
}

function basePlotOptions(height) {
    return {
        width: plotWidth(),
        height,
        marginLeft: MARGIN_LEFT,
        marginRight: MARGIN_RIGHT,
        x: { type: "time", domain: [state.range.start, state.range.end], label: null },
        style: { background: "transparent", color: "#cbd5e1", fontSize: "11px" },
    };
}

// ---------- parameter picker ----------

function renderPicker() {
    const groups = d3.groups(state.parameters, (p) => `${p.device_type}|${p.device_name}`);
    document.getElementById("picker-groups").innerHTML = groups.map(([group, params]) => {
        const [type, name] = group.split("|");
        const items = params.map((p) => {
            const key = seriesKey(p);
            const unit = p.unit ? ` <span class="text-gray-500">(${esc(p.unit)})</span>` : "";
            return `<label class="flex items-center gap-2 py-0.5">
                <input type="checkbox" class="accent-blue-600" data-key="${esc(key)}"
                    ${state.selected.has(key) ? "checked" : ""}>
                <span>${esc(p.device_command)}${unit}</span>
                <span class="text-gray-500 text-xs">${esc(p.dtype)}</span>
            </label>`;
        }).join("");
        return `<fieldset class="rounded bg-black/20 p-2">
            <legend class="text-xs text-gray-400 px-1">${esc(type)} · ${esc(name)}</legend>
            ${items}
        </fieldset>`;
    }).join("");
    updatePickerCount();
}

function updatePickerCount() {
    document.getElementById("picker-count").textContent =
        `(${state.selected.size} of ${state.parameters.length} selected)`;
}

function onSelectionChanged() {
    updatePickerCount();
    saveSettings();
    buildPlotContainers();
    loadMissingSeries();
}

// ---------- loading ----------

async function loadParameters() {
    const res = await fetch("/api/history/parameters");
    const body = await res.json();
    if (body.status !== "success") throw new Error(body.message || "Could not list parameters");
    state.parameters = body.data.sort(compareParameters);

    const known = new Set(state.parameters.map(seriesKey));
    const saved = loadSettings().selected;
    const keys = Array.isArray(saved) ? saved.filter((k) => known.has(k)) : [];
    state.selected = new Set(
        keys.length ? keys : state.parameters.filter(isDefault).map(seriesKey)
    );
}

async function loadAll() {
    if (state.load) state.load.controller.abort();
    if (state.range.hours) setPreset(state.range.hours);

    const load = {
        controller: new AbortController(),
        series: new Map(),
        pending: new Set(),
        schedule: null,
    };
    state.load = load;
    updateXScale();
    buildPlotContainers();
    renderTimeline();
    setStatus("Loading…");

    await Promise.all([
        loadSchedule(load),
        runLimited(selectedParameters(), MAX_CONCURRENT_REQUESTS, (p) => loadSeries(load, p)),
    ]);

    if (state.load === load) {
        setStatus(`Updated ${new Date().toLocaleTimeString()}`);
    }
}

function loadMissingSeries() {
    const load = state.load;
    if (!load) return;
    const missing = selectedParameters().filter(
        (p) => !load.series.has(seriesKey(p)) && !load.pending.has(seriesKey(p))
    );
    runLimited(missing, MAX_CONCURRENT_REQUESTS, (p) => loadSeries(load, p));
}

function rangeParams() {
    return {
        start: state.range.start.toISOString(),
        end: state.range.end.toISOString(),
    };
}

async function loadSeries(load, p) {
    const key = seriesKey(p);
    if (load.series.has(key) || load.pending.has(key)) return;
    load.pending.add(key);
    try {
        const params = new URLSearchParams({
            device_type: p.device_type,
            device_name: p.device_name,
            device_command: p.device_command,
            ...rangeParams(),
            max_points: String(maxPoints()),
        });
        const res = await fetch(`/api/history/series?${params}`, { signal: load.controller.signal });
        const body = await res.json();
        if (state.load !== load) return;
        if (body.status !== "success") throw new Error(body.message || "Request failed");
        load.series.set(key, body.data);
        renderSeries(p);
    } catch (error) {
        if (error.name === "AbortError" || state.load !== load) return;
        renderSeriesMessage(p, `Could not load: ${error.message}`);
    } finally {
        load.pending.delete(key);
    }
}

async function loadSchedule(load) {
    try {
        const res = await fetch(`/api/history/schedule?${new URLSearchParams(rangeParams())}`, {
            signal: load.controller.signal,
        });
        const body = await res.json();
        if (state.load !== load) return;
        if (body.status !== "success") throw new Error(body.message || "Request failed");
        load.schedule = body.data;
        renderTimeline();
        renderSnapshots();
        // Twilight shading arrives with the schedule, so draw the plots again
        for (const p of selectedParameters()) {
            if (load.series.has(seriesKey(p))) renderSeries(p);
        }
    } catch (error) {
        if (error.name === "AbortError" || state.load !== load) return;
        document.getElementById("timeline").innerHTML =
            `<p class="text-red-400 text-xs">Could not load schedule history: ${esc(error.message)}</p>`;
    }
}

// ---------- series plots ----------

function buildPlotContainers() {
    const container = document.getElementById("plots");
    const params = selectedParameters();
    const wanted = new Set(params.map((p) => domId(seriesKey(p))));

    // Remove plots that are no longer selected
    [...container.children].forEach((child) => {
        if (!wanted.has(child.id)) child.remove();
    });

    // Add new plots and keep the sort order
    params.forEach((p) => {
        const id = domId(seriesKey(p));
        let wrapper = document.getElementById(id);
        if (!wrapper) {
            const unit = p.unit ? ` · ${esc(p.unit)}` : "";
            wrapper = document.createElement("div");
            wrapper.id = id;
            wrapper.className = "rounded-lg bg-gray-600/20 px-0 py-2";
            wrapper.innerHTML = `
                <div class="flex justify-between gap-2 px-3 text-xs">
                    <span><span class="font-medium text-slate-100">${esc(p.device_command)}</span>
                    <span class="text-gray-400">${esc(p.device_name)}${unit}</span>
                    <span class="note text-amber-400"></span></span>
                    <span class="readout text-gray-300 tabular-nums"></span>
                </div>
                <div class="history-plot relative">
                    <div class="plot-body text-gray-500 text-xs px-3 py-3">Loading…</div>
                    <div class="crosshair"></div>
                </div>`;
        }
        container.appendChild(wrapper);
        if (state.load?.series.has(seriesKey(p))) renderSeries(p);
    });

    if (!params.length) {
        container.innerHTML = `<p class="text-gray-400 text-xs">No parameters selected. Open "Parameters" to pick some.</p>`;
    }
}

function renderSeriesMessage(p, message) {
    const wrapper = document.getElementById(domId(seriesKey(p)));
    if (!wrapper) return;
    wrapper.querySelector(".plot-body").outerHTML =
        `<div class="plot-body text-gray-500 text-xs px-3 py-3">${esc(message)}</div>`;
}

function renderSeries(p) {
    const wrapper = document.getElementById(domId(seriesKey(p)));
    const series = state.load?.series.get(seriesKey(p));
    if (!wrapper || !series) return;

    const isNumeric = series.dtype === "float" || series.dtype === "int";
    if (!series.data.t.length) {
        renderSeriesMessage(p, "No data in this range.");
        return;
    }

    const plot = isNumeric ? numericPlot(p, series) : statePlot(p, series);
    plot.classList.add("plot-body");
    wrapper.querySelector(".plot-body").replaceWith(plot);
    wrapper.querySelector(".note").textContent =
        series.data.truncated ? " · too many changes, only the first part is shown" : "";
}

function gapThreshold(data) {
    const diffs = [];
    for (let i = 1; i < data.t.length; i++) diffs.push(data.t[i] - data.t[i - 1]);
    const median = diffs.length ? d3.median(diffs) : 0;
    return Math.max(3 * data.bucket_s * 1000, 3 * median);
}

function numericPoints(data) {
    // A point with no value between two far-apart points breaks the line, so
    // a time without data (device offline) shows as a gap
    const gap = gapThreshold(data);
    const points = [];
    for (let i = 0; i < data.t.length; i++) {
        if (i > 0 && data.t[i] - data.t[i - 1] > gap) {
            points.push({ t: new Date(data.t[i - 1] + 1), mean: null, min: null, max: null });
        }
        points.push({ t: new Date(data.t[i]), mean: data.mean[i], min: data.min[i], max: data.max[i] });
    }
    return points;
}

function numericDomain(series) {
    const values = series.data.min.concat(series.data.max).filter((v) => v !== null);
    let lo = d3.min(values);
    let hi = d3.max(values);
    let span = hi - lo || Math.abs(hi) * 0.1 || 1;
    // Show a safety limit when it is near the data
    for (const limit of [series.limits?.lower, series.limits?.upper]) {
        if (limit === null || limit === undefined) continue;
        if (limit >= lo - span * 0.5 && limit <= hi + span * 0.5) {
            lo = Math.min(lo, limit);
            hi = Math.max(hi, limit);
        }
    }
    if (lo === hi) {
        lo -= span / 2;
        hi += span / 2;
    }
    return [lo, hi];
}

function numericPlot(p, series) {
    const color = seriesColor(p);
    const points = numericPoints(series.data);
    const domain = numericDomain(series);
    // Only limits near the data are in the domain; others are not drawn
    const limits = [
        { value: series.limits?.lower, label: "lower safety limit" },
        { value: series.limits?.upper, label: "upper safety limit" },
    ].filter((d) => d.value != null && d.value >= domain[0] && d.value <= domain[1]);

    return Plot.plot({
        ...basePlotOptions(150),
        marginTop: 8,
        marginBottom: 32,
        y: { label: null, grid: true, domain, nice: true },
        marks: [
            ...twilightMarks(),
            Plot.areaY(points, { x: "t", y1: "min", y2: "max", fill: color, fillOpacity: 0.3 }),
            Plot.lineY(points, { x: "t", y: "mean", stroke: color, strokeWidth: 1.5 }),
            points.length < 60
                ? Plot.dot(points, { x: "t", y: "mean", fill: color, r: 1.5 })
                : null,
            Plot.ruleY(limits, {
                y: "value",
                stroke: "red",
                strokeDasharray: "5,3",
                strokeOpacity: 0.6,
                title: (d) => `${d.label}: ${d.value} ${series.unit}`,
            }),
        ],
    });
}

function valueLabel(series, value) {
    if (value === null || value === undefined) return "unknown";
    if (series.labels && series.labels[String(value)] !== undefined) return series.labels[String(value)];
    if (series.dtype === "bool") return value ? "True" : "False";
    return String(value);
}

function stateColor(p, series, value) {
    if (series.dtype === "bool") {
        if (SAFETY_BOOLS.has(p.device_command)) return value ? "#16a34a" : "#dc2626";
        return value ? "#2563eb" : "#4b5563";
    }
    const label = valueLabel(series, value);
    if (/error/i.test(label)) return "#dc2626";
    if (typeof value === "number") return d3.schemeTableau10[((value % 10) + 10) % 10];
    let hash = 0;
    for (const c of label) hash = (hash * 31 + c.charCodeAt(0)) | 0;
    return d3.schemeTableau10[Math.abs(hash) % 10];
}

function withoutGaps(x1, x2, gaps) {
    // Split [x1, x2] into the parts that are not inside a gap
    const parts = [];
    let from = x1;
    for (const [g1, g2] of gaps) {
        if (g2 <= from || g1 >= x2) continue;
        if (g1 > from) parts.push([from, g1]);
        from = Math.max(from, g2);
    }
    if (from < x2) parts.push([from, x2]);
    return parts;
}

function stateSegments(series) {
    const { t, v, last_t, gaps = [] } = series.data;
    const end = state.range.end.getTime();
    // Data is current if the last sample is recent, so the last value lasts
    // until the end. Otherwise it stops at the last sample (device stopped).
    const lastEnd = last_t !== null && end - last_t > 5 * 60 * 1000 ? last_t : end;
    const segments = [];
    for (let i = 0; i < t.length; i++) {
        const x2 = i + 1 < t.length ? t[i + 1] : Math.max(lastEnd, t[i]);
        for (const [a, b] of withoutGaps(t[i], x2, gaps)) {
            const clipped = clipToDomain(a, b);
            if (!clipped) continue;
            segments.push({
                x1: new Date(clipped[0]),
                x2: new Date(clipped[1]),
                value: v[i],
                label: valueLabel(series, v[i]),
            });
        }
    }
    return segments;
}

function statePlot(p, series) {
    const segments = stateSegments(series);
    const x = state.xScale;
    const wide = segments.filter((s) => x(s.x2) - x(s.x1) > 8 * s.label.length + 8);

    return Plot.plot({
        ...basePlotOptions(48),
        marginTop: 4,
        marginBottom: 4,
        x: { ...basePlotOptions(48).x, axis: null },
        marks: [
            Plot.rect(segments, {
                x1: "x1",
                x2: "x2",
                fill: (d) => stateColor(p, series, d.value),
                fillOpacity: 0.75,
                title: (d) => `${d.label}\n${formatTime(d.x1)} – ${formatTime(d.x2)}`,
            }),
            Plot.text(wide, {
                x: (d) => new Date((d.x1.getTime() + d.x2.getTime()) / 2),
                text: "label",
                fill: "white",
                fontSize: 10,
            }),
        ],
    });
}

// ---------- schedule timeline ----------

function scheduleRuns(events) {
    const open = new Map();
    const runs = [];
    for (const e of events) {
        const key = `${e.snapshot_id}|${e.action_index}`;
        if (e.event === "action_started") {
            open.set(key, e);
        } else if (["action_finished", "action_failed", "action_stopped"].includes(e.event)) {
            const start = open.get(key);
            if (start) {
                runs.push({ start, end: e.t, status: e.event.replace("action_", ""), message: e.message });
                open.delete(key);
            }
        } else if (e.event === "schedule_stopped") {
            for (const start of open.values()) runs.push({ start, end: e.t, status: "stopped" });
            open.clear();
        }
    }
    for (const start of open.values()) runs.push({ start, end: state.range.end.getTime(), status: "running" });
    return runs;
}

function plannedActions(schedule, events) {
    // Each loaded schedule is planned from its load time until the next load
    const loads = events.filter((e) => e.event === "loaded" && e.snapshot_id !== null);
    if (schedule.loaded_at_start !== null && !loads.some((e) => e.t < state.range.start.getTime())) {
        loads.unshift({ t: -Infinity, snapshot_id: schedule.loaded_at_start });
    }
    const planned = [];
    loads.forEach((load, i) => {
        const until = i + 1 < loads.length ? loads[i + 1].t : Infinity;
        const snapshot = schedule.snapshots[String(load.snapshot_id)];
        if (!snapshot) return;
        for (const action of snapshot.actions) {
            const clipped = clipToDomain(action.start, Math.min(action.end, until));
            if (clipped) planned.push({ ...action, x1: new Date(clipped[0]), x2: new Date(clipped[1]) });
        }
    });
    return planned;
}

function actionLabel(schedule, event) {
    const action = schedule.snapshots[String(event.snapshot_id)]?.actions
        .find((a) => a.index === event.action_index);
    return action ? action.label : event.action_type;
}

function renderTimeline() {
    const container = document.getElementById("timeline");
    const schedule = state.load?.schedule;
    if (!schedule) {
        container.innerHTML = `<p class="text-gray-500 text-xs py-2">Loading…</p>`;
        return;
    }

    const events = schedule.events;
    const planned = plannedActions(schedule, events);
    // A run of a few seconds is still drawn at least 3 px wide
    const minRunMs = state.xScale.invert(MARGIN_LEFT + 3).getTime() - state.range.start.getTime();
    const runs = scheduleRuns(events)
        .map((run) => {
            const clipped = clipToDomain(run.start.t, Math.max(run.end, run.start.t + minRunMs));
            return clipped && {
                ...run,
                device_name: run.start.device_name,
                action_type: run.start.action_type,
                label: actionLabel(schedule, run.start),
                x1: new Date(clipped[0]),
                x2: new Date(clipped[1]),
                finished: run.end,
            };
        })
        .filter(Boolean);
    const markers = events
        .filter((e) => e.event in MARKER_COLORS && clipToDomain(e.t, e.t + 1))
        .map((e) => ({ ...e, x: new Date(e.t) }));

    const rows = [...new Set([...planned, ...runs].map((d) => d.device_name))].sort();
    if (!rows.length && !markers.length) {
        container.innerHTML = `<p class="text-gray-500 text-xs py-2">No schedule activity in this range.</p>`;
        return;
    }
    if (!rows.length) rows.push("");

    const plot = Plot.plot({
        ...basePlotOptions(38 + 42 * rows.length),
        marginTop: 6,
        marginBottom: 32,
        y: { domain: rows, axis: null, padding: 0.1 },
        marks: [
            ...twilightMarks(),
            Plot.barX(planned, {
                x1: "x1",
                x2: "x2",
                y: "device_name",
                fill: (d) => ACTION_COLORS[d.action_type] || "#9ca3af",
                fillOpacity: 0.25,
                insetTop: 15,
                insetBottom: 14,
                title: (d) => `Planned: ${d.action_type} ${d.label}\n${formatTime(d.start)} – ${formatTime(d.end)}`,
            }),
            Plot.barX(runs, {
                x1: "x1",
                x2: "x2",
                y: "device_name",
                fill: (d) => d.status === "failed" ? "#dc2626" : (ACTION_COLORS[d.action_type] || "#9ca3af"),
                fillOpacity: 0.9,
                insetTop: 27,
                insetBottom: 2,
                title: (d) => `Ran: ${d.action_type} ${d.label} (${d.status})\n`
                    + `${formatTime(d.start.t)} – ${formatTime(d.finished)}${d.message ? `\n${d.message}` : ""}`,
            }),
            Plot.text(rows, {
                y: (d) => d,
                text: (d) => d,
                frameAnchor: "left",
                textAnchor: "start",
                dx: 4,
                dy: -13,
                fill: "#94a3b8",
                fontSize: 10,
            }),
            Plot.ruleX(markers, {
                x: "x",
                stroke: (d) => MARKER_COLORS[d.event],
                strokeWidth: 1.5,
                strokeDasharray: (d) => d.event === "loaded" ? "4,3" : null,
                title: (d) => `${d.event.replace("_", " ")} ${formatTime(d.t)}${d.message ? `\n${d.message}` : ""}`,
            }),
        ],
    });
    container.replaceChildren(plot);
    const crosshair = document.createElement("div");
    crosshair.className = "crosshair";
    container.appendChild(crosshair);
}

function renderSnapshots() {
    const container = document.getElementById("snapshots");
    const schedule = state.load?.schedule;
    if (!schedule) return;

    const loads = schedule.events.filter(
        (e) => e.event === "loaded" && e.t >= state.range.start.getTime()
    );
    const chips = [];
    if (schedule.loaded_at_start !== null && schedule.snapshots[String(schedule.loaded_at_start)]) {
        chips.push({ id: schedule.loaded_at_start, text: "Loaded at start of range" });
    }
    for (const e of loads) {
        if (schedule.snapshots[String(e.snapshot_id)]) {
            chips.push({ id: e.snapshot_id, text: `Loaded ${formatTime(e.t)}` });
        }
    }

    container.innerHTML = chips.map((chip) => {
        const n = schedule.snapshots[String(chip.id)].n_actions;
        return `<button type="button" data-snapshot="${chip.id}"
            class="px-2 py-1 rounded bg-gray-700/80 hover:bg-gray-600/80 text-xs">
            ${esc(chip.text)} · ${n} action${n === 1 ? "" : "s"}</button>`;
    }).join("");
    document.getElementById("snapshot-detail").classList.add("hidden");
}

function showSnapshot(id) {
    const snapshot = state.load?.schedule?.snapshots[String(id)];
    const detail = document.getElementById("snapshot-detail");
    if (!snapshot) return;
    const rows = snapshot.actions.map((a) => `<tr class="border-t border-gray-700/50">
        <td class="pr-3 py-0.5">${esc(a.device_name)}</td>
        <td class="pr-3">${esc(a.action_type)}</td>
        <td class="pr-3">${esc(a.label)}</td>
        <td class="pr-3 tabular-nums">${esc(formatTime(a.start))}</td>
        <td class="tabular-nums">${esc(formatTime(a.end))}</td>
    </tr>`).join("");
    detail.innerHTML = `
        <div class="flex items-center gap-3 mb-2 text-xs">
            <span class="text-gray-300">Schedule loaded ${esc(formatTime(snapshot.t))}</span>
            <a class="text-blue-400 hover:underline" href="/api/history/schedule/${snapshot.id}">Download JSONL</a>
            <button type="button" id="snapshot-close" class="text-gray-400 hover:text-white">Close</button>
        </div>
        <table class="text-xs text-left w-full">
            <thead class="text-gray-400"><tr>
                <th class="pr-3 font-normal">Device</th><th class="pr-3 font-normal">Action</th>
                <th class="pr-3 font-normal">Target</th><th class="pr-3 font-normal">Start</th>
                <th class="font-normal">End</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>`;
    detail.classList.remove("hidden");
}

// ---------- crosshair and value readouts ----------

function readoutAt(series, t) {
    const data = series.data;
    if (!data.t.length) return "";
    if (series.dtype === "float" || series.dtype === "int") {
        const i = d3.bisector((d) => d).center(data.t, t);
        if (Math.abs(data.t[i] - t) > gapThreshold(data)) return "no data";
        const unit = series.unit ? ` ${series.unit}` : "";
        const range = data.min[i] !== data.max[i]
            ? ` (${formatNumber(data.min[i])} – ${formatNumber(data.max[i])})` : "";
        return `${formatNumber(data.mean[i])}${unit}${range}`;
    }
    const segment = stateSegments(series).find((s) => s.x1.getTime() <= t && t <= s.x2.getTime());
    return segment ? segment.label : "no data";
}

function updateCrosshair(x) {
    const inside = Boolean(state.xScale) && x >= MARGIN_LEFT && x <= plotWidth() - MARGIN_RIGHT;
    if (!inside && !state.crosshairVisible) return;
    state.crosshairVisible = inside;
    document.querySelectorAll(".crosshair").forEach((line) => {
        line.style.display = inside ? "block" : "none";
        if (inside) line.style.left = `${x}px`;
    });
    if (!inside) {
        document.querySelectorAll("#plots .readout").forEach((r) => (r.textContent = ""));
        return;
    }
    const t = state.xScale.invert(x).getTime();
    for (const p of selectedParameters()) {
        const wrapper = document.getElementById(domId(seriesKey(p)));
        const series = state.load?.series.get(seriesKey(p));
        if (wrapper && series) {
            wrapper.querySelector(".readout").textContent = `${formatTime(t)} · ${readoutAt(series, t)}`;
        }
    }
}

document.addEventListener("pointermove", (event) => {
    const area = event.target.closest?.(".history-plot");
    if (!area) {
        updateCrosshair(-1);
        return;
    }
    updateCrosshair(event.clientX - area.getBoundingClientRect().left);
});

// ---------- events and start ----------

document.querySelectorAll(".preset").forEach((button) => {
    button.addEventListener("click", () => {
        setPreset(Number(button.dataset.hours));
        saveSettings();
        loadAll();
    });
});

document.getElementById("apply-range").addEventListener("click", applyCustomRange);
document.getElementById("refresh").addEventListener("click", () => loadAll());

document.getElementById("auto-refresh").addEventListener("change", (event) => {
    clearInterval(state.autoRefreshTimer);
    if (event.target.checked) {
        state.autoRefreshTimer = setInterval(() => {
            // Only a range that ends now moves with time
            if (state.range.hours && !document.hidden) loadAll();
        }, AUTO_REFRESH_MS);
    }
    saveSettings();
});

document.getElementById("picker-groups").addEventListener("change", (event) => {
    const key = event.target.dataset.key;
    if (!key) return;
    if (event.target.checked) state.selected.add(key);
    else state.selected.delete(key);
    onSelectionChanged();
});

document.getElementById("select-defaults").addEventListener("click", () => {
    state.selected = new Set(state.parameters.filter(isDefault).map(seriesKey));
    renderPicker();
    onSelectionChanged();
});

document.getElementById("select-none").addEventListener("click", () => {
    state.selected = new Set();
    renderPicker();
    onSelectionChanged();
});

document.getElementById("snapshots").addEventListener("click", (event) => {
    const button = event.target.closest("[data-snapshot]");
    if (button) showSnapshot(button.dataset.snapshot);
});

document.getElementById("snapshot-detail").addEventListener("click", (event) => {
    if (event.target.id === "snapshot-close") {
        document.getElementById("snapshot-detail").classList.add("hidden");
    }
});

let resizeTimer;
window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
        if (!state.load) return;
        updateXScale();
        renderTimeline();
        for (const p of selectedParameters()) renderSeries(p);
    }, 200);
});

(async function start() {
    const settings = loadSettings();
    const presetHours = [...document.querySelectorAll(".preset")].map((b) => Number(b.dataset.hours));
    setPreset(presetHours.includes(settings.hours) ? settings.hours : 24);

    if (settings.autoRefresh) {
        const checkbox = document.getElementById("auto-refresh");
        checkbox.checked = true;
        checkbox.dispatchEvent(new Event("change"));
    }

    try {
        await loadParameters();
    } catch (error) {
        setStatus(`Could not load the list of parameters: ${error.message}`);
        return;
    }
    renderPicker();
    loadAll();
})();
