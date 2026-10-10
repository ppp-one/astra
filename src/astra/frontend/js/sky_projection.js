/**
 * All-Sky Projection Visualization
 *
 * Displays a real-time circular all-sky projection with zenith at center and horizon at edge.
 * Shows brightest stars, planets, sun, moon, and telescope positions.
 * Uses azimuthal equidistant projection: radius = 90° - altitude, angle = azimuth
 */

// read stars.json file in vanilla JS
let STAR_CATALOG = [];

fetch("js/stars.json")
    .then((response) => response.json())
    .then((data) => {
        // Convert star data to array of [name, ra, dec, mag]
        STAR_CATALOG = data.map((star) => [
            star.name,
            star.ra, // in degrees
            star.dec, // in degrees
            star.mag, // apparent magnitude
        ]);
        console.log("Loaded star catalog with", STAR_CATALOG.length, "stars.");

        // Update chart if it's already running
        if (skyData) {
            plotSkyProjection();
        }
    });
// Messier and NGC objects from OpenNGC (https://github.com/mattiaverga/OpenNGC,
// CC BY-SA 4.0), built into dso.json
let DSO_CATALOG = [];

fetch("js/dso.json")
    .then((response) => response.json())
    .then((data) => {
        DSO_CATALOG = data.objects.map(
            ([name, messier, commonName, type, ra, dec, mag, size]) => ({
                name: messier || name, // Messier name first, e.g. "M31", else "NGC 891"
                commonName,
                dsoType: type,
                ra,
                dec,
                mag,
                size,
                messier: messier !== "",
            }),
        );
        console.log("Loaded deep-sky catalog with", DSO_CATALOG.length, "objects.");

        if (skyData) {
            plotSkyProjection();
        }
    });

// OpenNGC type codes grouped for colour and legend
const DSO_CLASSES = {
    galaxy: { label: "galaxy", color: "#fda4af", types: ["G", "GPair", "GTrpl", "GGroup"] }, // rose-300
    openCluster: { label: "open cluster", color: "#93c5fd", types: ["OCl", "*Ass", "**"] }, // blue-300
    globularCluster: { label: "globular cluster", color: "#fde68a", types: ["GCl"] }, // amber-200
    nebula: { label: "nebula", color: "#86efac", types: ["Neb", "HII", "EmN", "RfN", "SNR", "Cl+N", "DrkN"] }, // green-300
    planetaryNebula: { label: "planetary nebula", color: "#5eead4", types: ["PN"] }, // teal-300
};

function dsoClass(type) {
    return Object.values(DSO_CLASSES).find((c) => c.types.includes(type)) || { label: "other", color: "#9ca3af" };
}

function dsoIsShown(dso) {
    // all Messier objects; NGC objects to magnitude 8 at full view, and one
    // magnitude fainter for each doubling of the zoom
    if (dso.messier) return true;
    return dso.mag !== null && dso.mag <= 8 + Math.log2(skyView.scale);
}

// Global variables
let skyChart = null;
let skyData = null;
let telescopes = [];
let updateInterval = null;
let skyChartMousePos = { x: null, y: null, type: "mouse" };
// objects drawn in the current plot, to find the one under a right-click or long-press
let skyPlottedObjects = [];

// Zoom and pan: the centre of the view in plot units (1 unit = 1° from the
// zenith) and the zoom factor, 1 = whole sky
const SKY_MAX_ZOOM = 40;
const SKY_LABEL_ZOOM = 3; // from this zoom on, label NGC objects and stars
// a proper star name: capitalised words, and no constellation abbreviation at the end
const STAR_PROPER_NAME = /^(?![^]* [A-Z][a-z]{2}$)[A-Z][a-z']+( [A-Z][a-z']+)*$/;
let skyView = { cx: 0, cy: 0, scale: 1 };
let skyChartView = skyView; // the view skyChart was drawn with
let skyRedrawPending = false;

function setSkyView(cx, cy, scale) {
    scale = Math.min(SKY_MAX_ZOOM, Math.max(1, scale));
    // keep the centre on the sky disc, and centred at full view
    const limit = 100 - 100 / scale;
    skyView = {
        cx: Math.min(limit, Math.max(-limit, cx)),
        cy: Math.min(limit, Math.max(-limit, cy)),
        scale,
    };
    // at full view one-finger drag scrolls the page; zoomed in, it moves the map
    const container = document.getElementById("sky-chart");
    if (container) container.style.touchAction = scale > 1 ? "none" : "pan-y";
    redrawSky();
}

function zoomSkyAt(point, factor) {
    // zoom so the sky point under the cursor stays under the cursor
    const scale = Math.min(SKY_MAX_ZOOM, Math.max(1, skyView.scale * factor));
    const keep = skyView.scale / scale;
    setSkyView(point.x - (point.x - skyView.cx) * keep, point.y - (point.y - skyView.cy) * keep, scale);
}

function redrawSky() {
    // at most one redraw per frame while zooming or dragging
    if (skyRedrawPending || !skyData) return;
    skyRedrawPending = true;
    requestAnimationFrame(() => {
        skyRedrawPending = false;
        plotSkyProjection();
    });
}

function clientToSky(clientX, clientY, view = skyView) {
    // Screen position to plot units in the given view. The drawn plot can be
    // one frame behind skyView (redraws wait for the next frame), so convert
    // with its scales, then move from the drawn view to the given view.
    const rect = skyChart.getBoundingClientRect();
    const drawn = skyChartView;
    const keep = drawn.scale / view.scale;
    return {
        x: view.cx + (skyChart.scale("x").invert(clientX - rect.left) - drawn.cx) * keep,
        y: view.cy + (skyChart.scale("y").invert(clientY - rect.top) - drawn.cy) * keep,
    };
}

/**
 * Convert RA/Dec (J2000) to Alt/Az for current time and location
 * @param {number} ra - Right ascension in degrees
 * @param {number} dec - Declination in degrees
 * @param {number} lat - Observatory latitude in degrees
 * @param {number} lon - Observatory longitude in degrees
 * @param {Date} datetime - Current datetime
 * @returns {{alt: number, az: number}} Altitude and azimuth in degrees
 */
function convertRaDecToAltAz(ra, dec, lat, lon, datetime) {
    // Convert to radians
    const raRad = (ra * Math.PI) / 180;
    const decRad = (dec * Math.PI) / 180;
    const latRad = (lat * Math.PI) / 180;
    // const lonRad = (lon * Math.PI) / 180;

    // Calculate Local Sidereal Time
    const jd = datetime.getTime() / 86400000 + 2440587.5; // Julian Date
    const T = (jd - 2451545.0) / 36525.0; // Julian centuries since J2000
    const gmst =
        280.46061837 +
        360.98564736629 * (jd - 2451545.0) +
        0.000387933 * T * T -
        (T * T * T) / 38710000.0;
    const lst = (gmst + lon) % 360; // Local Sidereal Time in degrees
    const lstRad = (lst * Math.PI) / 180;

    // Calculate Hour Angle
    const ha = lstRad - raRad;

    // Convert to Alt/Az
    const sinAlt =
        Math.sin(decRad) * Math.sin(latRad) +
        Math.cos(decRad) * Math.cos(latRad) * Math.cos(ha);
    const alt = Math.asin(sinAlt);

    // Azimuth calculation: measured from North (0°) through East (90°)
    const sinAz = -Math.sin(ha) * Math.cos(decRad);
    const cosAz =
        Math.cos(latRad) * Math.sin(decRad) -
        Math.sin(latRad) * Math.cos(decRad) * Math.cos(ha);
    let az = Math.atan2(sinAz, cosAz);

    // Convert to degrees and ensure positive azimuth
    const altDeg = (alt * 180) / Math.PI;
    let azDeg = (az * 180) / Math.PI;
    if (azDeg < 0) azDeg += 360;

    return { alt: altDeg, az: azDeg };
}

/**
 * Convert Alt/Az to plot coordinates (azimuthal equidistant projection)
 * @param {number} alt - Altitude in degrees
 * @param {number} az - Azimuth in degrees
 * @returns {{x: number, y: number, radius: number}} Plot coordinates
 */
function altAzToXY(alt, az) {
    // Zenith at center, horizon at edge
    // North up and East left, as seen when looking up at the sky (like an
    // all-sky camera)
    const radius = 90 - alt; // 0° at center (zenith), 90° at edge (horizon)
    const azRad = (az * Math.PI) / 180;

    const x = -radius * Math.sin(azRad);
    const y = radius * Math.cos(azRad);

    return { x, y, radius };
}

/**
 * Plot the all-sky projection using Observable Plot
 */
function plotSkyProjection() {
    if (!skyData) return;

    const container = document.getElementById("sky-chart");
    if (!container) return;

    const obs = skyData.observatory;
    const datetime = new Date(skyData.utc_time + "Z"); // Ensure UTC

    // Calculate star positions
    const stars = STAR_CATALOG.map(([name, ra, dec, mag]) => {
        const { alt, az } = convertRaDecToAltAz(
            ra,
            dec,
            obs.lat,
            obs.lon,
            datetime,
        );
        if (alt < 0) return null; // Below horizon

        const { x, y } = altAzToXY(alt, az);
        return { name, ra, dec, alt, az, x, y, mag, type: "star" };
    }).filter((s) => s !== null);

    // Calculate deep-sky object positions
    const dsos = DSO_CATALOG.filter(dsoIsShown)
        .map((dso) => {
            const { alt, az } = convertRaDecToAltAz(dso.ra, dso.dec, obs.lat, obs.lon, datetime);
            if (alt < 0) return null; // Below horizon

            const { x, y } = altAzToXY(alt, az);
            return { ...dso, alt, az, x, y, type: "dso", color: dsoClass(dso.dsoType).color };
        })
        .filter((d) => d !== null);

    // Calculate celestial body positions
    const celestialBodies = skyData.celestial_bodies
        .map((body) => {
            if (body.alt < 0) return null; // Below horizon

            const { x, y } = altAzToXY(body.alt, body.az);
            return { ...body, x, y };
        })
        .filter((b) => b !== null);

    // Calculate telescope positions and trajectories using RA/Dec for consistency
    const telescopeMarkers = telescopes
        .map((tel) => {
            if (tel.ra == null || tel.dec == null) return null;

            // Use current browser time for telescope calculations (not cached sky data time)
            const currentTime = new Date();

            // Calculate Alt/Az from RA/Dec
            const { alt, az } = convertRaDecToAltAz(
                tel.ra,
                tel.dec,
                obs.lat,
                obs.lon,
                currentTime,
            );

            // Only show if above horizon
            if (alt < 0) return null;

            const { x, y } = altAzToXY(alt, az);
            return { ...tel, alt, az, x, y, type: "telescope" };
        })
        .filter((t) => t !== null);

    // Calculate telescope trajectories (24 hours into future)
    const telescopeTrajectories = telescopes
        .map((tel) => {
            if (tel.ra == null || tel.dec == null || !tel.tracking) return null;

            // Use current browser time for trajectory calculations
            const currentTime = new Date();
            const points = [];
            const numPoints = 96; // One point every 15 minutes

            // Add current position as first point calculated from RA/Dec
            const currentPos = convertRaDecToAltAz(
                tel.ra,
                tel.dec,
                obs.lat,
                obs.lon,
                currentTime,
            );
            if (currentPos.alt >= 0) {
                const { x, y } = altAzToXY(currentPos.alt, currentPos.az);
                points.push({
                    x,
                    y,
                    time: currentTime,
                    alt: currentPos.alt,
                    az: currentPos.az,
                    opacity: 1.0,
                });
            }

            for (let i = 1; i <= numPoints; i++) {
                // Calculate time offset in milliseconds (24 hours = 86400000 ms)
                const timeOffset = ((i / numPoints) * 86400000) / 2;
                const futureTime = new Date(currentTime.getTime() + timeOffset);

                // Convert RA/Dec to Alt/Az at future time
                const { alt, az } = convertRaDecToAltAz(
                    tel.ra,
                    tel.dec,
                    obs.lat,
                    obs.lon,
                    futureTime,
                );

                // Calculate opacity that fades from 1 to 0
                const opacity = (1 - i / numPoints) * 0.5;

                // Only include points above horizon
                if (alt >= 0) {
                    const { x, y } = altAzToXY(alt, az);
                    points.push({ x, y, time: futureTime, alt, az, opacity });
                } else {
                    points.push("NaN"); // Break in trajectory
                }
            }

            return points.length > 1 ? { name: tel.name, points } : null;
        })
        .filter((t) => t !== null);

    const width = document.getElementById(`content`).clientWidth;
    const size = Math.max(width, 320);

    // Visible part of the sky, from the zoom and pan state
    const half = 100 / skyView.scale;
    const pxPerDegree = (size - 40) / (2 * half); // about, the plot has small margins
    const zoomedIn = skyView.scale >= SKY_LABEL_ZOOM;

    // Draw only what is in view (with a margin for large objects), so a zoomed
    // view with many faint objects stays fast
    const inView = (d) => {
        const margin = half * 0.1 + (d.size ? d.size / 60 : 0);
        return Math.abs(d.x - skyView.cx) <= half + margin && Math.abs(d.y - skyView.cy) <= half + margin;
    };
    const visibleStars = stars.filter(inView);
    const visibleDsos = dsos.filter(inView);

    // All objects for hover, right-click and long-press
    const allObjects = [...visibleStars, ...visibleDsos, ...celestialBodies, ...telescopeMarkers];

    // Grid when zoomed in: altitude every 10° and azimuth every 30°
    const gridAltitudes = skyView.scale >= 2 ? [10, 20, 40, 50, 70, 80] : [];
    const gridAzimuths = skyView.scale >= 2 ? Array.from({ length: 12 }, (_, i) => i * 30) : [];

    // Ring size in pixels: the real size of the object when that is bigger
    const dsoRadius = (d) =>
        Math.min(400, Math.max(d.messier ? 3.5 : 2, d.size ? ((d.size / 60) * pxPerDegree) / 2 : 0));

    // Create the plot
    const plot = Plot.plot({
        width: size,
        height: size,
        // marginTop: 20,
        // marginBottom: 20,
        // marginLeft: 20,
        // marginRight: 20,
        x: { domain: [skyView.cx - half, skyView.cx + half], axis: null },
        y: { domain: [skyView.cy - half, skyView.cy + half], axis: null },
        r: { type: "identity" }, // radius values are pixels
        clip: true, // hide marks outside the visible part when zoomed in
        style: {
            backgroundColor: "transparent",
            color: "#9ca3af", // gray-400
            fontFamily: "system-ui, sans-serif",
            fontSize: "12px",
            overflow: "visible",
        },
        marks: [
            // Horizon circle (0° altitude)
            Plot.line(
                Array.from({ length: 361 }, (_, i) => {
                    const angle = (i * Math.PI) / 180;
                    return { x: 90 * Math.cos(angle), y: 90 * Math.sin(angle) };
                }),
                {
                    x: "x",
                    y: "y",
                    stroke: "#4b5563", // gray-600
                    strokeWidth: 1,
                    strokeOpacity: 0.5,
                },
            ),

            // 30° and 60° altitude circles
            Plot.line(
                Array.from({ length: 361 }, (_, i) => {
                    const angle = (i * Math.PI) / 180;
                    return { x: 60 * Math.cos(angle), y: 60 * Math.sin(angle) };
                }),
                {
                    x: "x",
                    y: "y",
                    stroke: "#4b5563",
                    strokeWidth: 1,
                    strokeDasharray: "4,4",
                    strokeOpacity: 0.5,
                },
            ),
            Plot.line(
                Array.from({ length: 361 }, (_, i) => {
                    const angle = (i * Math.PI) / 180;
                    return { x: 30 * Math.cos(angle), y: 30 * Math.sin(angle) };
                }),
                {
                    x: "x",
                    y: "y",
                    stroke: "#4b5563",
                    strokeWidth: 1,
                    strokeDasharray: "4,4",
                    strokeOpacity: 0.5,
                },
            ),

            // Grid when zoomed in, to keep your bearings
            ...gridAltitudes.map((altitude) =>
                Plot.line(
                    Array.from({ length: 361 }, (_, i) => {
                        const angle = (i * Math.PI) / 180;
                        const r = 90 - altitude;
                        return { x: r * Math.cos(angle), y: r * Math.sin(angle) };
                    }),
                    { x: "x", y: "y", stroke: "#374151", strokeWidth: 1, strokeOpacity: 0.4 }, // gray-700
                ),
            ),
            ...gridAzimuths.map((azimuth) =>
                Plot.line([altAzToXY(90, azimuth), altAzToXY(0, azimuth)], {
                    x: "x",
                    y: "y",
                    stroke: "#374151",
                    strokeWidth: 1,
                    strokeOpacity: 0.4,
                }),
            ),
            // Grid labels: altitude along the line to the zenith, azimuth at the horizon
            Plot.text(
                gridAltitudes.map((altitude) => ({ ...altAzToXY(altitude, 0), label: `${altitude}°` })),
                { x: "x", y: "y", text: "label", fill: "#4b5563", fontSize: 10, dx: 12 },
            ),
            Plot.text(
                gridAzimuths.map((azimuth) => ({ ...altAzToXY(2, azimuth), label: `az ${azimuth}°` })),
                { x: "x", y: "y", text: "label", fill: "#4b5563", fontSize: 10 },
            ),

            // Cardinal direction labels (N, E, S, W)
            ...["N", "E", "S", "W"].map((dir, i) => {
                const azimuth = i * 90;
                const { x, y } = altAzToXY(-5, azimuth);
                return Plot.text([{ x, y, label: dir }], {
                    x: "x",
                    y: "y",
                    text: "label",
                    fontSize: 16,
                    fontWeight: "600",
                    fill: dir === "N" ? "#f87171" : "#9ca3af", // red-400 for North, gray-400 for others
                });
            }),

            // Stars - simple and clean
            Plot.dot(visibleStars, {
                x: "x",
                y: "y",
                r: 2,
                fill: (d) =>
                    `rgba(255, 255, 255, ${Math.min(1, Math.pow(10, -0.4 * d.mag))})`,
            }),

            // Deep-sky objects - open rings, coloured by type, bigger for Messier,
            // and at their real size when zoomed in far enough
            Plot.dot(visibleDsos, {
                x: "x",
                y: "y",
                r: dsoRadius,
                stroke: "color",
                strokeWidth: 1,
                strokeOpacity: (d) => (d.messier ? 0.9 : 0.6),
                fill: "none",
            }),

            // Deep-sky labels: at full view only the brightest Messier objects,
            // all Messier from 2x zoom, NGC too from SKY_LABEL_ZOOM
            Plot.text(
                visibleDsos.filter((d) =>
                    d.messier ? skyView.scale >= 2 || (d.mag !== null && d.mag <= 6) : zoomedIn,
                ),
                {
                    x: "x",
                    y: "y",
                    text: "name",
                    dx: (d) => dsoRadius(d) + 3,
                    textAnchor: "start",
                    fill: "#6b7280", // gray-500
                    fontSize: zoomedIn ? 11 : 9,
                },
            ),

            // Star names when zoomed in: only proper names such as "Vega", not
            // catalog designations such as "20Gam Her" or "HR 5958"
            Plot.text(zoomedIn ? visibleStars.filter((d) => STAR_PROPER_NAME.test(d.name)) : [], {
                x: "x",
                y: "y",
                text: "name",
                dy: 10,
                fill: "#4b5563", // gray-600
                fontSize: 10,
                fontStyle: "italic",
            }),

            // Sun
            Plot.dot(
                celestialBodies.filter((b) => b.type === "sun"),
                {
                    x: "x",
                    y: "y",
                    r: 8,
                    fill: "#fbbf24", // amber-400
                    stroke: "#f59e0b", // amber-500
                    strokeWidth: 2,
                    opacity: 0.9,
                },
            ),

            // Moon
            Plot.dot(
                celestialBodies.filter((b) => b.type === "moon"),
                {
                    x: "x",
                    y: "y",
                    r: 8,
                    fill: "#e5e7eb", // gray-200
                    stroke: "#9ca3af", // gray-400
                    strokeWidth: 1,
                    opacity: "phase",
                },
            ),

            // Planets
            Plot.dot(
                celestialBodies.filter((b) => b.type === "planet"),
                {
                    x: "x",
                    y: "y",
                    r: 4,
                    fill: (d) => {
                        const colors = {
                            Mercury: "#d1d5db", // gray-300
                            Venus: "#fef3c7", // amber-100
                            Mars: "#fca5a5", // red-300
                            Jupiter: "#fed7aa", // orange-200
                            Saturn: "#fde68a", // amber-200
                            Uranus: "#bae6fd", // sky-200
                            Neptune: "#a5b4fc", // indigo-200
                        };
                        return colors[d.name] || "#c4b5fd";
                    },
                    stroke: "transparent",
                    opacity: 0.6,
                },
            ),

            // Telescope trajectories - subtle dashed line
            ...telescopeTrajectories.map((traj) => {
                return Plot.line(traj.points, {
                    x: "x",
                    y: "y",
                    stroke: "#fed7aa",
                    strokeWidth: 1,
                    strokeDasharray: "3,3",
                    opacity: "opacity",
                });
            }),

            // Telescopes - minimalist crosshair
            ...telescopeMarkers.flatMap((tel) => {
                return [
                    Plot.line(
                        [
                            { x: tel.x - 5, y: tel.y },
                            { x: tel.x + 5, y: tel.y },
                        ],
                        { x: "x", y: "y", stroke: "#fed7aa", strokeWidth: 1.5 },
                    ),
                    Plot.line(
                        [
                            { x: tel.x, y: tel.y - 5 },
                            { x: tel.x, y: tel.y + 5 },
                        ],
                        { x: "x", y: "y", stroke: "#fed7aa", strokeWidth: 1.5 },
                    ),
                    Plot.text([tel], {
                        x: "x",
                        y: "y",
                        text: (d) => d.name,
                        dy: -10,
                        fill: "#fed7aa",
                        fontSize: 11,
                        fontWeight: "500",
                        stroke: "#000000",
                        strokeWidth: 2,
                    }),
                ];
            }),

            // Hover highlight
            Plot.dot(
                allObjects,
                Plot.pointer({
                    x: "x",
                    y: "y",
                    r: 8,
                    stroke: "#fbbf24", // amber-400
                    strokeWidth: 1.5,
                    fill: "none",
                }),
            ),

            // Hover: the name, next to the object, and where it is in the sky
            Plot.tip(
                allObjects,
                Plot.pointer({
                    x: "x",
                    y: "y",
                    title: (d) => {
                        const name = [d.name, d.commonName].filter((n) => n).join(" · ");
                        const position = [`alt ${d.alt.toFixed(0)}°`, `az ${d.az.toFixed(0)}°`];
                        if (d.type === "moon" && d.phase != null) position.push(`${(100 * d.phase).toFixed(0)}% lit`);
                        return `${name}\n${position.join(" · ")}`;
                    },
                    fontSize: 11,
                    fill: "#111827", // gray-900, Plot's default is white
                    stroke: "#374151", // gray-700
                }),
            ),
        ],
    });

    // Keep the plot for its scales, used by zoom and pan
    skyChart = plot;
    skyChartView = skyView;

    // Objects that a right-click or long-press can copy, and a grab hand when
    // the map can be moved
    skyPlottedObjects = allObjects.filter((d) => d.type !== "telescope");
    plot.style.cursor = skyView.scale > 1 ? "grab" : "";

    container.innerHTML = "";
    container.appendChild(plot);

    // Restore hover state using global mouse position
    if (skyChartMousePos.x !== null && skyChartMousePos.y !== null) {
        const newSvg = container.querySelector("svg");
        if (newSvg) {
            const pointermove = new PointerEvent("pointermove", {
                bubbles: true,
                pointerType: skyChartMousePos.type || "mouse",
                clientX: skyChartMousePos.x,
                clientY: skyChartMousePos.y,
            });
            newSvg.dispatchEvent(pointermove);
        }
    }
}

/**
 * Copy text to the clipboard. navigator.clipboard only works on https or
 * localhost, so fall back to a hidden text area on plain http.
 * @returns {Promise<boolean>} true if the text was copied
 */
async function copyText(text) {
    try {
        if (navigator.clipboard && window.isSecureContext) {
            await navigator.clipboard.writeText(text);
            return true;
        }
    } catch (error) {
        // fall back below
    }

    const textArea = document.createElement("textarea");
    textArea.value = text;
    textArea.setAttribute("readonly", "");
    textArea.style.position = "fixed";
    textArea.style.opacity = "0";
    textArea.style.userSelect = "text";
    textArea.style.webkitUserSelect = "text";
    document.body.appendChild(textArea);
    textArea.select();
    let copied = false;
    try {
        copied = document.execCommand("copy");
    } catch (error) {
        copied = false;
    }
    textArea.remove();
    return copied;
}

/**
 * The object nearest to a screen position, if one is within maxDistance pixels
 */
function objectAt(clientX, clientY, maxDistance) {
    if (!skyChart) return null;
    const rect = skyChart.getBoundingClientRect();
    const xScale = skyChart.scale("x");
    const yScale = skyChart.scale("y");
    let nearest = null;
    let nearestDistance = maxDistance;
    for (const d of skyPlottedObjects) {
        const distance = Math.hypot(rect.left + xScale.apply(d.x) - clientX, rect.top + yScale.apply(d.y) - clientY);
        if (distance <= nearestDistance) {
            nearest = d;
            nearestDistance = distance;
        }
    }
    return nearest;
}

/**
 * Short message at the bottom of the sky chart, e.g. "Copied M13"
 */
let skyToastTimer = null;
function showSkyToast(text, ok = true, duration = 1500) {
    const toast = document.getElementById("sky-toast");
    if (!toast) return;
    toast.textContent = text;
    toast.classList.toggle("text-green-300", ok);
    toast.classList.toggle("text-red-300", !ok);
    toast.hidden = false;
    clearTimeout(skyToastTimer);
    if (duration) skyToastTimer = setTimeout(() => (toast.hidden = true), duration);
}

/**
 * Copy the first name of an object: the Messier name if it has one,
 * otherwise its NGC, star or planet name
 */
async function copyObjectName(d) {
    const copied = await copyText(d.name);
    showSkyToast(copied ? `Copied ${d.name}` : `Could not copy ${d.name}`, copied);
}

/**
 * Fetch celestial data from backend API
 */
async function updateSkyChart() {
    try {
        const response = await fetch("/api/sky_data");
        const result = await response.json();

        if (result.status === "success") {
            skyData = result.data;
            plotSkyProjection();
        } else {
            console.error("Error fetching sky data:", result.message);
        }
    } catch (error) {
        console.error("Error updating sky chart:", error);
    }
}

/**
 * Update telescope positions from websocket data
 * @param {Array} newTelescopeData - Array of telescope objects with name, alt, az
 */
function updateTelescopePositions(newTelescopeData) {
    const nextTelescopes = newTelescopeData || [];

    // Check if data has changed to avoid unnecessary replots
    if (JSON.stringify(telescopes) !== JSON.stringify(nextTelescopes)) {
        telescopes = nextTelescopes;
        // Only redraw if we have sky data (don't wait for next celestial update)
        if (skyData) {
            plotSkyProjection();
        }
    }
}

/**
 * Initialize the sky projection chart
 */
function initializeSkyChart() {
    // Legend, from the same colours the map uses
    const legend = document.getElementById("sky-legend");
    if (legend) {
        for (const { label, color } of Object.values(DSO_CLASSES)) {
            const item = document.createElement("span");
            const ring = document.createElement("span");
            ring.style.color = color;
            ring.textContent = "○";
            item.append(ring, ` ${label}`);
            legend.appendChild(item);
        }
    }

    // Initial update
    updateSkyChart();

    // Update celestial data every 60 seconds
    if (updateInterval) {
        clearInterval(updateInterval);
    }
    updateInterval = setInterval(updateSkyChart, 60000);

    // Redraw on window resize
    window.addEventListener("resize", () => {
        if (skyData) {
            plotSkyProjection();
        }
    });

    // Add event listeners to container for persistent hover
    const container = document.getElementById("sky-chart");
    if (container) {
        const updateMousePos = (event) => {
            skyChartMousePos = {
                x: event.clientX,
                y: event.clientY,
                type: event.pointerType,
            };
        };

        container.addEventListener("pointermove", updateMousePos);

        container.style.touchAction = "pan-y";

        // Zoom with trackpad pinch or Ctrl + scroll (both send wheel events with
        // ctrlKey), at the cursor position. A plain scroll only zooms when the
        // map is already zoomed in, so at full view it still scrolls the page.
        container.addEventListener(
            "wheel",
            (event) => {
                if (!skyChart) return;
                if (!event.ctrlKey && !event.metaKey && skyView.scale === 1) return;
                event.preventDefault();
                // limit each step, as one mouse wheel notch can send a large delta
                const delta = Math.max(-50, Math.min(50, event.deltaY));
                const factor = Math.exp(-delta * (event.ctrlKey ? 0.01 : 0.004));
                zoomSkyAt(clientToSky(event.clientX, event.clientY), factor);
            },
            { passive: false },
        );

        // Double-click to zoom in at that point
        container.addEventListener("dblclick", (event) => {
            if (!skyChart) return;
            zoomSkyAt(clientToSky(event.clientX, event.clientY), 2);
        });

        // Drag to move the map, pinch with two fingers to zoom
        const pointers = new Map(); // pointerId -> last { x, y }
        let dragStart = null;
        let dragMoved = false;
        let pinch = null; // at the start of a pinch: { distance, view, point }

        // Long-press on touch screens copies the name of the object under the
        // finger. Browsers only allow the clipboard right after the user acts,
        // and on touch that is when the finger lifts, so the copy happens on
        // release: after LONG_PRESS_MS the map says "Release to copy ...".
        const LONG_PRESS_MS = 500;
        let longPress = null; // { pointerId, start, object, ready, timer }
        const cancelLongPress = () => {
            if (!longPress) return;
            clearTimeout(longPress.timer);
            if (longPress.ready) document.getElementById("sky-toast").hidden = true;
            longPress = null;
        };
        const pinchState = () => {
            const [a, b] = [...pointers.values()];
            return {
                distance: Math.hypot(a.x - b.x, a.y - b.y),
                middle: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 },
            };
        };

        // Capture phase, so this runs before Plot's own pointerdown handler on
        // the chart. Over an object, Plot stops the event there (which would
        // block a drag) and makes its hover "sticky" (frozen on that object).
        // Mouse presses therefore do not go on to Plot. Hover still works, as
        // Plot uses pointermove for it; touch is not affected.
        container.addEventListener(
            "pointerdown",
            (event) => {
                updateMousePos(event);
                if (event.pointerType === "mouse") event.stopPropagation();
                if (event.pointerType === "mouse" && event.button !== 0) return;
                // the first finger (or the mouse) starts a new gesture, so drop
                // pointers whose pointerup never arrived
                if (event.isPrimary) pointers.clear();
                if (pointers.size === 0) {
                    dragStart = { x: event.clientX, y: event.clientY };
                    dragMoved = false;
                }
                pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
                if (pointers.size === 2) {
                    // Zoom from the state at the start of the pinch, not step by
                    // step, so small errors in each step do not add up
                    const { distance, middle } = pinchState();
                    pinch = { distance, view: skyView, point: clientToSky(middle.x, middle.y) };
                    // Each redraw replaces the SVG. A touch stays on the element it
                    // started on, so without capture, the moves of a finger that
                    // started on the old SVG no longer reach the container.
                    for (const id of pointers.keys()) {
                        if (!container.hasPointerCapture(id)) container.setPointerCapture(id);
                    }
                }

                cancelLongPress();
                if (event.pointerType !== "mouse" && pointers.size === 1) {
                    const object = objectAt(event.clientX, event.clientY, 30);
                    if (object) {
                        longPress = { pointerId: event.pointerId, start: dragStart, object, ready: false };
                        longPress.timer = setTimeout(() => {
                            longPress.ready = true;
                            if (navigator.vibrate) navigator.vibrate(15);
                            showSkyToast(`Release to copy ${object.name}`, true, 0);
                        }, LONG_PRESS_MS);
                    }
                }
            },
            { capture: true },
        );

        container.addEventListener("pointermove", (event) => {
            // Not the event that a redraw sends to restore the hover. It has
            // pointerId 0, which can be the id of a real finger.
            if (!event.isTrusted) return;
            const last = pointers.get(event.pointerId);
            if (!last || !skyChart) return;
            const now = { x: event.clientX, y: event.clientY };

            // moving means it is not a long-press (a second finger cancels it in pointerdown)
            if (longPress && Math.hypot(now.x - longPress.start.x, now.y - longPress.start.y) > 8) {
                cancelLongPress();
            }

            if (pointers.size === 2) {
                pointers.set(event.pointerId, now);
                // keep the sky point that was between the fingers between them
                const { distance, middle } = pinchState();
                const scale = Math.min(SKY_MAX_ZOOM, Math.max(1, (pinch.view.scale * distance) / pinch.distance));
                const offset = clientToSky(middle.x, middle.y, { cx: 0, cy: 0, scale });
                setSkyView(pinch.point.x - offset.x, pinch.point.y - offset.y, scale);
                dragMoved = true;
                return;
            }

            // a small move is still a click; nothing to move at full view
            if (!dragMoved && Math.hypot(now.x - dragStart.x, now.y - dragStart.y) < 5) return;
            if (skyView.scale === 1) return;
            if (!dragMoved) {
                dragMoved = true;
                container.setPointerCapture(event.pointerId);
                container.style.cursor = "grabbing";
            }

            const from = clientToSky(last.x, last.y);
            const to = clientToSky(now.x, now.y);
            pointers.set(event.pointerId, now);
            setSkyView(skyView.cx - (to.x - from.x), skyView.cy - (to.y - from.y), skyView.scale);
        });

        // At full view, touch-action is pan-y so one finger scrolls the page.
        // The browser can then take a two-finger gesture as a scroll (or zoom
        // the whole page) and cancel our pointers. Block that for two fingers.
        container.addEventListener(
            "touchmove",
            (event) => {
                if (event.touches.length > 1 && event.cancelable) event.preventDefault();
            },
            { passive: false },
        );

        const endPointer = (event) => {
            pointers.delete(event.pointerId);
            if (pointers.size < 2) pinch = null;
            if (pointers.size === 0) container.style.cursor = "";
        };
        container.addEventListener("pointerup", (event) => {
            // finger lifted after a long-press: copy now, while the browser allows it
            if (longPress && longPress.ready && longPress.pointerId === event.pointerId) {
                copyObjectName(longPress.object);
                clearTimeout(longPress.timer);
                longPress = null;
            } else {
                cancelLongPress();
            }
            endPointer(event);
        });
        container.addEventListener("pointercancel", (event) => {
            cancelLongPress();
            endPointer(event);
        });

        // Right-click an object to copy its name; elsewhere the browser menu
        // opens as usual. On touch the long-press above copies, so the
        // browser's own long-press menu is blocked.
        container.addEventListener("contextmenu", (event) => {
            if (longPress || event.pointerType === "touch") {
                event.preventDefault();
                return;
            }
            const object = objectAt(event.clientX, event.clientY, 20);
            if (object) {
                event.preventDefault();
                copyObjectName(object);
            }
        });

        container.addEventListener("pointerleave", () => {
            skyChartMousePos = { x: null, y: null, type: null };
        });
    }
}

// Export functions for use in main page
if (typeof window !== "undefined") {
    window.initializeSkyChart = initializeSkyChart;
    window.updateTelescopePositions = updateTelescopePositions;
    window.skyZoomIn = () => zoomSkyAt({ x: skyView.cx, y: skyView.cy }, 2);
    window.skyZoomOut = () => zoomSkyAt({ x: skyView.cx, y: skyView.cy }, 0.5);
    window.skyZoomReset = () => setSkyView(0, 0, 1);
}
