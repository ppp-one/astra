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
// Global variables
let skyChart = null;
let skyData = null;
let telescopes = [];
let updateInterval = null;
let skyChartMousePos = { x: null, y: null, type: "mouse" };

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
    const lonRad = (lon * Math.PI) / 180;

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
    const radius = 90 - alt; // 0° at center (zenith), 90° at edge (horizon)
    const azRad = ((az - 90) * Math.PI) / 180; // Rotate so North is up (az=0° → -90°)

    const x = radius * Math.cos(azRad);
    const y = radius * Math.sin(azRad);

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
    console.log(
        `Plotting sky projection for ${datetime.toISOString()} at lat=${obs.lat}, lon=${obs.lon}`,
    );

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
            if (!tel.ra || !tel.dec) return null;

            // Use current browser time for telescope calculations (not cached sky data time)
            const currentTime = new Date();
            console.log(
                `Telescope ${tel.name}: RA=${tel.ra}, Dec=${tel.dec} at ${currentTime.toISOString()}`,
            );

            // Calculate Alt/Az from RA/Dec
            const { alt, az } = convertRaDecToAltAz(
                tel.ra,
                tel.dec,
                obs.lat,
                obs.lon,
                currentTime,
            );
            console.log(
                `Telescope ${tel.name}: Alt=${alt.toFixed(2)}, Az=${az.toFixed(2)}`,
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
            if (!tel.ra || !tel.dec || !tel.tracking) return null;

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

    // All objects for plotting
    const allObjects = [...stars, ...celestialBodies, ...telescopeMarkers];
    const width = document.getElementById(`content`).clientWidth;
    const size = Math.max(width, 320);

    // Create the plot
    const plot = Plot.plot({
        width: size,
        height: size,
        // marginTop: 20,
        // marginBottom: 20,
        // marginLeft: 20,
        // marginRight: 20,
        x: { domain: [-100, 100], axis: null },
        y: { domain: [-100, 100], axis: null },
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
            Plot.dot(stars, {
                x: "x",
                y: "y",
                r: 2,
                fill: (d) =>
                    `rgba(255, 255, 255, ${Math.min(1, Math.pow(10, -0.4 * d.mag))})`,
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

            // Tooltip - Top Left
            Plot.text(
                allObjects,
                Plot.pointer({
                    px: "x",
                    py: "y",
                    x: null,
                    y: null,
                    frameAnchor: "top-left",
                    dx: 10,
                    dy: 10,
                    fontVariant: "tabular-nums",
                    text: (d) => {
                        const extras =
                            d.type === "moon" && d.phase != null
                                ? `, phase: ${(100 * d.phase).toFixed(1)}%`
                                : "";
                        return `${d.name} (alt: ${d.alt.toFixed(1)}° az: ${d.az.toFixed(1)}°${extras})`;
                    },
                    fill: "#f3f4f6", // gray-100
                    fontSize: 10,
                    fontWeight: "500",
                    stroke: "#111827", // gray-900
                    strokeWidth: 3,
                    paintOrder: "stroke",
                }),
            ),
        ],
    });

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
        container.addEventListener("pointerdown", updateMousePos);

        container.addEventListener("pointerleave", () => {
            skyChartMousePos = { x: null, y: null, type: null };
        });
    }
}

// Export functions for use in main page
if (typeof window !== "undefined") {
    window.initializeSkyChart = initializeSkyChart;
    window.updateTelescopePositions = updateTelescopePositions;
}
