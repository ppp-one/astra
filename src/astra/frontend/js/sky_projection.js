/**
 * All-Sky Projection Visualization
 * 
 * Displays a real-time circular all-sky projection with zenith at center and horizon at edge.
 * Shows brightest stars, planets, sun, moon, and telescope positions.
 * Uses azimuthal equidistant projection: radius = 90° - altitude, angle = azimuth
 */

// Hardcoded catalog of ~50 brightest stars (J2000 coordinates)
// Format: [name, RA (degrees), Dec (degrees), magnitude]
const STAR_CATALOG = [
    ["Sirius", 101.287, -16.716, -1.46],
    ["Canopus", 95.988, -52.696, -0.72],
    ["Arcturus", 213.915, 19.182, -0.04],
    ["Alpha Centauri", 219.902, -60.834, -0.01],
    ["Vega", 279.234, 38.784, 0.03],
    ["Capella", 79.172, 45.998, 0.08],
    ["Rigel", 78.634, -8.202, 0.12],
    ["Procyon", 114.825, 5.225, 0.38],
    ["Achernar", 24.429, -57.237, 0.46],
    ["Betelgeuse", 88.793, 7.407, 0.50],
    ["Hadar", 210.956, -60.373, 0.61],
    ["Altair", 297.696, 8.868, 0.77],
    ["Aldebaran", 68.980, 16.509, 0.85],
    ["Antares", 247.352, -26.432, 0.96],
    ["Spica", 201.298, -11.161, 1.04],
    ["Pollux", 116.329, 28.026, 1.14],
    ["Fomalhaut", 344.413, -29.622, 1.16],
    ["Deneb", 310.358, 45.280, 1.25],
    ["Mimosa", 191.930, -59.689, 1.25],
    ["Regulus", 152.093, 11.967, 1.35],
    ["Adhara", 104.656, -28.972, 1.50],
    ["Castor", 113.650, 31.888, 1.57],
    ["Shaula", 263.402, -37.104, 1.63],
    ["Bellatrix", 81.283, 6.350, 1.64],
    ["Elnath", 84.411, 28.608, 1.65],
    ["Miaplacidus", 138.300, -69.717, 1.68],
    ["Alnilam", 84.053, -1.202, 1.69],
    ["Alnitak", 85.190, -1.943, 1.70],
    ["Alnair", 332.058, -46.961, 1.74],
    ["Alioth", 193.507, 55.960, 1.77],
    ["Kaus Australis", 276.043, -34.385, 1.85],
    ["Mirfak", 51.080, 49.861, 1.79],
    ["Dubhe", 165.932, 61.751, 1.79],
    ["Wezen", 107.098, -26.393, 1.84],
    ["Alkaid", 206.885, 49.313, 1.86],
    ["Sargas", 264.330, -42.998, 1.87],
    ["Avior", 125.628, -59.509, 1.86],
    ["Menkalinan", 89.882, 44.947, 1.90],
    ["Atria", 253.083, -69.028, 1.92],
    ["Alhena", 99.428, 16.399, 1.93],
    ["Peacock", 306.412, -56.735, 1.94],
    ["Polaris", 37.955, 89.264, 1.98],
    ["Mirzam", 95.674, -17.956, 1.98],
    ["Alphard", 141.897, -8.659, 1.98],
    ["Hamal", 31.793, 23.462, 2.00],
    ["Algieba", 154.993, 19.842, 2.08],
    ["Diphda", 10.897, -17.987, 2.04],
    ["Nunki", 283.816, -26.297, 2.02],
    ["Mizar", 200.981, 54.925, 2.27],
    ["Kochab", 222.676, 74.155, 2.08]
];

// Global variables
let skyChart = null;
let skyData = null;
let telescopes = [];
let updateInterval = null;

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
    const raRad = ra * Math.PI / 180;
    const decRad = dec * Math.PI / 180;
    const latRad = lat * Math.PI / 180;
    const lonRad = lon * Math.PI / 180;

    // Calculate Local Sidereal Time
    const jd = datetime.getTime() / 86400000 + 2440587.5; // Julian Date
    const T = (jd - 2451545.0) / 36525.0; // Julian centuries since J2000
    const gmst = 280.46061837 + 360.98564736629 * (jd - 2451545.0) + 0.000387933 * T * T - (T * T * T) / 38710000.0;
    const lst = (gmst + lon) % 360; // Local Sidereal Time in degrees
    const lstRad = lst * Math.PI / 180;

    // Calculate Hour Angle
    const ha = lstRad - raRad;

    // Convert to Alt/Az
    const sinAlt = Math.sin(decRad) * Math.sin(latRad) + Math.cos(decRad) * Math.cos(latRad) * Math.cos(ha);
    const alt = Math.asin(sinAlt);

    // Azimuth calculation: measured from North (0°) through East (90°)
    const sinAz = -Math.sin(ha) * Math.cos(decRad);
    const cosAz = Math.cos(latRad) * Math.sin(decRad) - Math.sin(latRad) * Math.cos(decRad) * Math.cos(ha);
    let az = Math.atan2(sinAz, cosAz);

    // Convert to degrees and ensure positive azimuth
    const altDeg = alt * 180 / Math.PI;
    let azDeg = az * 180 / Math.PI;
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
    const azRad = (az - 90) * Math.PI / 180; // Rotate so North is up (az=0° → -90°)

    const x = radius * Math.cos(azRad);
    const y = radius * Math.sin(azRad);

    return { x, y, radius };
}

/**
 * Plot the all-sky projection using Observable Plot
 */
function plotSkyProjection() {
    if (!skyData) return;

    const container = document.getElementById('sky-chart');
    if (!container) return;

    const obs = skyData.observatory;
    const datetime = new Date(skyData.utc_time);

    // Calculate star positions
    const stars = STAR_CATALOG.map(([name, ra, dec, mag]) => {
        const { alt, az } = convertRaDecToAltAz(ra, dec, obs.lat, obs.lon, datetime);
        if (alt < 0) return null; // Below horizon

        const { x, y } = altAzToXY(alt, az);
        return { name, ra, dec, alt, az, x, y, mag, type: 'star' };
    }).filter(s => s !== null);

    // Calculate celestial body positions
    const celestialBodies = skyData.celestial_bodies.map(body => {
        if (body.alt < 0) return null; // Below horizon

        const { x, y } = altAzToXY(body.alt, body.az);
        return { ...body, x, y };
    }).filter(b => b !== null);

    // Calculate telescope positions and trajectories using RA/Dec for consistency
    const telescopeMarkers = telescopes.map(tel => {
        if (!tel.ra || !tel.dec) return null;

        // Use current browser time for telescope calculations (not cached sky data time)
        const currentTime = new Date();

        // Calculate Alt/Az from RA/Dec
        const { alt, az } = convertRaDecToAltAz(tel.ra, tel.dec, obs.lat, obs.lon, currentTime);

        // Only show if above horizon
        if (alt < 0) return null;

        const { x, y } = altAzToXY(alt, az);
        return { ...tel, alt, az, x, y, type: 'telescope' };
    }).filter(t => t !== null);

    // Calculate telescope trajectories (24 hours into future)
    const telescopeTrajectories = telescopes.map(tel => {
        if (!tel.ra || !tel.dec) return null;

        // Use current browser time for trajectory calculations
        const currentTime = new Date();
        const points = [];
        const numPoints = 96; // One point every 15 minutes

        // Add current position as first point calculated from RA/Dec
        const currentPos = convertRaDecToAltAz(tel.ra, tel.dec, obs.lat, obs.lon, currentTime);
        if (currentPos.alt >= 0) {
            const { x, y } = altAzToXY(currentPos.alt, currentPos.az);
            points.push({ x, y, time: currentTime, alt: currentPos.alt, az: currentPos.az, opacity: 1.0 });
        }

        for (let i = 1; i <= numPoints; i++) {
            // Calculate time offset in milliseconds (24 hours = 86400000 ms)
            const timeOffset = (i / numPoints) * 86400000 / 2;
            const futureTime = new Date(currentTime.getTime() + timeOffset);

            // Convert RA/Dec to Alt/Az at future time
            const { alt, az } = convertRaDecToAltAz(tel.ra, tel.dec, obs.lat, obs.lon, futureTime);

            // Calculate opacity that fades from 1 to 0
            const opacity = (1 - (i / numPoints)) * 0.5;

            // Only include points above horizon
            if (alt >= 0) {
                const { x, y } = altAzToXY(alt, az);
                points.push({ x, y, time: futureTime, alt, az, opacity });
            } else {
                points.push("NaN"); // Break in trajectory
            }
        }

        return points.length > 1 ? { name: tel.name, points } : null;
    }).filter(t => t !== null);

    // All objects for plotting
    const allObjects = [...stars, ...celestialBodies, ...telescopeMarkers];

    const size = Math.min(window.innerWidth * 0.9, 600);

    // Create the plot
    const plot = Plot.plot({
        width: size,
        height: size,
        x: { domain: [-95, 95], axis: null },
        y: { domain: [-95, 95], axis: null },
        style: {
            backgroundColor: "transparent"
        },
        marks: [
            // Horizon circle (0° altitude, radius = 90)
            Plot.line(
                Array.from({ length: 361 }, (_, i) => {
                    const angle = (i * Math.PI) / 180;
                    return { x: 90 * Math.cos(angle), y: 90 * Math.sin(angle) };
                }),
                {
                    x: "x",
                    y: "y",
                    stroke: "rgb(100, 100, 120)",
                    strokeWidth: 2,
                    strokeDasharray: "5,3",
                    opacity: 0.5
                }
            ),

            // 30° altitude circle (radius = 60)
            Plot.line(
                Array.from({ length: 361 }, (_, i) => {
                    const angle = (i * Math.PI) / 180;
                    return { x: 60 * Math.cos(angle), y: 60 * Math.sin(angle) };
                }),
                {
                    x: "x",
                    y: "y",
                    stroke: "rgb(80, 90, 110)",
                    strokeWidth: 1.5,
                    strokeDasharray: "5,3",
                    opacity: 0.5
                }
            ),

            // 60° altitude circle (radius = 30)
            Plot.line(
                Array.from({ length: 361 }, (_, i) => {
                    const angle = (i * Math.PI) / 180;
                    return { x: 30 * Math.cos(angle), y: 30 * Math.sin(angle) };
                }),
                {
                    x: "x",
                    y: "y",
                    stroke: "rgb(80, 90, 110)",
                    strokeWidth: 1.5,
                    strokeDasharray: "5,3",
                    opacity: 0.5
                }
            ),

            // Altitude labels
            Plot.text([{ x: 0, y: 60, label: "30°" }], {
                x: "x",
                y: "y",
                text: "label",
                fontSize: 10,
                fill: "rgb(150, 150, 170)",
                fontStyle: "italic"
            }),
            Plot.text([{ x: 0, y: 30, label: "60°" }], {
                x: "x",
                y: "y",
                text: "label",
                fontSize: 10,
                fill: "rgb(150, 150, 170)",
                fontStyle: "italic"
            }),

            // Zenith marker
            Plot.dot([{ x: 0, y: 0 }], {
                r: 2,
                fill: "rgb(150, 150, 170)",
                stroke: "white",
                strokeWidth: 1
            }),

            // Cardinal direction lines
            ...['N', 'E', 'S', 'W'].map((dir, i) => {
                const angle = i * 90 - 90; // N=0°, E=90°, S=180°, W=270°, rotated -90° for plot
                const angleRad = angle * Math.PI / 180;
                return Plot.line(
                    [
                        { x: 0, y: 0 },
                        { x: 90 * Math.cos(angleRad), y: 90 * Math.sin(angleRad) }
                    ],
                    {
                        x: "x",
                        y: "y",
                        stroke: "rgb(80, 80, 100)",
                        strokeWidth: 0.5,
                        opacity: 0.3
                    }
                );
            }),

            // Cardinal direction labels
            ...['N', 'E', 'S', 'W'].map((dir, i) => {
                const azimuth = i * 90; // N=0°, E=90°, S=180°, W=270°
                const { x, y } = altAzToXY(-3, azimuth); // Just outside horizon
                return Plot.text([{ x, y, label: dir }], {
                    x: "x",
                    y: "y",
                    text: "label",
                    fontSize: 14,
                    fontWeight: "bold",
                    fill: "rgb(200, 200, 220)"
                });
            }),

            // Stars (varying size and brightness by magnitude)
            Plot.dot(stars, {
                x: "x",
                y: "y",
                r: d => Math.max(1.5, 5 - d.mag * 0.8), // Brighter stars (lower mag) are larger
                fill: d => {
                    // Scale brightness based on magnitude
                    const brightness = Math.max(0, Math.min(1, (2 - d.mag) / 4));
                    const gray = Math.floor(180 + brightness * 75);
                    return `rgb(${gray}, ${gray}, ${gray})`;
                },
                opacity: d => Math.max(0.5, Math.min(1, (3 - d.mag) / 3)) // Brighter stars more opaque
            }),

            // Sun (magnitude -26.74, by far the brightest)
            Plot.dot(celestialBodies.filter(b => b.type === 'sun'), {
                x: "x",
                y: "y",
                r: 15,
                fill: "rgb(255, 240, 150)",
                stroke: "rgb(255, 220, 100)",
                strokeWidth: 3,
                opacity: 1
            }),

            // Moon (magnitude ~-12, second brightest)
            Plot.dot(celestialBodies.filter(b => b.type === 'moon'), {
                x: "x",
                y: "y",
                r: 10,
                fill: "rgb(240, 240, 250)",
                stroke: "rgb(220, 220, 240)",
                strokeWidth: 2,
                opacity: 0.95
            }),

            // Planets (varying size by magnitude)
            Plot.dot(celestialBodies.filter(b => b.type === 'planet'), {
                x: "x",
                y: "y",
                r: d => Math.max(3, 7 - d.magnitude * 0.8), // Venus brightest, Saturn dimmest
                fill: d => {
                    const colors = {
                        'Mercury': 'rgb(200, 160, 120)',
                        'Venus': 'rgb(255, 250, 200)',  // Brightest planet
                        'Mars': 'rgb(240, 120, 100)',
                        'Jupiter': 'rgb(230, 200, 160)',
                        'Saturn': 'rgb(240, 220, 180)',
                        'Uranus': 'rgb(180, 220, 240)',
                        'Neptune': 'rgb(150, 180, 240)'
                    };
                    return colors[d.name] || 'rgb(150, 150, 200)';
                },
                stroke: "white",
                strokeWidth: 1.5,
                opacity: d => Math.max(0.7, 1 - d.magnitude * 0.1) // Brighter planets more opaque
            }),

            // Telescope trajectories (12 hours into future)
            ...telescopeTrajectories.map(traj => {
                return Plot.line(
                    traj.points,
                    {
                        x: "x",
                        y: "y",
                        stroke: "rgb(150, 150, 120)",
                        strokeWidth: 2,
                        strokeDasharray: "5,3",
                        fill: "none",
                        opacity: "opacity"  // Use the opacity value from each point
                    }
                );
            }),

            // Telescopes (as cross symbols)
            ...telescopeMarkers.flatMap(tel => {
                const size = 4;

                return [
                    // Horizontal line
                    Plot.line(
                        [
                            { x: tel.x - size, y: tel.y },
                            { x: tel.x + size, y: tel.y }
                        ],
                        {
                            x: "x",
                            y: "y",
                            stroke: "rgb(150, 150, 150)",
                            strokeWidth: 1.5
                        }
                    ),
                    // Vertical line
                    Plot.line(
                        [
                            { x: tel.x, y: tel.y - size },
                            { x: tel.x, y: tel.y + size }
                        ],
                        {
                            x: "x",
                            y: "y",
                            stroke: "rgb(150, 150, 150)",
                            strokeWidth: 1.5
                        }
                    )
                ];
            }),

            // Hover highlight for all objects
            Plot.dot(
                allObjects,
                Plot.pointer({
                    x: "x",
                    y: "y",
                    r: 10,
                    stroke: "yellow",
                    strokeWidth: 3,
                    fill: "none"
                })
            ),

            // Hover tooltip text
            Plot.text(
                allObjects,
                Plot.pointer({
                    px: "x",
                    py: "y",
                    dy: -20,
                    frameAnchor: "bottom",
                    fontVariant: "tabular-nums",
                    text: d => {
                        if (d.type === 'star') {
                            return `${d.name} (mag ${d.mag.toFixed(1)})`;
                        } else if (d.type === 'telescope') {
                            return `${d.name}: Alt ${d.alt.toFixed(3)}° Az ${d.az.toFixed(3)}°`;
                        } else if (d.type === 'moon' && d.phase !== undefined) {
                            const phasePercent = (d.phase * 100).toFixed(0);
                            return `${d.name}: (${phasePercent}% illuminated)`;
                        } else {
                            return `${d.name}: Alt ${d.alt.toFixed(3)}° Az ${d.az.toFixed(3)}°`;
                        }
                    },
                    fill: "white",
                    fontSize: 14,
                    fontWeight: "bold",
                    stroke: "rgb(20, 25, 40)",
                    strokeWidth: 5,
                    paintOrder: "stroke"
                })
            )
        ]
    });

    container.innerHTML = '';
    container.appendChild(plot);
}

/**
 * Fetch celestial data from backend API
 */
async function updateSkyChart() {
    try {
        const response = await fetch('/api/sky_data');
        const result = await response.json();

        if (result.status === 'success') {
            skyData = result.data;
            plotSkyProjection();
        } else {
            console.error('Error fetching sky data:', result.message);
        }
    } catch (error) {
        console.error('Error updating sky chart:', error);
    }
}

/**
 * Update telescope positions from websocket data
 * @param {Array} newTelescopeData - Array of telescope objects with name, alt, az
 */
function updateTelescopePositions(newTelescopeData) {
    telescopes = newTelescopeData || [];
    // Only redraw if we have sky data (don't wait for next celestial update)
    if (skyData) {
        plotSkyProjection();
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
    window.addEventListener('resize', () => {
        if (skyData) {
            plotSkyProjection();
        }
    });
}

// Export functions for use in main page
if (typeof window !== 'undefined') {
    window.initializeSkyChart = initializeSkyChart;
    window.updateTelescopePositions = updateTelescopePositions;
}
