// Guiding chart functionality for telescope autoguiding performance visualization

// Global variables for guiding data management
let guidingDataCache = {}; // Changed to object keyed by telescope_name
let guidingLatestTimestamp = {};
let guidingActive = {}; // Track active status per telescope

/**
 * Update the guiding chart by fetching new data from the API
 * @param {boolean} update - Whether this is an update (true) or initial load (false)
 */
function updateGuidingChart(update = false) {
    // Get list of active telescopes
    const activeTelescopeNames = Object.keys(guidingActive).filter(name => guidingActive[name]);

    if (activeTelescopeNames.length === 0) {
        // Hide guiding chart if not active
        document.getElementById('guiding-chart-container').classList.add('hidden');
        return;
    }

    // Show guiding chart container
    document.getElementById('guiding-chart-container').classList.remove('hidden');

    // Fetch data (no telescope filter - get all telescopes)
    let fetchUrl;
    if (update) {
        // Get the oldest timestamp across all active telescopes
        let oldestTimestamp = null;
        for (const telescopeName of activeTelescopeNames) {
            const ts = guidingLatestTimestamp[telescopeName];
            if (ts && (!oldestTimestamp || ts < oldestTimestamp)) {
                oldestTimestamp = ts;
            }
        }

        if (oldestTimestamp) {
            // Format timestamp for API: replace 'T' with space if present
            let since = oldestTimestamp;
            if (since.includes('T')) {
                since = since.replace('T', ' ');
            }
            fetchUrl = '/api/db/guiding?since=' + encodeURIComponent(since);
        } else {
            fetchUrl = '/api/db/guiding?day=1';
        }
    } else {
        fetchUrl = '/api/db/guiding?day=1';
    }

    fetch(fetchUrl)
        .then(response => response.json())
        .then(result => {
            if (result.status === 'success') {
                if (result.data.length > 0) {
                    // Group data by telescope_name
                    result.data.forEach(row => {
                        const telescopeName = row.telescope_name;

                        if (!guidingDataCache[telescopeName]) {
                            guidingDataCache[telescopeName] = [];
                        }

                        if (update && guidingLatestTimestamp[telescopeName]) {
                            // Append new data
                            guidingDataCache[telescopeName].push(row);
                            // Keep only last 2000 points to avoid memory issues
                            if (guidingDataCache[telescopeName].length > 2000) {
                                guidingDataCache[telescopeName] = guidingDataCache[telescopeName].slice(-2000);
                            }
                        } else {
                            // Initial load - only add if not duplicate
                            if (!guidingDataCache[telescopeName].find(d => d.datetime === row.datetime)) {
                                guidingDataCache[telescopeName].push(row);
                            }
                        }
                    });

                    // Update latest timestamps per telescope
                    for (const telescopeName in guidingDataCache) {
                        const data = guidingDataCache[telescopeName];
                        if (data.length > 0) {
                            guidingLatestTimestamp[telescopeName] = data[data.length - 1].datetime;
                        }
                    }
                }

                // Plot all active telescopes
                plotAllGuidingData(activeTelescopeNames);
            }
        })
        .catch(error => {
            console.error('Error fetching guiding data:', error);
        });
}

/**
 * Plot all active telescopes' guiding data
 * @param {Array} activeTelescopeNames - Array of telescope names currently guiding
 */
function plotAllGuidingData(activeTelescopeNames) {
    const plotContainer = document.getElementById('guiding-chart');

    if (!plotContainer) {
        console.error('guiding-chart element not found!');
        return;
    }

    // Remove all existing charts
    plotContainer.innerHTML = '';

    // Plot each active telescope
    for (const telescopeName of activeTelescopeNames) {
        const data = guidingDataCache[telescopeName];
        if (data && data.length > 0) {
            plotGuidingData(telescopeName, data, plotContainer);
        }
    }
}

/**
 * Plot guiding data for a single telescope showing RA and Dec corrections over time
 * @param {string} telescopeName - Name of the telescope
 * @param {Array} data - Array of guiding data points with datetime, post_pid_x, post_pid_y
 * @param {HTMLElement} container - Container element to append the plot to
 */
function plotGuidingData(telescopeName, data, container) {
    if (!data || data.length === 0) {
        console.warn(`No guiding data to plot for telescope: ${telescopeName}`);
        return;
    }

    // Filter data to only show the last hour
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    const filteredData = data.filter(d => new Date(d.datetime + 'Z') >= oneHourAgo);

    if (filteredData.length === 0) {
        console.warn(`No recent guiding data to plot for telescope: ${telescopeName}`);
        return;
    }

    const width = document.getElementById('content').clientWidth;
    const fixed_width = 320;
    const height = Math.max(width, fixed_width) * 0.3;

    // Create a wrapper div for this telescope's chart
    const chartWrapper = document.createElement('div');
    chartWrapper.className = 'telescope-chart-wrapper mb-4';

    // Add telescope name label
    const label = document.createElement('div');
    label.className = 'text-xs font-medium text-gray-400 mb-1';
    label.textContent = telescopeName + "'s Guiding Performance";
    chartWrapper.appendChild(label);

    // Create plot for both axes
    const plot = Plot.plot({
        width: Math.max(width, fixed_width),
        height: height,
        grid: true,
        x: {
            label: "Time (UTC)",
        },
        y: {
            label: "Correction (pixels)",
            grid: true,
        },
        // color: {
        //     legend: true,
        //     domain: ["RA (post_pid_x)", "Dec (post_pid_y)"],
        //     range: ["rgb(65, 105, 225)", "rgb(255, 99, 71)"]
        // },
        marks: [
            Plot.axisY({
                anchor: "right",
            }),
            Plot.ruleY([0], { stroke: "gray", strokeDasharray: "4,4" }),
            // x line and dots
            Plot.lineY(filteredData, {
                x: (d) => new Date(d.datetime + 'Z'),
                y: "post_pid_x",
                stroke: "rgb(65, 105, 225)",
                strokeWidth: 2,
            }),
            Plot.dot(filteredData, {
                x: (d) => new Date(d.datetime + 'Z'),
                y: "post_pid_x",
                r: 3,
                fill: "rgb(65, 105, 225)",
                stroke: "white",
                strokeWidth: 1,
            }),
            // Dec line and dots
            Plot.lineY(filteredData, {
                x: (d) => new Date(d.datetime + 'Z'),
                y: "post_pid_y",
                stroke: "rgb(255, 99, 71)",
                strokeWidth: 2,
            }),
            Plot.dot(filteredData, {
                x: (d) => new Date(d.datetime + 'Z'),
                y: "post_pid_y",
                r: 3,
                fill: "rgb(255, 99, 71)",
                stroke: "white",
                strokeWidth: 1,
            }),
            // Hover interaction
            Plot.ruleX(
                filteredData,
                Plot.pointerX({
                    x: (d) => new Date(d.datetime + 'Z'),
                    py: "post_pid_x",
                    stroke: "yellow",
                    strokeWidth: 2,
                })
            ),
            Plot.dot(
                filteredData,
                Plot.pointerX({
                    x: (d) => new Date(d.datetime + 'Z'),
                    y: "post_pid_x",
                    r: 6,
                    fill: "rgb(65, 105, 225)",
                    stroke: "yellow",
                    strokeWidth: 2,
                })
            ),
            Plot.dot(
                filteredData,
                Plot.pointerX({
                    x: (d) => new Date(d.datetime + 'Z'),
                    y: "post_pid_y",
                    r: 6,
                    fill: "rgb(255, 99, 71)",
                    stroke: "yellow",
                    strokeWidth: 2,
                })
            ),
            Plot.text(
                filteredData,
                Plot.pointerX({
                    px: (d) => new Date(d.datetime + 'Z'),
                    py: "post_pid_x",
                    dy: -17,
                    frameAnchor: "top-left",
                    fontVariant: "tabular-nums",
                    text: (d) => {
                        const timestamp = d.datetime.replace('T', ' ').slice(0, 19);
                        return `${timestamp}   x (blue): ${d.post_pid_x.toFixed(2)}px  y (red): ${d.post_pid_y.toFixed(2)}px`;
                    },
                })
            ),
        ],
    });

    chartWrapper.appendChild(plot);
    container.appendChild(chartWrapper);
}

/**
 * Check and update guiding status from websocket message
 * @param {Array} deviceData - Array of device data from websocket
 * @returns {Object} - Object with telescope names as keys and status as values
 */
function updateGuidingStatus(deviceData) {
    const newGuidingActive = {};

    // Find all guider entries in device data
    for (let i = 0; i < deviceData.length; i++) {
        if (deviceData[i]['item'] === 'guider') {
            // Extract telescope name from the guider name (format: "telescope_name's guider")
            const guiderName = deviceData[i]['name'];
            const telescopeName = guiderName.replace("'s guider", "");
            newGuidingActive[telescopeName] = deviceData[i]['status'] === true;
        }
    }

    // Check if any guiding status changed
    let statusChanged = false;

    // Check for new or changed telescopes
    for (const telescopeName in newGuidingActive) {
        if (guidingActive[telescopeName] !== newGuidingActive[telescopeName]) {
            statusChanged = true;
            guidingActive[telescopeName] = newGuidingActive[telescopeName];

            if (newGuidingActive[telescopeName]) {
                // Guiding just started for this telescope - initialize cache
                if (!guidingDataCache[telescopeName]) {
                    guidingDataCache[telescopeName] = [];
                }
                guidingLatestTimestamp[telescopeName] = null;
            }
        }
    }

    // Check for telescopes that are no longer present (stopped)
    for (const telescopeName in guidingActive) {
        if (!(telescopeName in newGuidingActive)) {
            guidingActive[telescopeName] = false;
            statusChanged = true;
        }
    }

    // If any status changed, update chart
    if (statusChanged) {
        updateGuidingChart(false);
    }

    return guidingActive;
}
