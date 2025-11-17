// Guiding chart functionality for telescope autoguiding performance visualization

// Global variables for guiding data management
let guidingDataCache = [];
let guidingLatestTimestamp = null;
let guidingActive = false;

/**
 * Update the guiding chart by fetching new data from the API
 * @param {boolean} update - Whether this is an update (true) or initial load (false)
 */
function updateGuidingChart(update = false) {
    if (!guidingActive) {
        // Hide guiding chart if not active
        document.getElementById('guiding-chart-container').classList.add('hidden');
        return;
    }

    // Show guiding chart container
    document.getElementById('guiding-chart-container').classList.remove('hidden');

    let fetchUrl;
    if (update && guidingLatestTimestamp) {
        // Format timestamp for API: replace 'T' with space if present
        let since = guidingLatestTimestamp;
        if (since.includes('T')) {
            since = since.replace('T', ' ');
        }
        fetchUrl = '/api/db/guiding?since=' + encodeURIComponent(since);
    } else {
        fetchUrl = '/api/db/guiding?day=1';
    }

    fetch(fetchUrl)
        .then(response => response.json())
        .then(result => {
            if (result.status === 'success') {
                if (result.data.length > 0) {
                    if (update && guidingLatestTimestamp) {
                        // Append new data
                        guidingDataCache = guidingDataCache.concat(result.data);
                        // Keep only last 2000 points to avoid memory issues
                        if (guidingDataCache.length > 2000) {
                            guidingDataCache = guidingDataCache.slice(-2000);
                        }
                    } else {
                        // Initial load
                        guidingDataCache = result.data;
                    }

                    if (guidingDataCache.length > 0) {
                        guidingLatestTimestamp = guidingDataCache[guidingDataCache.length - 1].datetime;
                    }
                }

                // Always re-plot if we have cached data (even if no new data arrived)
                if (guidingDataCache.length > 0) {
                    plotGuidingData(guidingDataCache);
                }
            }
        })
        .catch(error => {
            console.error('Error fetching guiding data:', error);
        });
}

/**
 * Plot guiding data showing RA and Dec corrections over time
 * @param {Array} data - Array of guiding data points with datetime, post_pid_x, post_pid_y
 */
function plotGuidingData(data) {
    if (!data || data.length === 0) {
        return;
    }

    // Filter data to only show the last hour
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    const filteredData = data.filter(d => new Date(d.datetime + 'Z') >= oneHourAgo);

    if (filteredData.length === 0) {
        return;
    }

    const width = document.getElementById('content').clientWidth;
    const fixed_width = 320;
    const height = Math.max(width, fixed_width) * 0.3;

    const plotContainer = document.getElementById('guiding-chart');

    if (!plotContainer) {
        console.error('guiding-chart element not found!');
        return;
    }

    plotContainer.innerHTML = '';

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
        color: {
            legend: true,
            domain: ["RA (post_pid_x)", "Dec (post_pid_y)"],
            range: ["rgb(65, 105, 225)", "rgb(255, 99, 71)"]
        },
        marks: [
            Plot.axisY({
                anchor: "right",
            }),
            Plot.ruleY([0], { stroke: "gray", strokeDasharray: "4,4" }),
            // RA line and dots
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
                        return `${timestamp}   RA: ${d.post_pid_x.toFixed(2)}px  Dec: ${d.post_pid_y.toFixed(2)}px`;
                    },
                })
            ),
        ],
    });

    plotContainer.appendChild(plot);
}

/**
 * Check and update guiding status from websocket message
 * @param {Array} deviceData - Array of device data from websocket
 * @returns {boolean} - Whether guiding is currently active
 */
function updateGuidingStatus(deviceData) {
    let newGuidingActive = false;
    for (let i = 0; i < deviceData.length; i++) {
        if (deviceData[i]['item'] === 'guider' && deviceData[i]['status'] === true) {
            newGuidingActive = true;
            break;
        }
    }

    // If guiding status changed, reset cache and update chart
    if (newGuidingActive !== guidingActive) {
        guidingActive = newGuidingActive;
        if (guidingActive) {
            // Guiding just started - fetch initial data
            guidingDataCache = [];
            guidingLatestTimestamp = null;
            updateGuidingChart(false);
        } else {
            // Guiding stopped - hide chart
            updateGuidingChart(false);
        }
    }

    return guidingActive;
}
