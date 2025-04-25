// import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";
// todo:
// - show when night begins
// - radial circle on touch for wind direction?
// - shared tooltip?

// Color palette for weather parameters
function color_palette(parameter) {
    const colorMap = {
        CloudCover: "rgba(180, 180, 180, 0.5)", // Light gray, slightly more opaque for visibility
        DewPoint: "rgba(135, 206, 250, 0.5)", // Lighter, brighter blue for dew point
        Humidity: "rgba(65, 105, 225, 0.5)", // Royal blue, more vibrant
        Pressure: "rgba(255, 160, 122, 0.5)", // Lighter coral/salmon for pressure
        RainRate: "rgba(30, 144, 255, 0.5)", // Dodger blue, more saturated
        SkyBrightness: "rgba(255, 215, 0, 0.5)", // Bright gold for sky brightness
        SkyQuality: "rgba(186, 85, 211, 0.5)", // Medium orchid, more readable
        SkyTemperature: "rgba(46, 129, 187, 0.5)", // Sea blue, deeper tone
        StarFWHM: "rgba(221, 160, 221, 0.5)", // Plum, softer and more visible
        Temperature: "rgba(255, 99, 71, 0.5)", // Tomato red, vibrant but not too harsh
        WindDirection: "rgba(100, 149, 237, 0.5)", // Cornflower blue
        WindGust: "rgba(192, 192, 192, 0.5)", // Silver gray
        WindSpeed: "rgba(32, 178, 170, 0.5)", // Dark turquoise, slightly brighter
    };

    return colorMap[parameter] || "rgba(128, 128, 128, 0.8)"; // default to grey if parameter not found
    // return "rgba(255, 255, 255, 0.5)"; // default to grey if parameter not found
}

// Helper function to create text label for data points
function createDataLabel(d, parameter, unit) {
    const timestamp = d.datetime;
    const value = `${d[parameter].toFixed(2)} ${unit}`;
    return `${timestamp}   ${value}`;
};

// Function to determine dot color based on safety limits
function getDotColor(d, parameter, safety_limits, defaultColor) {
    const value = d[parameter];
    const { upper, lower } = safety_limits[parameter];

    if (upper !== null && value > upper) {
        return "red";
    }

    if (lower !== null && value < lower) {
        return "red";
    }
    return defaultColor;
};

// Function to add units to weather parameters
function addUnits(parameter, weather_safety_limits) {
    if (
        parameter === "Temperature" ||
        parameter === "DewPoint" ||
        parameter === "SkyTemperature"
    ) {
        weather_safety_limits[parameter]["unit"] = "°C";
    } else if (parameter === "Humidity") {
        weather_safety_limits[parameter]["unit"] = "%";
    } else if (parameter === "WindSpeed" || parameter === "WindGust") {
        weather_safety_limits[parameter]["unit"] = "m/s";
    } else if (parameter === "RainRate") {
        weather_safety_limits[parameter]["unit"] = "mm/h";
    } else if (parameter === "SkyBrightness") {
        weather_safety_limits[parameter]["unit"] = "lux";
    } else if (parameter === "WindDirection") {
        weather_safety_limits[parameter]["unit"] = "°";
    } else if (parameter === "Pressure") {
        weather_safety_limits[parameter]["unit"] = "hPa";
    } else {
        weather_safety_limits[parameter]["unit"] = "";
    }
}

// Function to plot weather data
function plotWeather(data, observatory, update) {

    console.log("Plotting weather data for", observatory);

    const weather_data = data['data'];
    const weather_safety_limits = data['safety_limits'];

    const width = document.getElementById(`content--${observatory}`).clientWidth;
    const fixed_width = 320;
    const height = Math.max(width, fixed_width) * 0.4;
    const percent_to_show = 10;
    const weather_parameters = Object.keys(weather_data[0]);

    // sort the weather parameters such that temperature and dew point are first and then humidity, then the rest
    weather_parameters.sort((a, b) => {
        const priority = {
            SkyTemperature: 1,
            RainRate: 2,
            WindSpeed: 3,
            WindGust: 4,
            Humidity: 5,
            Temperature: 6,
            DewPoint: 7,
        };
        return (priority[a] || Infinity) - (priority[b] || Infinity);
    });


    const latest_values = data['latest'];

    weather_parameters.forEach((parameter) => {
        // find min and max values for each parameter
        const values = weather_data.map((d) => d[parameter]);
        const min = Math.min(...values);
        const max = Math.max(...values);

        if (!(parameter in weather_safety_limits)) {
            weather_safety_limits[parameter] = { lower: null, upper: null };
        }
        weather_safety_limits[parameter].min = min;
        weather_safety_limits[parameter].max = max;

        // add units for ASCOM spec
        addUnits(parameter, weather_safety_limits);

        const { upper: upper_safety_limit, lower: lower_safety_limit } = weather_safety_limits[parameter];
        const safety_range = upper_safety_limit - lower_safety_limit;

        // Calculate percentage differences
        const max_value_from_upper_safety_limit = upper_safety_limit !== null ? Math.abs(((max - upper_safety_limit) / safety_range) * 100) : Infinity;
        const min_value_from_lower_safety_limit = lower_safety_limit !== null ? Math.abs(((min - lower_safety_limit) / safety_range) * 100) : Infinity;

        // Determine upper domain
        const upper_domain = max < lower_safety_limit && lower_safety_limit !== null
            ? lower_safety_limit
            : max_value_from_upper_safety_limit < percent_to_show
                ? Math.max(max, upper_safety_limit)
                : max;

        // Determine lower domain
        const lower_domain = min > upper_safety_limit && upper_safety_limit !== null
            ? upper_safety_limit
            : min_value_from_lower_safety_limit < percent_to_show
                ? Math.min(min, lower_safety_limit)
                : min;

        weather_safety_limits[parameter].domain = [lower_domain, upper_domain];
    });


    document.getElementById(`weather-latest--${observatory}`).innerHTML = `
        <div class="flex-initial overflow-x-scroll">
        <table class="table-auto w-full proportional-nums font-variant-numeric rounded-lg bg-gray-600/20" id="weather-table--${observatory}" title="Last refreshed: ${new Date().toISOString().slice(0, 19).replace("T", " ")}">
        <thead>
            <tr class="border-b-2 border-b-slate-500">
                <td class="py-1.5 px-3" style="text-align: left;">Parameter</td>
                <td class="py-1.5 px-3" style="text-align: left;">Unit</td>
                <td class="py-1.5 px-3" style="text-align: right;">Latest</td>
                <td class="py-1.5 px-3" style="text-align: right;">&#10515;</td>
                <td class="py-1.5 px-3" style="text-align: right;">&#10514;</td>
            </tr>
        </thead>
        <tbody>
        ${weather_parameters
            .map((parameter, index) => {
                if (parameter === "datetime") return "";
                const value = latest_values[parameter];

                const hasSafetyLimit =
                    weather_safety_limits[parameter].lower !== null ||
                    weather_safety_limits[parameter].upper !== null;

                const isExceedingLimit =
                    hasSafetyLimit &&
                    ((weather_safety_limits[parameter].lower !== null &&
                        value < weather_safety_limits[parameter].lower) ||
                        (weather_safety_limits[parameter].upper !== null &&
                            value > weather_safety_limits[parameter].upper));

                // is close to upper limit
                const isCloseToUpperLimit =
                    hasSafetyLimit &&
                    weather_safety_limits[parameter].upper !== null &&
                    Math.abs(
                        ((value - weather_safety_limits[parameter].upper) /
                            (weather_safety_limits[parameter].upper -
                                (weather_safety_limits[parameter].lower !== null
                                    ? weather_safety_limits[parameter].lower
                                    : 0))) *
                        100
                    ) < percent_to_show;

                const isCloseToLowerLimit =
                    hasSafetyLimit &&
                    weather_safety_limits[parameter].lower !== null &&
                    Math.abs(
                        ((value - weather_safety_limits[parameter].lower) /
                            ((weather_safety_limits[parameter].upper !== null
                                ? weather_safety_limits[parameter].upper
                                : value) -
                                weather_safety_limits[parameter].lower)) *
                        100
                    ) < percent_to_show;

                const colorStyleLink = isExceedingLimit
                    ? "color: red;"
                    : isCloseToUpperLimit || isCloseToLowerLimit
                        ? "color: orange;"
                        : "";

                const colorStyle = isExceedingLimit
                    ? "color: red;"
                    : isCloseToUpperLimit || isCloseToLowerLimit
                        ? "color: orange;"
                        : "color: rgb(34 197 94 / var(--tw-text-opacity, 1));";

                const colorStyleUpper = isExceedingLimit
                    ? "color: red;"
                    : isCloseToUpperLimit
                        ? "color: orange;"
                        : "";

                const colorStyleLower = isExceedingLimit
                    ? "color: red;"
                    : isCloseToLowerLimit
                        ? "color: orange;"
                        : "";

                return `<tr class="${index != weather_parameters.length - 1 ? 'border-b border-b-slate-500' : ""} cursor-pointer"
                            onclick="window.location.href='#plot-${parameter}--${observatory}';">
                            <td class="py-1.5 px-3" style="text-align: left; ${colorStyleLink}">${parameter}</td>
                            <td class="py-1.5 px-3" style="${colorStyleLink} text-align: left;">${weather_safety_limits[parameter].unit
                    }</td>
                            <td class="py-1.5 px-3" style="text-align: right; ${colorStyle}">
                            ${latest_values[parameter].toFixed(1)}
                            </td>
                            <td class="py-1.5 px-3" style="text-align: right; ${colorStyleLower}">
                            ${weather_safety_limits[parameter].lower !== null
                        ? weather_safety_limits[parameter].lower.toFixed(1)
                        : ""
                    }
                            </td>
                            <td class="py-1.5 px-3" style="text-align: right; ${colorStyleUpper}">
                            ${weather_safety_limits[parameter].upper !== null
                        ? weather_safety_limits[parameter].upper.toFixed(1)
                        : ""
                    }
                            </td>
                        </tr>`;
            })
            .join("")}
          </tbody>
        </table>
        </div>`;


    // Helper function to create common plot marks
    const createCommonMarks = (
        weather_data,
        parameter,
        weather_safety_limits
    ) => [
            Plot.axisY({
                tickFormat: (d) =>
                    d >= 1000 ? `${(d / 1000).toFixed(1)}k` : d.toFixed(1),
                anchor: "right",
            }),
            Plot.ruleX(
                weather_data,
                Plot.pointerX({
                    x: (d) => new Date(d.datetime + 'Z'),
                    py: parameter,
                    stroke: "yellow",
                    strokeWidth: 2,
                })
            ),
            Plot.dot(
                weather_data,
                Plot.pointerX({
                    x: (d) => new Date(d.datetime + 'Z'),
                    y: parameter,
                    r: 3,
                    fill: "yellow",
                    fillOpacity: 0.8,
                })
            ),
            Plot.text(
                weather_data,
                Plot.pointerX({
                    px: (d) => new Date(d.datetime + 'Z'),
                    py: parameter,
                    dy: -17,
                    frameAnchor: "top-left",
                    fontVariant: "tabular-nums",
                    text: (d) =>
                        createDataLabel(
                            d,
                            parameter,
                            weather_safety_limits[parameter].unit
                        ),
                })
            ),
        ];

    // Main plotting function
    const createWeatherPlots = (
        weather_data,
        weather_parameters,
        weather_safety_limits,
        color_palette,
        width,
        fixed_width,
        height
    ) => {
        const plotContainer = document.getElementById(`weather-chart--${observatory}`);

        weather_parameters.forEach((parameter) => {
            if (parameter === "WindDirection" || parameter === "datetime") return;

            const safety_limits = weather_safety_limits[parameter];
            const baseConfig = {
                width: Math.max(width, fixed_width),
                height,
                grid: true,
                y: {
                    label: `${parameter} (${safety_limits.unit})`,
                },
                marks: [
                    Plot.dot(weather_data, {
                        x: (d) => new Date(d.datetime + 'Z'),
                        y: parameter,
                        r: 2,
                        fill: (d) =>
                            getDotColor(
                                d,
                                parameter,
                                weather_safety_limits,
                                color_palette(parameter)
                            ),
                    }),
                    ...createCommonMarks(
                        weather_data,
                        parameter,
                        weather_safety_limits,
                        color_palette
                    ),
                    ,
                ],
            };

            // Add safety limit rules if they exist
            if (safety_limits.upper !== null || safety_limits.lower !== null) {
                baseConfig.y.domain = safety_limits.domain;
                const rules = [];
                if (safety_limits.lower !== null) {
                    rules.push({
                        value: safety_limits.lower,
                        label: "lower safety limit",
                    });
                }
                if (safety_limits.upper !== null) {
                    rules.push({
                        value: safety_limits.upper,
                        label: "upper safety limit",
                    });
                }
                baseConfig.marks.push(
                    Plot.ruleY(rules, {
                        stroke: "red",
                        strokeDasharray: "5,3",
                        strokeOpacity: 0.5,
                        y: (d) => d.value,
                        tip: true,
                        title: (d) => `${d.label}: ${d.value} ${safety_limits.unit}`,
                    })
                );
            }

            const plot = Plot.plot(baseConfig);
            const existingPlotContainer = document.getElementById(`plot-${parameter}--${observatory}`);
            if (update && existingPlotContainer) {
                existingPlotContainer.innerHTML = '';
                existingPlotContainer.appendChild(plot);
            } else {
                const newPlotContainer = document.createElement("div");
                newPlotContainer.id = `plot-${parameter}--${observatory}`;
                newPlotContainer.appendChild(plot);
                plotContainer.appendChild(newPlotContainer);
            }
        });
    };

    createWeatherPlots(
        weather_data,
        weather_parameters,
        weather_safety_limits,
        color_palette,
        width,
        fixed_width,
        height
    );

    // plot wind direction
    if (weather_parameters.includes("WindDirection")) {

        const start_datetime = new Date(weather_data[0].datetime + 'Z').getTime();
        const end_datetime = new Date().getTime();

        let wind_direction_data = Array.from(
            { length: weather_data.length },
            (_, i) => {
                const date = weather_data[i].datetime;
                const date_parsed = new Date(weather_data[i].datetime + 'Z').getTime();
                const radius =
                    (date_parsed - start_datetime) / (end_datetime - start_datetime);

                const angle = weather_data[i].WindDirection;

                const xs = radius * Math.cos((-(angle - 90) * Math.PI) / 180);
                const ys = radius * Math.sin((-(angle - 90) * Math.PI) / 180);

                const speed = weather_data[i].WindSpeed;

                return { angle, radius, date, date_parsed, speed, xs, ys };
            }
        );

        if (weather_safety_limits["WindSpeed"]["lower"] === null) {
            weather_safety_limits["WindSpeed"]["lower"] = 0;
        }

        // Create the wind direction plot
        const plot_winddir = Plot.plot({
            width: Math.max(width, fixed_width),
            height: Math.max(width, fixed_width),
            color: {
                legend: false,
                scheme: "BuRd",
                type: "linear",
                opacity: 0.8,
                label: "WindSpeed (m/s)",
                domain: [
                    weather_safety_limits["WindSpeed"]["lower"],
                    weather_safety_limits["WindSpeed"]["upper"],
                ],
            },
            x: { domain: [-1.06, 1.06], axis: null },
            y: { domain: [-1.06, 1.06], axis: null },
            marks: [
                Plot.text([{ x: 1.05, y: 1.05 }], {
                    // label
                    x: "x",
                    y: "y",
                    text: (d) => "WindDirection (°) ↻",
                    dy: 0,
                    textAnchor: "end",
                    lineAnchor: "top",
                }),
                Plot.dot(wind_direction_data, {
                    x: "xs",
                    y: "ys",
                    fill: "speed",
                    r: 5,
                    opacity: 0.8,
                }),
                Plot.line(
                    Array.from({ length: 361 }, (_, i) => {
                        // now circle
                        const angle = (i * Math.PI) / 180;
                        return { x: Math.cos(angle), y: Math.sin(angle) };
                    }),
                    {
                        x: "x",
                        y: "y",
                        stroke: "white",
                        strokeDasharray: "5,3",
                        opacity: 0.5,
                    }
                ),
                Plot.text([{ x: 0, y: 1 }], {
                    // now text
                    x: "x",
                    y: "y",
                    text: (d) => "now",
                    dy: 4,
                    lineAnchor: "top",
                    fontStyle: "italic",
                }),
                Plot.line(
                    Array.from({ length: 361 }, (_, i) => {
                        // 50% ago circle
                        const angle = (i * Math.PI) / 180;
                        return { x: 0.5 * Math.cos(angle), y: 0.5 * Math.sin(angle) };
                    }),
                    {
                        x: "x",
                        y: "y",
                        strokeDasharray: "5,3",
                        stroke: "white",
                        opacity: 0.5,
                    }
                ),
                Plot.text([{ x: 0, y: 0.5 }], {
                    // 50% ago text
                    x: "x",
                    y: "y",
                    text: (d) =>
                        `-${((end_datetime - start_datetime) / (2 * 60 * 60 * 1000)).toFixed(0)}h`,
                    dy: 4,
                    lineAnchor: "top",
                    fontStyle: "italic",
                }),
                ...Array.from({ length: 8 }, (_, i) => {
                    const angle = i * 45;
                    return Plot.line(
                        [
                            { x: 0, y: 0 },
                            {
                                x: Math.cos((angle * Math.PI) / 180),
                                y: Math.sin((angle * Math.PI) / 180),
                            },
                        ],
                        {
                            x: "x",
                            y: "y",
                            stroke: "white",
                            opacity: 0.1,
                        }
                    );
                }),
                ...Array.from({ length: 8 }, (_, i) => {
                    const angle = i * 45 + 90;
                    const headings = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];
                    return Plot.text(
                        [
                            {
                                x: -1.04 * Math.cos((angle * Math.PI) / 180),
                                y: 1.04 * Math.sin((angle * Math.PI) / 180),
                            },
                        ],
                        {
                            x: "x",
                            y: "y",
                            text: (d) => headings[i],
                            fontWeight: "bold",
                        }
                    );
                }),
                Plot.dot(
                    wind_direction_data,
                    Plot.pointer({
                        x: "xs",
                        y: "ys",
                        r: 5,
                        stroke: "yellow",
                        opacity: 0.8,
                        // fillOpacity: 0.8,
                    })
                ),
                Plot.text(
                    wind_direction_data,
                    Plot.pointer({
                        px: "xs",
                        py: "ys",
                        dy: 0,
                        frameAnchor: "top-left",
                        fontVariant: "tabular-nums",
                        text: (d) =>
                            [
                                `${d.date}`,
                                `${d.speed.toFixed(2)} ${weather_safety_limits["WindSpeed"]["unit"]
                                }`,
                                `${d.angle.toFixed(2)} ${weather_safety_limits["WindDirection"]["unit"]
                                }`,
                            ].join("   "),
                    })
                ),
            ],
        });

        // const plot = Plot.plot(baseConfig);
        const plotContainer = document.getElementById(`weather-chart--${observatory}`);
        const existingPlotContainer = document.getElementById(`plot-WindDirection--${observatory}`);
        if (update && existingPlotContainer) {
            existingPlotContainer.innerHTML = '';
            existingPlotContainer.appendChild(plot_winddir);
        } else {
            const newPlotContainer = document.createElement("div");
            newPlotContainer.id = `plot-WindDirection--${observatory}`;
            newPlotContainer.appendChild(plot_winddir);
            plotContainer.appendChild(newPlotContainer);
        }
    }

};