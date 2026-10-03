import {
    parseFITSImage, zscale, buildHistogram, valueForPercentile, percentileForValue, formatNumber,
    splitHeaderValue, plateScaleFromPixelSize, buildStretchLut,
} from './utils.js';
import { calculateAdaptiveFWHM, drawApertureCircles } from './fwhm.js';
import { WCS, calculateGridTicks, intersect, selectHitByPriority, formatRa, formatDec } from './wcs.js';

const d3 = window.d3;
if (!d3) throw new Error('FITSViewer requires D3.js to be loaded before this module');

const VERT_SRC = `
  attribute vec2 a_pos;
  varying vec2 v_uv;
  void main() {
    v_uv = a_pos * 0.5 + 0.5;
    gl_Position = vec4(a_pos, 0.0, 1.0);
  }`;

const FRAG_SRC = `
  precision highp float;
  varying vec2 v_uv;
  uniform sampler2D u_image;
  uniform float u_vmin;
  uniform float u_vmax;
  uniform float u_gamma;
  uniform float u_useLog;
  void main() {
    float val = texture2D(u_image, v_uv).r;
    float range = u_vmax - u_vmin;
    if (range == 0.0) range = 1.0;
    float norm = clamp((val - u_vmin) / range, 0.0, 1.0);
    if (u_useLog > 0.5) {
      norm = log(1.0 + norm * range) / log(1.0 + range);
    }
    norm = pow(norm, 1.0 / u_gamma);
    gl_FragColor = vec4(norm, norm, norm, 1.0);
  }`;

// ── Constants ──────────────────────────────────────────────────────────────────
const FWHM_BOX_SIZE_ARCSEC = 20;   // default box side in arcseconds when plate scale is known
const FWHM_BOX_SIZE_PX = 20;   // fallback box side in pixels
const SNR_THRESHOLD = 5;    // minimum SNR to display FWHM / aperture circles
const SHARPNESS_THRESHOLD = 0.2;  // minimum sharpness (bin-1/bin-0 ratio) to accept a FWHM measurement
const HISTOGRAM_BINS = 4096;
const MAX_ZOOM = 100;
const FWHM_THROTTLE_MS = 50;   // minimum ms between FWHM recalculations on mousemove
const FWHM_BOX_SIZE_MAX_PX = 200; // largest box side, so a hover measurement stays fast
const PROFILE_SIZE = 100;      // px: width of the y-profile column, height of the x-profile row
const COMPACT_WIDTH = 560;     // px: narrower containers hide the profiles and show a readout bar
const COMPACT_HEIGHT = 420;    // px: so do lower ones (phones in landscape)
const LONG_PRESS_MS = 550;     // touch: hold this long to open the context menu
const LONG_PRESS_MOVE_PX = 10; // ...without moving more than this

export class FITSViewer {
    /**
     * @param {HTMLElement} container - DOM element to render the viewer into. The viewer
     *   fills it and follows its size.
     * @param {object} [options]
     * @param {boolean} [options.autoZScale=true]
     * @param {boolean} [options.drawApertureCircles=true]
     * @param {boolean} [options.useGPU=true]
     * @param {boolean} [options.keyboard=true] - Built-in shortcuts (left/right arrow: image/header
     *   view, Esc: clear header search). Turn off when the host page uses these keys.
     * @param {Function} [options.onOpenExternal] - Called with a URL string for external links.
     * @param {Function} [options.onLog] - Called with (level, message) for log forwarding.
     */
    constructor(container, options = {}) {
        this.container = container;
        this.options = {
            autoZScale: true,
            drawApertureCircles: true,
            useGPU: true,
            keyboard: true,
            onOpenExternal: (url) => {
                const a = document.createElement('a');
                a.href = url;
                a.target = '_blank';
                a.rel = 'noopener noreferrer';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
            },
            onLog: null,
            profileColor: null, // e.g. 'rgba(100,100,255,0.8)' — falls back to --vscode-editor-compositionBorder
            gridColor: null,    // e.g. '#cccccc' — falls back to --vscode-editor-foreground
            ...options,
        };

        // State
        this.imageWidth = null;
        this.imageHeight = null;
        this.imageData = null;
        this.histogram = null;
        this.currentTransform = d3.zoomIdentity;
        this.wcs = null;
        this.plateScale = null;
        this.headerData = {};
        this.filename = null;
        this.offscreenCanvas = null;
        this.offscreenCtx = null;
        this.useWebGL = false;
        this.useGPUSetting = this.options.useGPU;
        this.gl = null;
        this.glProgram = null;
        this.glUniforms = {};
        this.rect = null;
        this.scaleX = null;
        this.scaleY = null;
        this.profileColor = 'rgba(100,100,255,0.8)';
        this.gridColor = '#e0e0e0';
        this.activeContextMenu = null;
        this.showGrid = false;
        this._lastFwhmTime = 0;
        this._lastFwhmResult = null;
        this._lastFwhmValid = false;
        this.autoZScale = this.options.autoZScale;
        this.doDrawApertureCircles = this.options.drawApertureCircles;
        this.binning = 1;          // source pixels per image pixel (downsampled previews)
        this.headerDisplay = null; // header shown in the header view, if the host sets one
        this.compact = false;
        this._listeners = [];      // [target, type, handler, options], removed by destroy()
        this._zoom = null;

        this._buildDOM(container);
        this._wireEvents();
        this._updateColor();
        this._updateGridToggleVisibility(false);
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Load and display a FITS file from a URL.
     * @param {string} url
     * @returns {Promise<void>}
     */
    async load(url) {
        const filename = url.split('/').pop().replace(/\.[^.]+$/, '');
        this._showSpinner();
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Failed to load ${url}: HTTP ${response.status}`);
        const arrayBuffer = await response.arrayBuffer();
        await this.loadFromArrayBuffer(arrayBuffer, filename);
    }

    /**
     * Load and display a FITS file from an ArrayBuffer.
     * @param {ArrayBuffer} buffer
     * @param {string} [filename]
     * @param {object} [opts] - See loadImageData.
     * @returns {Promise<void>}
     */
    async loadFromArrayBuffer(buffer, filename = 'image', opts = {}) {
        this._showSpinner();
        const [header, width, height, data, dataMin, dataMax] = parseFITSImage(buffer, new DataView(buffer));
        await this.loadImageData({ header, width, height, data, dataMin, dataMax }, filename, opts);
    }

    /**
     * Display an image the host has already parsed, for example in a web worker.
     * @param {object} image
     * @param {object} image.header - Keyword to raw card value ("value / comment").
     * @param {number} image.width
     * @param {number} image.height
     * @param {ArrayLike<number>} image.data - Row-major pixel values, row 0 first.
     * @param {number} [image.dataMin] - Finite minimum; computed when left out.
     * @param {number} [image.dataMax] - Finite maximum; computed when left out.
     * @param {string} [filename] - Used for "Save image as png".
     * @param {object} [opts]
     * @param {number} [opts.binning=1] - Source pixels per image pixel, for a downsampled
     *   preview. Above 1, star measurement is off (binning widens stars) and pixel
     *   positions are shown in source pixels.
     * @returns {Promise<void>}
     */
    async loadImageData(image, filename = 'image', { binning = 1 } = {}) {
        this.filename = filename;
        this.binning = Math.max(1, Math.round(binning) || 1);
        this.headerDisplay = null;
        this._processImage(image);
        this._setupZoom();
        this._updateColor();
    }

    /**
     * Show these header cards in the header view instead of the ones parsed from
     * the image (for example the original header of a downsampled preview).
     * @param {object|null} header - Keyword to "value / comment" or {value, comment}.
     */
    setHeaderData(header) {
        this.headerDisplay = header;
        if (this.headerGridContainer.style.display !== 'none') this._displayHeaderTable(this._headerForDisplay());
    }

    /** Switch to the header view. */
    showHeader() {
        this._displayHeaderTable(this._headerForDisplay());
        this.imageGridContainer.style.display = 'none';
        this.headerGridContainer.style.display = 'grid';
        this.searchInput.focus({ preventScroll: true });
    }

    /** Switch to the image view. */
    showImage() {
        this.headerGridContainer.style.display = 'none';
        this.imageGridContainer.style.display = 'grid';
        this.searchInput.blur();
        this._handleResize();
    }

    toggleHeader() {
        if (this.headerGridContainer.style.display === 'none') this.showHeader();
        else this.showImage();
    }

    get headerVisible() {
        return this.headerGridContainer.style.display !== 'none';
    }

    /** Zoom out to the whole image. */
    resetView() {
        if (this._zoom) d3.select(this.canvasContainer).call(this._zoom.transform, d3.zoomIdentity);
    }

    /** Current zoom factor (1 = whole image). */
    get zoomLevel() {
        return this.currentTransform.k;
    }

    /** Open or close the floating Z-scale (stretch) panel. */
    toggleZScalePanel() {
        if (this.imageData) this._showZScalePanel();
    }

    /** Open or close the floating histogram panel. */
    toggleHistogramPanel() {
        if (this.imageData) this._showHistogramPanel();
    }

    /**
     * Update viewer settings without reloading the image.
     * @param {object} settings
     */
    updateSettings(settings) {
        if (settings.autoZScale !== undefined) this.autoZScale = settings.autoZScale;
        if (settings.drawApertureCircles !== undefined) this.doDrawApertureCircles = settings.drawApertureCircles;
        if (settings.useGPU !== undefined) {
            this.useGPUSetting = settings.useGPU;
            if (this.imageData) {
                if (this.useGPUSetting) {
                    this.useWebGL = this._initWebGL(this.imageWidth, this.imageHeight, this.imageData);
                } else {
                    this.useWebGL = false;
                    this.offscreenCanvas = document.createElement('canvas');
                    this.offscreenCanvas.width = this.imageWidth;
                    this.offscreenCanvas.height = this.imageHeight;
                    this.offscreenCtx = this.offscreenCanvas.getContext('2d');
                }
                this._applyStretchFromInputs();
            }
            return;
        }
        if (this.imageData) {
            if (this.autoZScale) {
                this.autoZ.click();
            } else {
                this.stretchMin.value = this.stretchMin.min;
                this.stretchMax.value = this.stretchMax.max;
                this.stretchMin.dispatchEvent(new Event('input'));
                this.stretchMax.dispatchEvent(new Event('input'));
            }
        }
    }

    /**
     * Remove the viewer and clean up event listeners.
     */
    destroy() {
        if (this.gl) {
            const ext = this.gl.getExtension('WEBGL_lose_context');
            if (ext) ext.loseContext();
            this.gl = null;
        }
        this.closePanels();
        for (const [target, type, handler, opts] of this._listeners) {
            target.removeEventListener(type, handler, opts);
        }
        this._listeners = [];
        this._resizeObserver?.disconnect();
        this._mutationObserver?.disconnect();
        clearTimeout(this._longPressTimer);
        if (this._zoom) d3.select(this.canvasContainer).on('.zoom', null);
        this.container.classList.remove('sfv', 'sfv-compact');
        this.container.innerHTML = '';
    }

    /** Close the context menu and any floating panels. */
    closePanels() {
        if (this.activeContextMenu) this.activeContextMenu._close?.();
        for (const id of ['zScalePanel', 'histogramPanel', 'radialProfilePanel']) {
            const panel = document.getElementById(id);
            if (panel) (panel._close || (() => panel.remove()))();
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    // addEventListener that destroy() undoes
    _listen(target, type, handler, opts) {
        target.addEventListener(type, handler, opts);
        this._listeners.push([target, type, handler, opts]);
    }

    _showSpinner() {
        this.mainContainer.style.display = 'none';
        this.spinner.style.display = 'flex';
    }

    _headerForDisplay() {
        return this.headerDisplay || this.headerData || {};
    }

    // Image pixel to source pixel, for positions shown to the user
    _sourcePixel(v) {
        return this.binning > 1 ? Math.round(v * this.binning + (this.binning - 1) / 2) : v;
    }

    // ── DOM Construction ──────────────────────────────────────────────────────

    _buildDOM(container) {
        // Styles in style.css are scoped to .sfv. Its default size (100% x 100%) is
        // a class rule, so the host's own CSS for the container wins.
        container.classList.add('sfv');
        container.style.position = 'relative';

        container.innerHTML = `
            <div id="spinnerWrapper" style="position:absolute;inset:0;display:none;align-items:center;justify-content:center">
                <div id="spinner"></div>
            </div>
            <div class="mainContainer" style="display:none;width:100%;height:100%">
                <div id="controlsContainer" style="display:none">
                    <div id="stretchControls" class="stretch-controls">
                        <div class="stretch-header">
                            <span class="stretch-title">Z-scale</span>
                            <div class="stretch-header-actions">
                                <label class="stretch-log" for="logToggleButton" title="Apply logarithmic stretch">
                                    <input id="logToggleButton" type="checkbox"/>
                                    <span>Log</span>
                                </label>
                                <button id="autoZ" type="button">Auto</button>
                            </div>
                        </div>
                        <label class="stretch-field">
                            <div class="stretch-field-header">
                                <span>Min</span>
                                <span id="stretchMinValue">0.00</span>
                            </div>
                            <input id="stretchMin" type="range" step="any"/>
                        </label>
                        <label class="stretch-field">
                            <div class="stretch-field-header">
                                <span>Max</span>
                                <span id="stretchMaxValue">0.00</span>
                            </div>
                            <input id="stretchMax" type="range" step="any"/>
                        </label>
                        <label class="stretch-field">
                            <div class="stretch-field-header">
                                <span>Gamma</span>
                                <span id="stretchGammaValue">1.00</span>
                            </div>
                            <input id="stretchGamma" type="range" min="10" max="300" value="100"/>
                        </label>
                    </div>
                </div>
                <div id="imageGridContainer" class="image-grid-container grid-container">
                    <div id="canvasContainer">
                        <canvas id="loadedImage"></canvas>
                        <canvas id="gridLayer"></canvas>
                    </div>
                    <canvas id="yProfile" width="100"></canvas>
                    <canvas id="xProfile" height="100"></canvas>
                    <div class="info-corner" id="imageInfo">
                        <p id="headerTab" class="toggle-button">header &gt;</p>
                        <p id="pixelValue"></p>
                        <p id="pixelPosition"></p>
                        <p id="fwhmValue"></p>
                        <p id="gridShow" class="toggle-button">show grid</p>
                    </div>
                </div>
                <div id="headerGridContainer" class="header-grid-container grid-container" style="display:none">
                    <div class="header-container">
                        <div class="search-controls">
                            <input type="text" id="searchInput" placeholder="Search headers..."/>
                            <div id="resetButton" title="reset search">
                                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                                    <path stroke-linecap="round" stroke-linejoin="round" d="m9.75 9.75 4.5 4.5m0-4.5-4.5 4.5M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z"/>
                                </svg>
                            </div>
                        </div>
                        <table id="headerTable"></table>
                    </div>
                    <div></div>
                    <div style="display:flex;align-items:flex-end;justify-content:center;height:100%">
                        <p style="font-size:12px;opacity:0.25">Made with &#10084;&#65039; by <a href="https://www.ppp.one/" style="text-decoration-line:underline">Peter Pihlmann Pedersen</a></p>
                    </div>
                    <div class="info-corner">
                        <p id="returnButton" class="toggle-button">&lt; image</p>
                    </div>
                </div>
            </div>
        `;

        // Store DOM references
        const q = (id) => container.querySelector(id);
        this.spinner = q('#spinnerWrapper');
        this.canvas = q('#loadedImage');
        this.ctx = this.canvas.getContext('2d');
        this.gridCanvas = q('#gridLayer');
        this.gridCtx = this.gridCanvas.getContext('2d');
        this.xProfileCanvas = q('#xProfile');
        this.xProfileCtx = this.xProfileCanvas.getContext('2d');
        this.yProfileCanvas = q('#yProfile');
        this.yProfileCtx = this.yProfileCanvas.getContext('2d');
        this.canvasContainer = q('#canvasContainer');
        this.mainContainer = q('.mainContainer');
        this.headerTab = q('#headerTab');
        this.headerGridContainer = q('#headerGridContainer');
        this.imageGridContainer = q('#imageGridContainer');
        this.returnButton = q('#returnButton');
        this.headerTable = q('#headerTable');
        this.searchInput = q('#searchInput');
        this.resetButton = q('#resetButton');
        this.gridShow = q('#gridShow');
        this.autoZ = q('#autoZ');
        this.stretchMin = q('#stretchMin');
        this.stretchMax = q('#stretchMax');
        this.stretchMinValue = q('#stretchMinValue');
        this.stretchMaxValue = q('#stretchMaxValue');
        this.stretchGamma = q('#stretchGamma');
        this.stretchGammaValue = q('#stretchGammaValue');
        this.logToggleCheckbox = q('#logToggleButton');
        this.infoCorner = q('#imageInfo');
        this.pixelValueEl = q('#pixelValue');
        this.pixelPositionEl = q('#pixelPosition');
        this.fwhmValue = q('#fwhmValue');
    }

    // ── Event Wiring ──────────────────────────────────────────────────────────

    _wireEvents() {
        this._listen(this.headerTab, 'click', () => this.showHeader());
        this._listen(this.returnButton, 'click', () => this.showImage());

        if (this.options.keyboard) {
            this._listen(document, 'keydown', (event) => {
                if (event.key === 'ArrowLeft') {
                    this.showImage();
                } else if (event.key === 'ArrowRight') {
                    this.showHeader();
                } else if (event.key === 'Escape') {
                    this.searchInput.value = '';
                    this._displayHeaderTable(this._headerForDisplay());
                }
            });
        }

        // Escape in a non-empty search box clears it, and goes no further (so a
        // host page that closes the viewer on Escape does not close it)
        this._listen(this.searchInput, 'keydown', (event) => {
            if (event.key === 'Escape' && this.searchInput.value) {
                event.stopPropagation();
                this.searchInput.value = '';
                this._displayHeaderTable(this._headerForDisplay());
            }
        });

        this._listen(this.searchInput, 'input', () => {
            const query = this.searchInput.value.toLowerCase();
            const filteredData = Object.fromEntries(
                Object.entries(this._headerForDisplay()).filter(([key, value]) => {
                    const { value: v, comment } = splitHeaderValue(value);
                    return key.toLowerCase().includes(query) ||
                        v.toLowerCase().includes(query) ||
                        comment.toLowerCase().includes(query);
                })
            );
            this._displayHeaderTable(filteredData);
        });

        this._listen(this.resetButton, 'click', () => {
            this.searchInput.value = '';
            this._displayHeaderTable(this._headerForDisplay());
        });

        // Follow the container size (window resizes, host layout changes, rotation)
        this._resizeObserver = new ResizeObserver(() => {
            cancelAnimationFrame(this._resizeFrame);
            this._resizeFrame = requestAnimationFrame(() => this._handleResize());
        });
        this._resizeObserver.observe(this.container);

        this._listen(this.canvasContainer, 'mousemove', (event) => {
            if (this.imageData) this._imageInteractionHandler(event, this.imageWidth, this.imageHeight);
        });

        this._listen(this.canvasContainer, 'contextmenu', (event) => {
            event.preventDefault();
            // A touch long press already opened the menu
            if (Date.now() - (this._longPressAt || 0) < 1000) return;
            this._showContextMenu(event);
        });
        this._wireLongPress();

        this._listen(this.gridShow, 'click', () => {
            if (!this.wcs) return;
            this.showGrid = !this.showGrid;
            if (this.showGrid) {
                this._drawWCSGrid();
                this.gridShow.textContent = 'hide grid';
            } else {
                this.gridCtx.clearRect(0, 0, this.gridCtx.canvas.width, this.gridCtx.canvas.height);
                this.gridShow.textContent = 'show grid';
            }
        });

        this._mutationObserver = new MutationObserver(() => this._updateColor());
        this._mutationObserver.observe(document.body, { childList: false, attributes: true });

        // Stretch controls
        this.autoZ.addEventListener('click', () => {
            if (!this.imageData) return;
            const z = zscale(this.imageData, this.histogram, true);
            const pmin = percentileForValue(this.histogram, z.vmin);
            const pmax = percentileForValue(this.histogram, z.vmax);
            this.stretchMin.value = pmin;
            this.stretchMax.value = pmax;
            this.stretchMinValue.textContent = `${pmin.toFixed(2)}% → ${z.vmin.toFixed(2)}`;
            this.stretchMaxValue.textContent = `${pmax.toFixed(2)}% → ${z.vmax.toFixed(2)}`;
            this.stretchGamma.value = 100;
            this.stretchGammaValue.textContent = (1.0).toFixed(2);
            this.logToggleCheckbox.checked = false;
            this._applyStretchFromInputs();
        });

        this.stretchMin.addEventListener('input', (e) => {
            const p = Number(e.target.value);
            const v = valueForPercentile(this.histogram, p);
            this.stretchMinValue.textContent = `${p.toFixed(2)}% → ${v.toFixed(2)}`;
            this._applyStretchFromInputs();
        });

        this.stretchMax.addEventListener('input', (e) => {
            const p = Number(e.target.value);
            const v = valueForPercentile(this.histogram, p);
            this.stretchMaxValue.textContent = `${p.toFixed(2)}% → ${v.toFixed(2)}`;
            this._applyStretchFromInputs();
        });

        this.stretchGamma.addEventListener('input', (e) => {
            const g = parseInt(e.target.value, 10) / 100;
            this.stretchGammaValue.textContent = g.toFixed(2);
            this._applyStretchFromInputs();
        });

        this.logToggleCheckbox.addEventListener('change', () => {
            this._applyStretchFromInputs();
        });
    }

    // Touch screens have no right click: holding one finger still opens the context menu
    _wireLongPress() {
        let start = null;
        const cancel = () => {
            clearTimeout(this._longPressTimer);
            start = null;
        };
        this._listen(this.canvasContainer, 'pointerdown', (e) => {
            if (e.pointerType !== 'touch') return;
            if (start) { cancel(); return; } // a second finger: pinch, not a long press
            start = { x: e.clientX, y: e.clientY, id: e.pointerId };
            this._longPressTimer = setTimeout(() => {
                if (!start || !this.imageData) return;
                this._longPressAt = Date.now();
                this._showContextMenu({ clientX: start.x, clientY: start.y });
                start = null;
            }, LONG_PRESS_MS);
        });
        this._listen(this.canvasContainer, 'pointermove', (e) => {
            if (start && e.pointerId === start.id &&
                Math.hypot(e.clientX - start.x, e.clientY - start.y) > LONG_PRESS_MOVE_PX) cancel();
        });
        for (const type of ['pointerup', 'pointercancel']) this._listen(this.canvasContainer, type, cancel);
    }

    // ── Core Pipeline ─────────────────────────────────────────────────────────

    _processImage({ header, width, height, data, dataMin, dataMax }) {
        console.time('renderMonochromeImage');
        this.headerData = header || {};
        this.imageWidth = width;
        this.imageHeight = height;
        this.imageData = data;

        if (!Number.isFinite(dataMin) || !Number.isFinite(dataMax)) {
            dataMin = Infinity; dataMax = -Infinity;
            for (let i = 0; i < data.length; i++) {
                const v = data[i];
                if (v < dataMin && v !== -Infinity) dataMin = v;
                if (v > dataMax && v !== Infinity) dataMax = v;
            }
        }

        console.time('buildHistogram');
        this.histogram = buildHistogram(this.imageData, HISTOGRAM_BINS, dataMin, dataMax);
        console.timeEnd('buildHistogram');

        try {
            this.wcs = new WCS(this.headerData);
        } catch (err) {
            console.log('No usable WCS:', err.message || err);
            this.wcs = null;
        }

        this._updateGridToggleVisibility(Boolean(this.wcs));

        this.plateScale = null;
        if (this.wcs) {
            const cd = this.wcs.cd;
            const wcsScale = Math.sqrt(cd[0][0] ** 2 + cd[1][0] ** 2) * 3600;
            if (wcsScale > 0) {
                this.plateScale = wcsScale;
                console.log('Plate scale from WCS:', this.plateScale);
            }
        }
        if (!this.plateScale) {
            this.plateScale = plateScaleFromPixelSize(this.headerData);
        }

        this._lastFwhmResult = null;
        this._lastFwhmValid = false;
        this.fwhmValue.textContent = this.binning > 1 ? 'FWHM: full res only' : '';
        this.fwhmValue.title = this.binning > 1
            ? 'This image is a downsampled preview, which widens stars. Load the full-resolution image to measure them.'
            : 'FWHM of the star under the cursor';

        this.canvas.width = this.imageWidth;
        this.canvas.height = this.imageHeight;
        this.canvas.imageSmoothingEnabled = false;

        this.useWebGL = this.useGPUSetting ? this._initWebGL(this.imageWidth, this.imageHeight, this.imageData) : false;
        if (this.useWebGL) {
            console.log('WebGL acceleration enabled');
        } else {
            console.log('WebGL unavailable, using CPU rendering');
            this.offscreenCanvas = document.createElement('canvas');
            this.offscreenCanvas.width = this.imageWidth;
            this.offscreenCanvas.height = this.imageHeight;
            this.offscreenCtx = this.offscreenCanvas.getContext('2d');
            // Initial CPU render will happen via _applyStretchFromInputs below
        }

        // Initialize stretch sliders
        try {
            const z = zscale(this.imageData, this.histogram, this.autoZScale);

            this.stretchMin.min = 0;
            this.stretchMin.max = 100;
            this.stretchMax.min = 0;
            this.stretchMax.max = 100;

            const pmin = percentileForValue(this.histogram, z.vmin);
            const pmax = percentileForValue(this.histogram, z.vmax);
            this.stretchMin.value = pmin;
            this.stretchMax.value = pmax;
            this.stretchMinValue.textContent = `${pmin.toFixed(2)}% → ${z.vmin.toFixed(2)}`;
            this.stretchMaxValue.textContent = `${pmax.toFixed(2)}% → ${z.vmax.toFixed(2)}`;
            this.stretchGamma.value = 100;
            this.stretchGammaValue.textContent = (1.0).toFixed(2);
            this.logToggleCheckbox.checked = false;
        } catch (err) {
            console.warn('Failed to initialize stretch sliders:', err);
        }

        // Refresh header table with new data
        this.headerTable.innerHTML = '';
        if (this.headerGridContainer.style.display !== 'none') {
            this._displayHeaderTable(this._headerForDisplay());
        }

        // Show the viewer, then size the canvas to the space it has
        this.spinner.style.display = 'none';
        this.mainContainer.style.display = 'flex';
        this.mainContainer.style.justifyContent = 'center';
        this._handleResize();
        this._applyStretchFromInputs();

        console.timeEnd('renderMonochromeImage');
    }

    // Fit the image into the container, keeping its aspect ratio
    _handleResize() {
        if (!this.imageWidth || !this.imageHeight) return;
        const cw = this.container.clientWidth;
        const ch = this.container.clientHeight;
        if (!cw || !ch) return; // hidden: the ResizeObserver calls again once shown

        this.compact = cw < COMPACT_WIDTH || ch < COMPACT_HEIGHT;
        this.container.classList.toggle('sfv-compact', this.compact);
        if (this.imageGridContainer.style.display === 'none') return; // header view

        // Compact: no profiles, readout bar under the image instead
        const profile = this.compact ? 0 : PROFILE_SIZE;
        const readout = this.compact ? (this.infoCorner.offsetHeight || 28) : 0;
        const scale = Math.min(
            Math.max(1, cw - profile) / this.imageWidth,
            Math.max(1, ch - profile - readout) / this.imageHeight
        );
        const displayWidth = Math.max(1, Math.floor(this.imageWidth * scale));
        const displayHeight = Math.max(1, Math.floor(this.imageHeight * scale));

        for (const el of [this.canvasContainer, this.canvas, this.gridCanvas]) {
            el.style.width = `${displayWidth}px`;
            el.style.height = `${displayHeight}px`;
        }
        this.gridCanvas.width = displayWidth;
        this.gridCanvas.height = displayHeight;
        this.xProfileCanvas.width = displayWidth;
        this.yProfileCanvas.height = displayHeight;

        this.rect = this.canvas.getBoundingClientRect();
        this.scaleX = this.canvas.width / displayWidth;
        this.scaleY = this.canvas.height / displayHeight;

        if (this.showGrid) this._drawWCSGrid();
    }

    // ── Rendering ─────────────────────────────────────────────────────────────

    // Blit the offscreen canvas (already stretched) onto the main canvas
    // using the current pan/zoom transform.
    _blitToCanvas() {
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        this.ctx.save();
        this.ctx.imageSmoothingEnabled = false;
        this.ctx.translate(this.currentTransform.x * this.scaleX, this.currentTransform.y * this.scaleY);
        this.ctx.scale(this.currentTransform.k, this.currentTransform.k);
        this.ctx.drawImage(this.offscreenCanvas, 0, 0, this.imageWidth, this.imageHeight);
        this.ctx.restore();
    }

    // Map image coordinates to the canvas. Row 0 is at the bottom, and pixel
    // (x, y) covers [x, x + 1) x [y, y + 1).
    _applyFITSImageTransform(ctx) {
        ctx.translate(this.currentTransform.x * this.scaleX, this.currentTransform.y * this.scaleY);
        ctx.scale(this.currentTransform.k, this.currentTransform.k);
        ctx.translate(0, this.imageHeight);
        ctx.scale(1, -1);
    }

    _applyStretchFromInputs() {
        const pmin = parseFloat(this.stretchMin.value);
        const pmax = parseFloat(this.stretchMax.value);
        const vmin = valueForPercentile(this.histogram, pmin);
        const vmax = valueForPercentile(this.histogram, pmax);
        const gamma = parseInt(this.stretchGamma.value, 10) / 100;
        const useLog = this.logToggleCheckbox.checked;
        this._applyStretch(vmin, vmax, gamma, useLog);
    }

    _applyStretch(vmin, vmax, gamma = 1.0, useLog = false) {
        if (!this.imageData) return;

        if (this.useWebGL && this.gl) {
            this._glRenderStretch(vmin, vmax, gamma <= 0 ? 1 : gamma, useLog);
        } else {
            if (!this.offscreenCtx) return;
            const { imageWidth: width, imageHeight: height } = this;
            const pixels = this.offscreenCtx.createImageData(width, height);
            const out = new Uint32Array(pixels.data.buffer); // one RGBA pixel per element
            const range = vmax - vmin || 1;
            const lut = buildStretchLut(range, gamma <= 0 ? 1 : gamma, useLog);
            const last = lut.length - 1;
            const scale = last / range;

            for (let srcY = 0; srcY < height; srcY++) {
                const srcRow = srcY * width;
                const dstRow = (height - 1 - srcY) * width;
                for (let x = 0; x < width; x++) {
                    const t = (this.imageData[srcRow + x] - vmin) * scale;
                    // NaN fails `t > 0` and maps to black
                    out[dstRow + x] = lut[t > 0 ? (t < last ? t | 0 : last) : 0];
                }
            }
            this.offscreenCtx.putImageData(pixels, 0, 0);
        }

        this._blitToCanvas();
    }

    _glRenderStretch(vmin, vmax, gamma, useLog) {
        this.gl.uniform1f(this.glUniforms.vmin, vmin);
        this.gl.uniform1f(this.glUniforms.vmax, vmax);
        this.gl.uniform1f(this.glUniforms.gamma, gamma);
        this.gl.uniform1f(this.glUniforms.useLog, useLog ? 1.0 : 0.0);
        this.gl.drawArrays(this.gl.TRIANGLES, 0, 6);
    }

    _initWebGL(width, height, floatData) {
        this.offscreenCanvas = document.createElement('canvas');
        this.offscreenCanvas.width = width;
        this.offscreenCanvas.height = height;

        this.gl = this.offscreenCanvas.getContext('webgl', { preserveDrawingBuffer: true });
        if (!this.gl) return false;

        // Larger images than the GPU's texture limit would render black
        const maxSize = this.gl.getParameter(this.gl.MAX_TEXTURE_SIZE);
        if (width > maxSize || height > maxSize) {
            console.log(`Image ${width}x${height} exceeds MAX_TEXTURE_SIZE ${maxSize}; using CPU rendering`);
            this.gl = null;
            return false;
        }

        const ext = this.gl.getExtension('OES_texture_float');
        if (!ext) { this.gl = null; return false; }

        const compile = (type, src) => {
            const s = this.gl.createShader(type);
            this.gl.shaderSource(s, src);
            this.gl.compileShader(s);
            if (!this.gl.getShaderParameter(s, this.gl.COMPILE_STATUS)) {
                console.error(this.gl.getShaderInfoLog(s));
                return null;
            }
            return s;
        };
        const vs = compile(this.gl.VERTEX_SHADER, VERT_SRC);
        const fs = compile(this.gl.FRAGMENT_SHADER, FRAG_SRC);
        if (!vs || !fs) { this.gl = null; return false; }

        this.glProgram = this.gl.createProgram();
        this.gl.attachShader(this.glProgram, vs);
        this.gl.attachShader(this.glProgram, fs);
        this.gl.linkProgram(this.glProgram);
        if (!this.gl.getProgramParameter(this.glProgram, this.gl.LINK_STATUS)) {
            console.error(this.gl.getProgramInfoLog(this.glProgram));
            this.gl = null;
            return false;
        }
        this.gl.useProgram(this.glProgram);

        const buf = this.gl.createBuffer();
        this.gl.bindBuffer(this.gl.ARRAY_BUFFER, buf);
        this.gl.bufferData(this.gl.ARRAY_BUFFER, new Float32Array([
            -1, -1, 1, -1, -1, 1,
            -1, 1, 1, -1, 1, 1
        ]), this.gl.STATIC_DRAW);
        const aPos = this.gl.getAttribLocation(this.glProgram, 'a_pos');
        this.gl.enableVertexAttribArray(aPos);
        this.gl.vertexAttribPointer(aPos, 2, this.gl.FLOAT, false, 0, 0);

        const tex = this.gl.createTexture();
        this.gl.bindTexture(this.gl.TEXTURE_2D, tex);
        this.gl.texParameteri(this.gl.TEXTURE_2D, this.gl.TEXTURE_MIN_FILTER, this.gl.NEAREST);
        this.gl.texParameteri(this.gl.TEXTURE_2D, this.gl.TEXTURE_MAG_FILTER, this.gl.NEAREST);
        this.gl.texParameteri(this.gl.TEXTURE_2D, this.gl.TEXTURE_WRAP_S, this.gl.CLAMP_TO_EDGE);
        this.gl.texParameteri(this.gl.TEXTURE_2D, this.gl.TEXTURE_WRAP_T, this.gl.CLAMP_TO_EDGE);
        this.gl.texImage2D(this.gl.TEXTURE_2D, 0, this.gl.LUMINANCE, width, height, 0,
            this.gl.LUMINANCE, this.gl.FLOAT, new Float32Array(floatData));

        this.glUniforms = {
            vmin: this.gl.getUniformLocation(this.glProgram, 'u_vmin'),
            vmax: this.gl.getUniformLocation(this.glProgram, 'u_vmax'),
            gamma: this.gl.getUniformLocation(this.glProgram, 'u_gamma'),
            useLog: this.gl.getUniformLocation(this.glProgram, 'u_useLog'),
        };

        this.gl.viewport(0, 0, width, height);
        return true;
    }

    // ── Zoom / Pan ────────────────────────────────────────────────────────────

    // Created once; the zoom carries over between images (compare the same region)
    _setupZoom() {
        if (this._zoom) return;
        const zoom = d3.zoom()
            .scaleExtent([1, MAX_ZOOM])
            .on('zoom', (event) => {
                let transform = event.transform;

                const right = (transform.x - this.imageWidth / this.scaleX) / (transform.k / this.scaleX) + this.imageWidth;
                const left = transform.x / (transform.k / this.scaleX);
                const top = transform.y / (transform.k / this.scaleY);
                const bottom = (transform.y - this.imageHeight / this.scaleY) / (transform.k / this.scaleY) + this.imageHeight;

                if (left > 0) transform.x = 0;
                if (top > 0) transform.y = 0;
                if (right < 0) transform.x = (left * transform.k) / this.scaleX - (right * transform.k) / this.scaleX;
                if (bottom < 0) transform.y = (top * transform.k) / this.scaleY - (bottom * transform.k) / this.scaleY;

                this.currentTransform = transform;
                this._blitToCanvas();
                if (this.showGrid) this._drawWCSGrid();
                this._drawCachedAperture();

                if (event.sourceEvent) {
                    this._imageInteractionHandler(event.sourceEvent, this.imageWidth, this.imageHeight);
                }
            });

        this._zoom = zoom;
        d3.select(this.canvasContainer).call(zoom);
    }

    // ── Interaction ───────────────────────────────────────────────────────────

    _imageInteractionHandler(event, width, height) {
        if (!Number.isFinite(event.clientX)) return; // touch events from the zoom handler
        // The host layout can move the canvas, so read its position every time
        this.rect = this.canvas.getBoundingClientRect();
        const x = Math.floor((event.clientX - this.rect.left) * this.scaleX);
        const y = Math.floor((event.clientY - this.rect.top) * this.scaleY);

        const transformedX = Math.floor((x - this.currentTransform.x * this.scaleX) / this.currentTransform.k);
        const transformedDisplayY = Math.floor((y - this.currentTransform.y * this.scaleY) / this.currentTransform.k);
        const transformedY = height - 1 - transformedDisplayY;

        const xWidth = Math.ceil(width / this.currentTransform.k);
        const yHeight = Math.ceil(height / this.currentTransform.k);
        const left = Math.floor((-this.currentTransform.x * this.scaleX) / this.currentTransform.k);
        const top = Math.floor((-this.currentTransform.y * this.scaleY) / this.currentTransform.k);

        if (transformedX >= 0 && transformedX < width && transformedDisplayY >= 0 && transformedDisplayY < height) {
            if (!this.compact) {
                const xProfile = this.imageData.slice(
                    transformedY * width + left,
                    transformedY * width + left + xWidth + 1
                );
                const yLen = Math.min(yHeight + 1, height - top);
                const yProfile = new Float32Array(yLen);
                for (let i = 0; i < yLen; i++) {
                    const imageY = height - 1 - (top + i);
                    yProfile[i] = this.imageData[imageY * width + transformedX];
                }

                this._drawLineProfile(this.xProfileCtx, xProfile, true,
                    (-this.currentTransform.x * this.scaleX) / this.currentTransform.k - left);
                this._drawLineProfile(this.yProfileCtx, yProfile, false,
                    (-this.currentTransform.y * this.scaleY) / this.currentTransform.k - top);
            }

            const pixelValue = formatNumber(this.imageData[transformedY * width + transformedX], 2);
            this.pixelValueEl.innerText = `${pixelValue}`;
            this.pixelPositionEl.innerText = `${this._sourcePixel(transformedX)}, ${this._sourcePixel(transformedY)}`;

            // Binning widens stars, so a FWHM measured on a downsampled preview is wrong
            if (this.binning > 1) return;

            // Throttle FWHM: expensive calculation, limit to FWHM_THROTTLE_MS intervals
            const now = performance.now();
            if (now - this._lastFwhmTime >= FWHM_THROTTLE_MS) {
                this._lastFwhmTime = now;

                const fwhmResult = this._measureStar(transformedX, transformedY);
                this._lastFwhmResult = fwhmResult;
                this._lastFwhmValid = fwhmResult.valid;

                this._blitToCanvas();
                this._drawCachedAperture();

                if (fwhmResult.valid) {
                    let fwhm = fwhmResult.fwhm;
                    if (this.plateScale) {
                        fwhm *= this.plateScale;
                        this.fwhmValue.innerText = `FWHM: ${formatNumber(fwhm, 2)} "`;
                    } else {
                        this.fwhmValue.innerText = `FWHM: ${formatNumber(fwhm, 2)} px`;
                    }
                } else {
                    this.fwhmValue.innerText = 'FWHM: -';
                }
            }
        }
    }

    // Measure the star nearest to image pixel (x, y). `valid` is false for noise and hot pixels.
    _measureStar(x, y) {
        // Box of FWHM_BOX_SIZE_ARCSEC when the plate scale is known, but at least
        // FWHM_BOX_SIZE_PX (the background border must clear the star) and at most
        // FWHM_BOX_SIZE_MAX_PX (cost)
        const boxSizePx = Math.min(FWHM_BOX_SIZE_MAX_PX, Math.max(
            FWHM_BOX_SIZE_PX,
            this.plateScale ? Math.ceil(FWHM_BOX_SIZE_ARCSEC / this.plateScale) : 0
        ));
        const result = calculateAdaptiveFWHM(
            x, y, null, this.imageData, this.imageWidth, this.imageHeight,
            { boxSizePx, maxBoxSizePx: FWHM_BOX_SIZE_MAX_PX }
        );
        result.snr = result.backgroundSigma > 0
            ? (result.peak - result.background) / result.backgroundSigma
            : (result.background > 0 ? (result.peak / result.background - 1) * 100 : 0);
        result.valid = result.snr > SNR_THRESHOLD && result.fwhm > 0 && result.sharpness > SHARPNESS_THRESHOLD;
        return result;
    }

    _drawCachedAperture() {
        if (!this._lastFwhmValid || !this._lastFwhmResult || !this.doDrawApertureCircles) return;
        this.ctx.save();
        this._applyFITSImageTransform(this.ctx);
        drawApertureCircles(this._lastFwhmResult, this.scaleX / this.currentTransform.k, this.ctx);
        this.ctx.restore();
    }

    // Pointer position in image coordinates: pixel (x, y) covers [x, x + 1) x [y, y + 1)
    _eventToImageCoords(clientX, clientY) {
        if (!this.imageData) return null;
        this.rect = this.canvas.getBoundingClientRect();
        const x = (clientX - this.rect.left) * this.scaleX;
        const y = (clientY - this.rect.top) * this.scaleY;
        const px = (x - this.currentTransform.x * this.scaleX) / this.currentTransform.k;
        const displayY = (y - this.currentTransform.y * this.scaleY) / this.currentTransform.k;
        const py = this.imageHeight - displayY;
        return { px, py };
    }

    // ── Context Menu ──────────────────────────────────────────────────────────

    _showContextMenu(event) {
        this.activeContextMenu?._close?.();

        // Larger targets on touch screens
        const coarse = Boolean(window.matchMedia?.('(pointer: coarse)').matches);
        const contextMenu = document.createElement('div');
        contextMenu.className = 'sfv-menu';
        contextMenu.setAttribute('role', 'menu');
        contextMenu.style.cssText = `position:fixed;top:${event.clientY}px;left:${event.clientX}px;background:var(--sfv-panel-bg, #1e1e1e);color:var(--sfv-panel-fg, #d4d4d4);border:1px solid var(--sfv-panel-border, #555);padding:4px;box-shadow:0 4px 12px rgba(0,0,0,0.5);border-radius:5px;z-index:2100;min-width:180px;font-family:Arial, sans-serif`;

        const addItem = (text, onClick, { disabled = false } = {}) => {
            const item = document.createElement('div');
            item.setAttribute('role', 'menuitem');
            item.textContent = text;
            item.style.cssText = `cursor:${disabled ? 'default' : 'pointer'};padding:${coarse ? '12px 14px' : '4px 8px'};border-radius:3px;font-size:${coarse ? 15 : 13}px${disabled ? ';opacity:0.45' : ''}`;
            if (disabled) {
                item.setAttribute('aria-disabled', 'true');
            } else {
                item.addEventListener('mouseenter', () => { item.style.background = 'var(--sfv-panel-hover, #2d2d2d)'; });
                item.addEventListener('mouseleave', () => { item.style.background = ''; });
                item.addEventListener('click', onClick);
            }
            contextMenu.appendChild(item);
            return item;
        };

        const closeMenu = () => contextMenu._close();

        const addSep = () => {
            const sep = document.createElement('div');
            sep.style.cssText = 'height:1px;background:var(--sfv-panel-border, #444);margin:3px 0';
            contextMenu.appendChild(sep);
        };

        const coords = this._eventToImageCoords(event.clientX, event.clientY);
        const inside = coords && coords.px >= 0 && coords.px < this.imageWidth && coords.py >= 0 && coords.py < this.imageHeight;

        if (this.imageData) {
            if (this.binning > 1) {
                addItem('Measure star (full resolution only)', null, { disabled: true });
            } else if (inside) {
                addItem('Measure star', () => {
                    closeMenu();
                    const mResult = this._measureStar(Math.floor(coords.px), Math.floor(coords.py));
                    this._showRadialProfilePanel(mResult);
                });
            }
            addItem('Z-scale', () => { closeMenu(); this._showZScalePanel(); });
            addItem('Histogram', () => { closeMenu(); this._showHistogramPanel(); });
            if (this.currentTransform.k > 1) {
                addItem('Reset zoom', () => { closeMenu(); this.resetView(); });
            }
            addSep();
        }

        if (navigator.clipboard?.write && window.ClipboardItem) {
            const copyItem = addItem('Copy image to clipboard', async () => {
                copyItem.textContent = 'Copying...';
                try {
                    this.offscreenCanvas.toBlob(async (blob) => {
                        try {
                            await navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })]);
                            copyItem.textContent = 'Copied!';
                        } catch (err) {
                            copyItem.textContent = 'Failed to copy';
                        }
                        setTimeout(() => { copyItem.textContent = 'Copy image to clipboard'; }, 1000);
                    }, 'image/png');
                } catch (err) {
                    copyItem.textContent = 'Failed to copy';
                    setTimeout(() => { copyItem.textContent = 'Copy image to clipboard'; }, 1000);
                }
            });
        }

        const saveItem = addItem('Save image as png', () => {
            saveItem.textContent = 'Preparing image...';
            requestAnimationFrame(() => {
                this.offscreenCanvas.toBlob((blob) => {
                    const url = URL.createObjectURL(blob);
                    const link = document.createElement('a');
                    link.href = url;
                    link.download = `${this.filename}.png`;
                    link.click();
                    setTimeout(() => {
                        URL.revokeObjectURL(url);
                        saveItem.textContent = 'Save image as png';
                    }, 100);
                }, 'image/png');
            });
        });

        if (this.wcs && inside) {
            addSep();
            addItem('Query SIMBAD here', () => {
                closeMenu();
                // WCS uses FITS pixel coordinates (centre of the first pixel at 1)
                const radec = this.wcs.sipPixelxy2radec(coords.px + 0.5, coords.py + 0.5);
                if (!radec) return;
                const [raDeg, decDeg] = radec;
                const url = this._buildSimbadUrl(raDeg, decDeg);
                this.options.onOpenExternal(url);
            });
        }

        document.body.appendChild(contextMenu);
        this.activeContextMenu = contextMenu;

        // Keep the menu inside the viewport
        const r = contextMenu.getBoundingClientRect();
        if (r.right > window.innerWidth - 4) contextMenu.style.left = `${Math.max(4, window.innerWidth - r.width - 4)}px`;
        if (r.bottom > window.innerHeight - 4) contextMenu.style.top = `${Math.max(4, window.innerHeight - r.height - 4)}px`;

        // Close on a press outside the menu (not on click: lifting the finger after
        // a long press must not close it) or on Escape
        const onPointerDown = (e) => { if (!contextMenu.contains(e.target)) closeMenu(); };
        const onKey = (e) => {
            if (e.key !== 'Escape') return;
            e.stopPropagation();
            closeMenu();
        };
        contextMenu._close = () => {
            contextMenu.remove();
            if (this.activeContextMenu === contextMenu) this.activeContextMenu = null;
            document.removeEventListener('pointerdown', onPointerDown, true);
            document.removeEventListener('keydown', onKey, true);
        };
        document.addEventListener('pointerdown', onPointerDown, true);
        document.addEventListener('keydown', onKey, true);
    }

    // Floating panel with a title bar: draggable on wide screens, a bottom sheet on
    // narrow ones. Dragging uses pointer capture, so it works with touch and leaves
    // no listeners behind. `onClose(fn)` registers cleanup for panel._close().
    _createPanel(id, title, { width, side = 'right' }) {
        const narrow = window.innerWidth < 600;
        const coarse = Boolean(window.matchMedia?.('(pointer: coarse)').matches);
        const panel = document.createElement('div');
        panel.id = id;
        panel.className = 'sfv sfv-panel';
        Object.assign(panel.style, {
            position: 'fixed', zIndex: '2000', boxSizing: 'border-box',
            background: 'var(--sfv-panel-bg, #1e1e1e)', color: 'var(--sfv-panel-fg, #d4d4d4)',
            border: '1px solid var(--sfv-panel-border, #555)', borderRadius: '6px',
            boxShadow: '0 4px 24px rgba(0,0,0,0.6)',
            fontFamily: 'Arial, sans-serif', fontSize: '13px', userSelect: 'none',
            maxHeight: narrow ? '55vh' : 'calc(100vh - 80px)', overflowY: 'auto',
        });
        if (narrow) Object.assign(panel.style, { left: '8px', right: '8px', bottom: '8px' });
        else Object.assign(panel.style, { top: '60px', [side]: '20px', width: `${width}px` });

        const hdr = document.createElement('div');
        Object.assign(hdr.style, {
            background: 'var(--sfv-panel-header-bg, #2d2d2d)', padding: '5px 10px',
            borderRadius: '6px 6px 0 0', cursor: narrow ? 'default' : 'move', touchAction: 'none',
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            borderBottom: '1px solid var(--sfv-panel-border, #444)',
            position: 'sticky', top: '0',
        });
        const titleSpan = document.createElement('span');
        titleSpan.style.cssText = 'font-weight:bold;font-size:12px';
        titleSpan.textContent = title;
        const closeBtn = document.createElement('button');
        closeBtn.type = 'button';
        closeBtn.textContent = '\u00d7';
        closeBtn.setAttribute('aria-label', `Close ${title}`);
        Object.assign(closeBtn.style, {
            background: 'none', border: 'none', color: 'var(--sfv-panel-muted, #aaa)', cursor: 'pointer', lineHeight: '1',
            fontSize: coarse ? '24px' : '16px', padding: coarse ? '4px 8px' : '0 2px',
        });
        hdr.appendChild(titleSpan);
        hdr.appendChild(closeBtn);
        panel.appendChild(hdr);

        let drag = null;
        hdr.addEventListener('pointerdown', (e) => {
            if (narrow || e.target === closeBtn) return;
            const br = panel.getBoundingClientRect();
            drag = { dx: e.clientX - br.left, dy: e.clientY - br.top };
            hdr.setPointerCapture(e.pointerId);
            e.preventDefault();
        });
        hdr.addEventListener('pointermove', (e) => {
            if (!drag) return;
            panel.style.left = `${e.clientX - drag.dx}px`;
            panel.style.top = `${e.clientY - drag.dy}px`;
            panel.style.right = 'auto';
        });
        const endDrag = () => { drag = null; };
        hdr.addEventListener('pointerup', endDrag);
        hdr.addEventListener('pointercancel', endDrag);

        const body = document.createElement('div');
        panel.appendChild(body);

        let cleanup = null;
        panel._close = () => {
            cleanup?.();
            panel.remove();
        };
        closeBtn.onclick = panel._close;
        return { panel, body, titleSpan, onClose: (fn) => { cleanup = fn; } };
    }

    _showZScalePanel() {
        const existing = document.getElementById('zScalePanel');
        if (existing) { existing._close?.(); return; }

        const stretchControlsEl = this.container.querySelector('#stretchControls');
        const originalParent = stretchControlsEl.parentNode;
        const originalNext = stretchControlsEl.nextSibling;

        const { panel, body, onClose } = this._createPanel('zScalePanel', 'Z-scale', { width: 260, side: 'left' });
        body.style.padding = '10px 12px';
        stretchControlsEl.style.display = 'flex';
        stretchControlsEl.style.width = '100%';
        body.appendChild(stretchControlsEl);

        onClose(() => {
            stretchControlsEl.style.display = 'none';
            stretchControlsEl.style.width = '';
            if (originalNext) {
                originalParent.insertBefore(stretchControlsEl, originalNext);
            } else {
                originalParent.appendChild(stretchControlsEl);
            }
        });
        document.body.appendChild(panel);
    }

    _showHistogramPanel() {
        const existing = document.getElementById('histogramPanel');
        if (existing) { existing._close?.(); return; }

        const { panel, body: pbody } = this._createPanel('histogramPanel', 'Histogram', { width: 340, side: 'right' });
        pbody.style.padding = '8px 10px';

        const bitpixRaw = this.headerData['BITPIX'] ? parseInt(this.headerData['BITPIX'].split('/')[0]) : null;
        const isIntegerBitpix = bitpixRaw && bitpixRaw > 0;
        const bitpixXMin = 0;
        const bitpixXMax = isIntegerBitpix ? Math.pow(2, bitpixRaw) - 1 : this.histogram.max;
        const dataXMin = this.histogram.min;
        const dataXMax = this.histogram.max;

        let useBitpixRange = isIntegerBitpix;
        let histXMin = useBitpixRange ? bitpixXMin : dataXMin;
        let histXMax = useBitpixRange ? bitpixXMax : dataXMax;

        const histW = 300, histH = 150;
        const padL = 50, padR = 10, padT = 5, padB = 25;
        const plotW = histW - padL - padR;
        const plotH = histH - padT - padB;

        const histHeader = document.createElement('div');
        histHeader.style.cssText = 'display:flex;justify-content:space-between;align-items:center;font-size:12px;font-weight:600;margin-bottom:4px';

        const histToggleGroup = document.createElement('div');
        histToggleGroup.style.cssText = 'display:flex;align-items:center;gap:8px';

        const histLogLabel = document.createElement('label');
        histLogLabel.style.cssText = 'display:flex;align-items:center;gap:4px;font-size:12px;cursor:pointer;font-weight:normal;color:#aaa';
        const histLogCheckbox = document.createElement('input');
        histLogCheckbox.type = 'checkbox';
        histLogCheckbox.checked = true;
        histLogLabel.appendChild(histLogCheckbox);
        histLogLabel.appendChild(document.createTextNode('Log'));
        histToggleGroup.appendChild(histLogLabel);

        let histBitpixCheckbox = null;
        if (isIntegerBitpix) {
            const histBitpixLabel = document.createElement('label');
            histBitpixLabel.style.cssText = 'display:flex;align-items:center;gap:4px;font-size:12px;cursor:pointer;font-weight:normal;color:#aaa';
            histBitpixCheckbox = document.createElement('input');
            histBitpixCheckbox.type = 'checkbox';
            histBitpixCheckbox.checked = useBitpixRange;
            histBitpixLabel.appendChild(histBitpixCheckbox);
            histBitpixLabel.appendChild(document.createTextNode('BITPIX scale'));
            histToggleGroup.appendChild(histBitpixLabel);
        }

        histHeader.appendChild(histToggleGroup);
        pbody.appendChild(histHeader);

        const histCanvas = document.createElement('canvas');
        const dpr = window.devicePixelRatio || 1;
        histCanvas.width = histW * dpr;
        histCanvas.height = histH * dpr;
        histCanvas.style.cssText = `width:${histW}px;height:${histH}px;display:block;cursor:crosshair`;
        pbody.appendChild(histCanvas);

        const histInfo = document.createElement('div');
        histInfo.style.cssText = 'font-size:11px;color:#888;min-height:16px;margin-top:2px';
        pbody.appendChild(histInfo);

        const histCtx = histCanvas.getContext('2d');
        histCtx.scale(dpr, dpr);

        function niceNum(range, round) {
            const exp = Math.floor(Math.log10(range));
            const frac = range / Math.pow(10, exp);
            let nice;
            if (round) { nice = frac < 1.5 ? 1 : frac < 3 ? 2 : frac < 7 ? 5 : 10; }
            else { nice = frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 5 ? 5 : 10; }
            return nice * Math.pow(10, exp);
        }
        function niceTicks(lo, hi, maxTicks) {
            if (hi === lo) return [lo];
            const range = niceNum(hi - lo, false);
            const step = niceNum(range / (maxTicks - 1), true);
            const start = Math.ceil(lo / step) * step;
            const ticks = [];
            for (let v = start; v <= hi + step * 0.01; v += step) ticks.push(parseFloat(v.toPrecision(12)));
            return ticks;
        }
        function formatTickLabel(val) {
            if (val === 0) return '0';
            if (Math.abs(val) >= 1e6 || (Math.abs(val) < 0.01 && val !== 0)) return val.toExponential(1);
            if (Number.isInteger(val)) return val.toLocaleString();
            return val.toPrecision(3);
        }

        const histogram = this.histogram;
        let valueBinWidth, numValueBins, binnedCounts, histRange;
        function rebuildBins() {
            histXMin = useBitpixRange ? bitpixXMin : dataXMin;
            histXMax = useBitpixRange ? bitpixXMax : dataXMax;
            histRange = histXMax - histXMin;
            valueBinWidth = (isIntegerBitpix && useBitpixRange) ? Math.max(10, histRange / plotW) : (histRange / plotW);
            numValueBins = Math.ceil(histRange / valueBinWidth) || 1;
            binnedCounts = new Float64Array(numValueBins);
            for (let i = 0; i < histogram.nbins; i++) {
                const binCenter = histogram.min + (i + 0.5) * histogram.binWidth;
                let di = Math.floor((binCenter - histXMin) / valueBinWidth);
                if (di < 0) di = 0;
                if (di >= numValueBins) di = numValueBins - 1;
                binnedCounts[di] += histogram.counts[i];
            }
        }
        rebuildBins();

        let selStart = null, selEnd = null, isDragging = false;
        function snap(v) { return Math.round(v * dpr) / dpr + 0.5 / dpr; }
        function binToPlotX(binIdx) {
            return [((binIdx * valueBinWidth) / histRange) * plotW, (((binIdx + 1) * valueBinWidth) / histRange) * plotW];
        }
        function plotXToBin(px) {
            let idx = Math.floor(((histXMin + (px / plotW) * histRange) - histXMin) / valueBinWidth);
            if (idx < 0) idx = 0;
            if (idx >= numValueBins) idx = numValueBins - 1;
            return idx;
        }

        function drawHistogram() {
            const useLog = histLogCheckbox.checked;
            histCtx.clearRect(0, 0, histW, histH);
            let maxCount = 0;
            for (let i = 0; i < numValueBins; i++) {
                const v = useLog ? Math.log10(binnedCounts[i] + 1) : binnedCounts[i];
                if (v > maxCount) maxCount = v;
            }
            if (maxCount === 0) maxCount = 1;

            if (selStart !== null && selEnd !== null) {
                const x0 = Math.min(selStart, selEnd), x1 = Math.max(selStart, selEnd);
                histCtx.fillStyle = 'rgba(66,133,244,0.15)';
                histCtx.fillRect(padL + x0, padT, x1 - x0, plotH);
            }

            histCtx.fillStyle = '#7a9abd';
            for (let i = 0; i < numValueBins; i++) {
                const raw = binnedCounts[i];
                if (raw === 0) continue;
                const v = useLog ? Math.log10(raw + 1) : raw;
                const barH = (v / maxCount) * plotH;
                const [bx0, bx1] = binToPlotX(i);
                histCtx.fillRect(padL + bx0, padT + plotH - barH, Math.max(1, bx1 - bx0), barH);
            }

            if (selStart !== null && selEnd !== null) {
                const x0 = Math.min(selStart, selEnd), x1 = Math.max(selStart, selEnd);
                const bi0 = plotXToBin(x0), bi1 = plotXToBin(x1);
                histCtx.fillStyle = 'rgba(66,133,244,0.6)';
                for (let i = bi0; i <= bi1 && i < numValueBins; i++) {
                    const raw = binnedCounts[i];
                    if (raw === 0) continue;
                    const v = useLog ? Math.log10(raw + 1) : raw;
                    const barH = (v / maxCount) * plotH;
                    const [bx0, bx1] = binToPlotX(i);
                    const drawX = padL + Math.max(bx0, x0), drawX1 = padL + Math.min(bx1, x1);
                    histCtx.fillRect(drawX, padT + plotH - barH, Math.max(1, drawX1 - drawX), barH);
                }
            }

            histCtx.strokeStyle = '#555'; histCtx.lineWidth = 1;
            histCtx.beginPath();
            histCtx.moveTo(snap(padL), snap(padT)); histCtx.lineTo(snap(padL), snap(padT + plotH));
            histCtx.lineTo(snap(padL + plotW), snap(padT + plotH)); histCtx.stroke();

            histCtx.fillStyle = '#aaa'; histCtx.font = '10px sans-serif';
            histCtx.textAlign = 'center'; histCtx.textBaseline = 'top';
            const xTicks = niceTicks(histXMin, histXMax, 6);
            for (const val of xTicks) {
                const frac = (val - histXMin) / (histXMax - histXMin);
                if (frac < 0 || frac > 1) continue;
                const x = snap(padL + frac * plotW);
                histCtx.beginPath(); histCtx.moveTo(x, snap(padT + plotH)); histCtx.lineTo(x, snap(padT + plotH + 4)); histCtx.stroke();
                histCtx.fillText(formatTickLabel(val), padL + frac * plotW, padT + plotH + 5);
            }

            histCtx.textAlign = 'right'; histCtx.textBaseline = 'middle';
            if (useLog) {
                const maxExp = Math.ceil(maxCount);
                const step = maxExp <= 4 ? 1 : Math.ceil(maxExp / 4);
                for (let e = 0; e <= maxExp; e += step) {
                    const frac = maxCount > 0 ? e / maxCount : 0;
                    if (frac > 1) break;
                    const y = snap(padT + plotH - frac * plotH);
                    histCtx.beginPath(); histCtx.moveTo(snap(padL - 4), y); histCtx.lineTo(snap(padL), y); histCtx.stroke();
                    const actual = Math.pow(10, e);
                    histCtx.fillText(actual >= 1e6 ? actual.toExponential(0) : actual.toLocaleString(), padL - 6, padT + plotH - frac * plotH);
                }
            } else {
                const yTicks = niceTicks(0, maxCount, 4);
                for (const val of yTicks) {
                    const frac = val / maxCount;
                    if (frac > 1) break;
                    const y = snap(padT + plotH - frac * plotH);
                    histCtx.beginPath(); histCtx.moveTo(snap(padL - 4), y); histCtx.lineTo(snap(padL), y); histCtx.stroke();
                    histCtx.fillText(formatTickLabel(val), padL - 6, padT + plotH - frac * plotH);
                }
            }
        }

        drawHistogram();
        histLogCheckbox.addEventListener('change', () => drawHistogram());
        if (histBitpixCheckbox) {
            histBitpixCheckbox.addEventListener('change', () => {
                useBitpixRange = histBitpixCheckbox.checked;
                selStart = null; selEnd = null;
                rebuildBins(); drawHistogram();
            });
        }

        function histXToPixelVal(px) { return histXMin + (px / plotW) * histRange; }
        const fmtVal = (v) => {
            if (Math.abs(v) >= 1e5 || (Math.abs(v) < 0.01 && v !== 0)) return v.toExponential(2);
            return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(1);
        };

        function computeSelectionPercent() {
            if (selStart === null || selEnd === null) return;
            const x0 = Math.max(0, Math.min(selStart, selEnd));
            const x1 = Math.min(plotW, Math.max(selStart, selEnd));
            const bi0 = plotXToBin(x0), bi1 = plotXToBin(x1);
            let selectedCount = 0;
            for (let i = bi0; i <= bi1 && i < numValueBins; i++) selectedCount += binnedCounts[i];
            const pct = histogram.total > 0 ? ((selectedCount / histogram.total) * 100).toFixed(3) : '0.000';
            const v0 = histXMin + bi0 * valueBinWidth, v1 = histXMin + (bi1 + 1) * valueBinWidth;
            histInfo.textContent = `${fmtVal(v0)} \u2013 ${fmtVal(v1)}: ${pct}% of pixels`;
        }

        histCanvas.addEventListener('mousedown', (e) => {
            const r = histCanvas.getBoundingClientRect();
            selStart = Math.max(0, Math.min(plotW, e.clientX - r.left - padL));
            selEnd = selStart; isDragging = true; e.preventDefault();
            e.stopPropagation();
        });

        function showBinTooltip(e) {
            const r = histCanvas.getBoundingClientRect();
            const x = e.clientX - r.left - padL;
            if (x < 0 || x >= plotW) { histInfo.textContent = ''; return; }
            const bi = plotXToBin(x);
            if (bi < 0 || bi >= numValueBins) return;
            const count = binnedCounts[bi];
            const valLo = histXMin + bi * valueBinWidth, valHi = valLo + valueBinWidth;
            histInfo.textContent = `${fmtVal(valLo)} \u2013 ${fmtVal(valHi)}: ${Math.round(count).toLocaleString()} pixels`;
        }

        histCanvas.addEventListener('mousemove', (e) => {
            if (isDragging) {
                const r = histCanvas.getBoundingClientRect();
                selEnd = Math.max(0, Math.min(plotW, e.clientX - r.left - padL));
                drawHistogram(); computeSelectionPercent();
            } else { showBinTooltip(e); }
        });
        histCanvas.addEventListener('mouseup', () => { isDragging = false; });
        histCanvas.addEventListener('mouseleave', () => {
            isDragging = false;
            if (selStart !== null && selEnd !== null) computeSelectionPercent();
            else histInfo.textContent = '';
        });

        document.body.appendChild(panel);
    }

    // ── WCS Grid ──────────────────────────────────────────────────────────────

    _drawWCSGrid() {
        const ctx = this.gridCtx;
        const canvas = ctx.canvas;
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        const k = this.currentTransform.k;
        const xWidth = this.imageWidth / k;
        const yHeight = this.imageHeight / k;
        const left = (-this.currentTransform.x * this.scaleX) / k;
        const top = (-this.currentTransform.y * this.scaleY) / k;

        const margin = 0.1;
        const boundsLeft = left - xWidth * margin;
        const boundsTop = top - yHeight * margin;
        const boundsWidth = xWidth * (1 + 2 * margin);
        const boundsHeight = yHeight * (1 + 2 * margin);
        const imageBoundsTop = this.imageHeight - 1 - (boundsTop + boundsHeight);
        let [ramin, ramax, decmin, decmax] = this.wcs.sipGetRadecBounds(
            50 / k,
            boundsLeft, imageBoundsTop,
            boundsWidth, boundsHeight
        );

        const pixScale = this.wcs.tanPixelScale();
        const lineStep = 50;
        const arcsecStep = pixScale * (lineStep / k);
        const step = arcsecStep / 3600;

        const rastep = calculateGridTicks((ramax - ramin) / (1 + 2 * margin));
        const decstep = calculateGridTicks((decmax - decmin) / (1 + 2 * margin));

        ctx.strokeStyle = 'white'; ctx.lineWidth = 1;
        ctx.beginPath();

        for (let ra = rastep * Math.floor(ramin / rastep); ra <= rastep * Math.ceil(ramax / rastep); ra += rastep) {
            if (ra >= rastep * Math.floor(ramin / rastep) + 360) continue;
            let lastOk = false, lastX = null, lastY = null;
            const hits = [];
            const n = Math.ceil((decmax - decmin) / step) + 2;
            for (let i = 0; i <= n; i++) {
                const rawDec = decmin + (i - 1) * step;
                const dec = Math.max(-90, Math.min(90, rawDec));
                const xy = this.wcs.sipRadec2Pixelxy(ra, dec);
                if (!xy) { lastOk = false; continue; }
                // FITS pixel coordinates: pixel x covers [x + 0.5, x + 1.5)
                const x = ((xy[0] - 0.5 - left) / (this.imageWidth / k)) * canvas.width;
                const y = ((this.imageHeight + 0.5 - xy[1] - top) / (this.imageHeight / k)) * canvas.height;
                if (lastX !== null && lastY !== null) hits.push(...intersect(x, y, lastX, lastY, canvas.width, canvas.height));
                if (lastOk) ctx.lineTo(x, y); else ctx.moveTo(x, y);
                lastOk = true; lastX = x; lastY = y;
                if (rawDec > 90) break;
            }
            const hit = selectHitByPriority(hits, ['bottom', 'right', 'top', 'left'], canvas.width, canvas.height);
            if (hit) this._drawLabel(ctx, hit, formatRa(ra, rastep));
        }

        for (let dec = decstep * Math.floor(decmin / decstep); dec <= decstep * Math.ceil(decmax / decstep); dec += decstep) {
            if (Math.abs(dec) >= 90) continue;
            const cosDec = Math.cos((dec * Math.PI) / 180);
            const rastepLocal = step / Math.max(0.1, cosDec);
            let lastOk = false, lastX = null, lastY = null;
            const hits = [];
            const n = Math.ceil((ramax - ramin) / rastepLocal) + 2;
            for (let i = 0; i <= n; i++) {
                const rawRa = ramin + (i - 1) * rastepLocal;
                const ra = Math.max(ramin, Math.min(ramin + 360, rawRa));
                const xy = this.wcs.sipRadec2Pixelxy(ra, dec);
                if (!xy) { lastOk = false; continue; }
                const x = ((xy[0] - 0.5 - left) / (this.imageWidth / k)) * canvas.width;
                const y = ((this.imageHeight + 0.5 - xy[1] - top) / (this.imageHeight / k)) * canvas.height;
                if (lastX !== null && lastY !== null) hits.push(...intersect(x, y, lastX, lastY, canvas.width, canvas.height));
                if (lastOk) ctx.lineTo(x, y); else ctx.moveTo(x, y);
                lastOk = true; lastX = x; lastY = y;
                if (rawRa - ramin > 360) break;
            }
            const hit = selectHitByPriority(hits, ['left', 'top', 'right', 'bottom'], canvas.width, canvas.height);
            if (hit) this._drawLabel(ctx, hit, formatDec(dec, decstep));
        }

        ctx.stroke();
    }

    _drawLabel(ctx, hit, text) {
        ctx.save();
        ctx.fillStyle = 'white'; ctx.font = '12px sans-serif'; ctx.textBaseline = 'bottom';
        ctx.translate(hit.x, hit.y); ctx.rotate(hit.angle);
        const pad = 18;
        switch (hit.edge) {
            case 'top':
                ctx.textAlign = Math.sin(hit.angle) <= 0 ? 'right' : 'left';
                ctx.fillText(text, Math.sin(hit.angle) <= 0 ? -pad : pad, -2);
                break;
            case 'bottom':
                ctx.textAlign = Math.sin(hit.angle) <= 0 ? 'left' : 'right';
                ctx.fillText(text, Math.sin(hit.angle) <= 0 ? pad : -pad, -2);
                break;
            case 'left':
                ctx.textAlign = 'left'; ctx.fillText(text, pad, -2);
                break;
            case 'right':
                ctx.textAlign = 'right'; ctx.fillText(text, -pad, -2);
                break;
        }
        ctx.restore();
    }

    // ── Line Profiles ─────────────────────────────────────────────────────────

    _drawLineProfile(profileCtx, profileData, isHorizontal, offset) {
        profileCtx.clearRect(0, 0, profileCtx.canvas.width, profileCtx.canvas.height);
        const maxVal = Math.max(...profileData.filter(v => !isNaN(v)));

        profileCtx.strokeStyle = this.gridColor;
        profileCtx.beginPath();
        if (isHorizontal) {
            for (let i = 0; i <= 5; i++) {
                const y = profileCtx.canvas.height * (1 - i / 5);
                profileCtx.moveTo(0, y); profileCtx.lineTo(profileCtx.canvas.width, y);
            }
        } else {
            for (let i = 0; i <= 5; i++) {
                const x = profileCtx.canvas.width * (1 - i / 5);
                profileCtx.moveTo(x, 0); profileCtx.lineTo(x, profileCtx.canvas.height);
            }
        }
        profileCtx.stroke();

        profileCtx.strokeStyle = this.profileColor;
        profileCtx.beginPath();
        if (isHorizontal) {
            let needsMove = true;
            profileData.forEach((val, index) => {
                const x = ((index - offset) / (this.imageWidth / this.currentTransform.k)) * profileCtx.canvas.width;
                if (isNaN(val)) { needsMove = true; return; }
                const y = profileCtx.canvas.height * (1 - val / maxVal);
                if (needsMove) { profileCtx.moveTo(x, y); needsMove = false; }
                else {
                    profileCtx.lineTo(x, profileCtx.canvas.height * (1 - profileData[index - 1] / maxVal));
                    profileCtx.lineTo(x, y);
                }
                if (index === profileData.length - 1) {
                    profileCtx.lineTo(((index + 1 - offset) / (this.imageWidth / this.currentTransform.k)) * profileCtx.canvas.width, y);
                }
            });
        } else {
            let needsMove = true;
            profileData.forEach((val, index) => {
                const y = ((index - offset) / (this.imageHeight / this.currentTransform.k)) * profileCtx.canvas.height;
                if (isNaN(val)) { needsMove = true; return; }
                const x = profileCtx.canvas.width * (1 - val / maxVal);
                if (needsMove) { profileCtx.moveTo(x, y); needsMove = false; }
                else {
                    profileCtx.lineTo(profileCtx.canvas.width * (1 - profileData[index - 1] / maxVal), y);
                    profileCtx.lineTo(x, y);
                }
                if (index === profileData.length - 1) {
                    profileCtx.lineTo(x, ((index + 1 - offset) / (this.imageHeight / this.currentTransform.k)) * profileCtx.canvas.height);
                }
            });
        }
        profileCtx.stroke();
    }

    // ── Radial Profile Panel ──────────────────────────────────────────────────

    _showRadialProfilePanel(fwhmResult) {
        document.getElementById('radialProfilePanel')?._close?.();

        const { background, backgroundSigma, peak, fwhm, fwhmMajor, fwhmMinor, posAngle, amplitude } = fwhmResult;
        const peakNet = peak - background;
        // Height of the fitted Gaussian; the peak pixel when the fit failed
        const modelAmplitude = amplitude > 0 ? amplitude : peakNet;

        const apRadii = [1, 2, 3, 4, 5].map(n => fwhm * n);

        const apertureSum = (cx, cy, r) => {
            if (r <= 0 || !this.imageData) return { rawSum: 0, npix: 0, net: 0 };
            const x0 = Math.max(0, Math.floor(cx - r)), x1 = Math.min(this.imageWidth - 1, Math.ceil(cx + r));
            const y0 = Math.max(0, Math.floor(cy - r)), y1 = Math.min(this.imageHeight - 1, Math.ceil(cy + r));
            let sum = 0, n = 0;
            const r2 = r * r;
            for (let y = y0; y <= y1; y++) {
                for (let x = x0; x <= x1; x++) {
                    const dx = x - cx, dy = y - cy;
                    if (dx * dx + dy * dy <= r2) { sum += this.imageData[y * this.imageWidth + x]; n++; }
                }
            }
            return { rawSum: sum, npix: n, net: sum - background * n };
        };
        const cx = fwhmResult.center.x, cy = fwhmResult.center.y;
        const apResults = apRadii.map(r => apertureSum(cx, cy, r));

        const plotRadius = Math.min(Math.max(fwhm * 6, 20), Math.min(this.imageWidth, this.imageHeight) / 2);
        const scatterPts = [];
        {
            const sx0 = Math.max(0, Math.floor(cx - plotRadius));
            const sx1 = Math.min(this.imageWidth - 1, Math.ceil(cx + plotRadius));
            const sy0 = Math.max(0, Math.floor(cy - plotRadius));
            const sy1 = Math.min(this.imageHeight - 1, Math.ceil(cy + plotRadius));
            const pr2 = plotRadius * plotRadius;
            for (let y = sy0; y <= sy1; y++) {
                for (let x = sx0; x <= sx1; x++) {
                    const dx = x - cx, dy = y - cy;
                    const r2 = dx * dx + dy * dy;
                    if (r2 <= pr2) scatterPts.push([Math.sqrt(r2), this.imageData[y * this.imageWidth + x]]);
                }
            }
        }

        // Centre and positions use the estimator's convention: pixel (x, y) has its centre at (x, y)
        const { panel, body: pbody, titleSpan } = this._createPanel('radialProfilePanel', 'Radial Profile', { width: 460, side: 'right' });
        titleSpan.innerHTML = `Radial Profile <span style="color:var(--sfv-panel-muted, #888);font-weight:normal">(${Math.round(cx)}, ${Math.round(cy)})</span>`;
        pbody.style.padding = '8px';

        const infoLine = document.createElement('div');
        infoLine.style.cssText = 'font-size:11px;color:#888;margin-bottom:3px;text-align:center';
        const fwhmInfo = fwhm > 0
            ? `FWHM: ${formatNumber(fwhm, 2)} px${this.plateScale ? '  /  ' + formatNumber(fwhm * this.plateScale, 2) + '\u2033' : ''}`
            : 'FWHM: \u2014';
        infoLine.textContent = fwhmInfo;
        pbody.appendChild(infoLine);

        const chartW = 440, chartH = 280;
        const padL = 56, padR = 8, padT = 20, padB = 30;
        const plotW = chartW - padL - padR, plotH = chartH - padT - padB;
        const dpr = window.devicePixelRatio || 1;
        const chartEl = document.createElement('canvas');
        chartEl.width = chartW * dpr; chartEl.height = chartH * dpr;
        chartEl.style.cssText = `width:100%;max-width:${chartW}px;height:auto;display:block`;
        pbody.appendChild(chartEl);
        const cc = chartEl.getContext('2d');
        cc.scale(dpr, dpr);

        function pfmt(v) {
            if (v === undefined || v === null || isNaN(v)) return '\u2014';
            if (Math.abs(v) >= 1e6) return v.toExponential(2);
            if (Math.abs(v) >= 1e3) return Math.round(v).toLocaleString();
            if (Math.abs(v) >= 1) return formatNumber(v, 1);
            return v.toPrecision(3);
        }

        let _tooltip = null;
        let yLo = 0, yHi = 1, yRange = 1, _step = 1;
        {
            function _niceNum(range, round) {
                if (range <= 0) return 1;
                const exp = Math.floor(Math.log10(range));
                const frac = range / Math.pow(10, exp);
                let nice;
                if (round) { nice = frac < 1.5 ? 1 : frac < 3 ? 2 : frac < 7 ? 5 : 10; }
                else { nice = frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 5 ? 5 : 10; }
                return nice * Math.pow(10, exp);
            }
            let dataMin = Infinity, dataMax = -Infinity;
            for (const [, v] of scatterPts) { if (v < dataMin) dataMin = v; if (v > dataMax) dataMax = v; }
            _step = _niceNum(_niceNum(dataMax - dataMin || 1, false) / 4, true);
            yLo = Math.floor(Math.min(dataMin, background - 3 * backgroundSigma) / _step) * _step;
            yHi = (Math.ceil(dataMax / _step) + 1) * _step;
            yRange = yHi - yLo;
        }

        const drawScatterChart = () => {
            cc.clearRect(0, 0, chartW, chartH);
            if (scatterPts.length === 0) {
                cc.fillStyle = '#666'; cc.font = '12px monospace'; cc.textAlign = 'center';
                cc.fillText('No data', chartW / 2, chartH / 2); return;
            }
            const xs = r => padL + (r / plotRadius) * plotW;
            const ys = v => padT + plotH - ((v - yLo) / yRange) * plotH;
            const ysClamp = v => Math.max(padT, Math.min(padT + plotH, ys(v)));

            cc.fillStyle = 'rgba(80,140,220,0.55)';
            for (const [r, v] of scatterPts) {
                if (r > plotRadius) continue;
                cc.fillRect(xs(r) - 2, ysClamp(v) - 2, 4, 4);
            }

            if (fwhm > 0 && modelAmplitude > 0) {
                cc.beginPath();
                for (let i = 0; i <= 300; i++) {
                    const r = (i / 300) * plotRadius;
                    const v = background + modelAmplitude * Math.exp(-4 * Math.LN2 * r * r / (fwhm * fwhm));
                    i === 0 ? cc.moveTo(xs(r), ysClamp(v)) : cc.lineTo(xs(r), ysClamp(v));
                }
                cc.strokeStyle = '#e040fb'; cc.lineWidth = 2.5; cc.setLineDash([]); cc.stroke();
            }

            function labelWithBg(text, x, y, baseline, color) {
                cc.textBaseline = baseline;
                const m = cc.measureText(text);
                const th = 10, pad = 2;
                const bx = x - m.width / 2 - pad;
                const by = baseline === 'top' ? y : y - th;
                cc.fillStyle = 'rgba(20,20,20,0.75)';
                cc.fillRect(bx, by, m.width + pad * 2, th + pad);
                cc.fillStyle = color;
                cc.fillText(text, x, y);
            }

            apRadii.forEach((r, i) => {
                if (r <= 0 || r > plotRadius) return;
                const x = xs(r);
                cc.beginPath(); cc.moveTo(x, padT); cc.lineTo(x, padT + plotH);
                cc.strokeStyle = '#ef5350'; cc.lineWidth = 1.5; cc.setLineDash([]); cc.stroke();
                cc.font = '10px monospace'; cc.textAlign = 'center';
                labelWithBg(`${i + 1}\u00d7`, x, padT + 1, 'top', '#ef5350');
            });

            const yBg = ys(background);
            if (yBg >= padT && yBg <= padT + plotH) {
                cc.beginPath(); cc.moveTo(padL, yBg); cc.lineTo(padL + plotW, yBg);
                cc.strokeStyle = 'rgba(255,152,0,0.5)'; cc.lineWidth = 1; cc.setLineDash([3, 3]); cc.stroke(); cc.setLineDash([]);
            }

            cc.strokeStyle = '#555'; cc.lineWidth = 1; cc.setLineDash([]);
            cc.beginPath(); cc.moveTo(padL, padT); cc.lineTo(padL, padT + plotH); cc.lineTo(padL + plotW, padT + plotH); cc.stroke();

            cc.fillStyle = '#777'; cc.font = '10px monospace'; cc.textAlign = 'center'; cc.textBaseline = 'top';
            const nXTicks = Math.min(8, Math.floor(plotRadius));
            const xStep = Math.ceil(plotRadius / nXTicks);
            for (let rv = 0; rv <= plotRadius; rv += xStep) {
                const x = xs(rv);
                if (x > padL + plotW + 1) break;
                cc.beginPath(); cc.moveTo(x, padT + plotH); cc.lineTo(x, padT + plotH + 3); cc.stroke();
                cc.fillText(Math.round(rv), x, padT + plotH + 4);
            }
            cc.fillStyle = '#666'; cc.fillText('Radius [pixels]', padL + plotW / 2, padT + plotH + 17);

            cc.textAlign = 'right'; cc.textBaseline = 'middle';
            for (let v = yLo; v <= yHi + _step * 0.01; v += _step) {
                v = parseFloat(v.toPrecision(12));
                const y = ys(v);
                if (y < padT - 2 || y > padT + plotH + 2) continue;
                cc.beginPath(); cc.moveTo(padL - 3, y); cc.lineTo(padL, y); cc.stroke();
                cc.fillStyle = '#777'; cc.fillText(pfmt(v), padL - 5, y);
            }

            cc.save();
            cc.translate(11, padT + plotH / 2); cc.rotate(-Math.PI / 2);
            cc.textAlign = 'center'; cc.font = '10px monospace'; cc.fillStyle = '#666';
            cc.fillText('ADU', 0, 0); cc.restore();

            if (_tooltip) {
                const { r: tr, rawV, fitV } = _tooltip;
                const tx = xs(tr);
                cc.beginPath(); cc.moveTo(tx, padT); cc.lineTo(tx, padT + plotH);
                cc.strokeStyle = 'rgba(255,255,255,0.25)'; cc.lineWidth = 1; cc.setLineDash([3, 3]); cc.stroke(); cc.setLineDash([]);
                const ty = ysClamp(rawV);
                cc.beginPath(); cc.moveTo(padL, ty); cc.lineTo(padL + plotW, ty);
                cc.strokeStyle = 'rgba(255,255,255,0.15)'; cc.lineWidth = 1; cc.setLineDash([3, 3]); cc.stroke(); cc.setLineDash([]);
                cc.beginPath(); cc.arc(tx, ty, 3.5, 0, Math.PI * 2);
                cc.fillStyle = 'rgba(100,160,255,0.9)'; cc.fill();
                if (fitV !== null) {
                    const fy = ysClamp(fitV);
                    cc.beginPath(); cc.arc(tx, fy, 3.5, 0, Math.PI * 2);
                    cc.fillStyle = 'rgba(224,64,251,0.9)'; cc.fill();
                }
                const lines = [`r = ${formatNumber(tr, 2)} px`, `raw \u2248 ${pfmt(rawV)}`, ...(fitV !== null ? [`fit = ${pfmt(fitV)}`] : [])];
                cc.font = '10px monospace';
                const lineH = 13, boxPad = 5;
                const boxW = Math.max(...lines.map(l => cc.measureText(l).width)) + boxPad * 2;
                const boxH = lines.length * lineH + boxPad * 2;
                let bx = tx + 8, by = ty - boxH / 2;
                if (bx + boxW > padL + plotW) bx = tx - boxW - 8;
                if (by < padT) by = padT;
                if (by + boxH > padT + plotH) by = padT + plotH - boxH;
                cc.fillStyle = 'rgba(20,20,20,0.85)';
                cc.beginPath(); cc.roundRect(bx, by, boxW, boxH, 3); cc.fill();
                cc.strokeStyle = 'rgba(255,255,255,0.15)'; cc.lineWidth = 0.5; cc.setLineDash([]); cc.stroke();
                const colors = ['#aaa', 'rgba(100,160,255,0.9)', 'rgba(224,64,251,0.9)'];
                lines.forEach((l, i) => {
                    cc.fillStyle = colors[i] || '#aaa'; cc.textAlign = 'left'; cc.textBaseline = 'top';
                    cc.fillText(l, bx + boxPad, by + boxPad + i * lineH);
                });
            }
        };

        drawScatterChart();

        chartEl.addEventListener('mousemove', e => {
            const rect = chartEl.getBoundingClientRect();
            const mx = (e.clientX - rect.left) * (chartW / rect.width);
            const my = (e.clientY - rect.top) * (chartH / rect.height);
            if (mx < padL || mx > padL + plotW || my < padT || my > padT + plotH) {
                if (_tooltip) { _tooltip = null; drawScatterChart(); } return;
            }
            let bestDist = Infinity, bestV = null, bestR = (mx - padL) / plotW * plotRadius;
            for (const [r, v] of scatterPts) {
                const screenX = (r / plotRadius) * plotW;
                const screenY = plotH - ((v - yLo) / yRange) * plotH;
                const d = (mx - padL - screenX) ** 2 + (my - padT - screenY) ** 2;
                if (d < bestDist) { bestDist = d; bestV = v; bestR = r; }
            }
            const fitV = (fwhm > 0 && modelAmplitude > 0)
                ? background + modelAmplitude * Math.exp(-4 * Math.LN2 * bestR * bestR / (fwhm * fwhm))
                : null;
            _tooltip = { r: bestR, rawV: bestV, fitV };
            drawScatterChart();
        });
        chartEl.addEventListener('mouseleave', () => {
            if (_tooltip) { _tooltip = null; drawScatterChart(); }
        });

        const statsDiv = document.createElement('div');
        Object.assign(statsDiv.style, { marginTop: '6px', borderTop: '1px solid #333', paddingTop: '6px', fontSize: '12px', lineHeight: '1.7' });

        function pRow(label, val) {
            const d = document.createElement('div');
            d.style.cssText = 'display:flex;justify-content:space-between;padding:0 2px';
            d.innerHTML = `<span style="color:#888">${label}</span><span style="color:#d4d4d4">${val}</span>`;
            return d;
        }
        function pSection(text) {
            const d = document.createElement('div');
            d.style.cssText = 'color:#aaa;font-size:11px;font-weight:bold;margin:4px 0 2px;padding:0 2px;text-transform:uppercase;letter-spacing:.05em';
            d.textContent = text;
            return d;
        }

        const fwhmPxStr = fwhm > 0 ? `${formatNumber(fwhm, 2)} px` : '\u2014';
        const fwhmArcsecStr = (fwhm > 0 && this.plateScale) ? `\u2002/\u2002${formatNumber(fwhm * this.plateScale, 2)}"` : '';
        const fwhmMajStr = fwhmMajor > 0 ? `${formatNumber(fwhmMajor, 2)} px${this.plateScale ? '  /  ' + formatNumber(fwhmMajor * this.plateScale, 2) + '\u2033' : ''}` : '\u2014';
        const fwhmMinStr = fwhmMinor > 0 ? `${formatNumber(fwhmMinor, 2)} px${this.plateScale ? '  /  ' + formatNumber(fwhmMinor * this.plateScale, 2) + '\u2033' : ''}` : '\u2014';
        const elongStr = (fwhmMajor > 0 && fwhmMinor > 0) ? formatNumber(fwhmMajor / fwhmMinor, 2) : '\u2014';
        const paStr = fwhmMajor > 0 ? `${formatNumber(posAngle, 1)}\u00b0` : '\u2014';

        statsDiv.appendChild(pSection('PSF'));
        statsDiv.appendChild(pRow('FWHM (radial)', fwhmPxStr + fwhmArcsecStr));
        statsDiv.appendChild(pRow('FWHM max', fwhmMajStr));
        statsDiv.appendChild(pRow('FWHM min', fwhmMinStr));
        statsDiv.appendChild(pRow('Elongation a/b', elongStr));
        statsDiv.appendChild(pRow('Position angle', paStr));
        statsDiv.appendChild(pRow('Peak', `${pfmt(peak)} ADU`));
        statsDiv.appendChild(pRow('Peak \u2212 bg', `${pfmt(peakNet)} ADU`));
        statsDiv.appendChild(pRow('Background', `${pfmt(background)} \u00b1 ${pfmt(backgroundSigma)} ADU`));

        statsDiv.appendChild(pSection('Aperture photometry'));
        const apTableHdr = document.createElement('div');
        apTableHdr.style.cssText = 'display:grid;grid-template-columns:28px 44px 68px 1fr 52px;gap:2px;padding:0 2px;color:#666;font-size:11px';
        apTableHdr.innerHTML = '<span>ap</span><span>FWHM</span><span>radius</span><span>bg-sub counts</span><span>area</span>';
        statsDiv.appendChild(apTableHdr);
        const subNames = ['\u2081', '\u2082', '\u2083', '\u2084', '\u2085'];
        apRadii.forEach((r, i) => {
            const apResult = apResults[i];
            const row = document.createElement('div');
            row.style.cssText = 'display:grid;grid-template-columns:28px 44px 68px 1fr 52px;gap:2px;padding:0 2px';
            row.innerHTML = `<span style="color:#888">r${subNames[i]}</span><span style="color:#999">${i + 1}\u00d7</span><span style="color:#777">${formatNumber(r, 1)} px</span><span style="color:#d4d4d4">${pfmt(apResult.net)}</span><span style="color:#555">${apResult.npix} px</span>`;
            statsDiv.appendChild(row);
        });

        pbody.appendChild(statsDiv);
        document.body.appendChild(panel);
    }

    // ── Header Table ──────────────────────────────────────────────────────────

    _displayHeaderTable(data) {
        this.headerTable.innerHTML = '';
        const headerRow = document.createElement('tr');
        ['Key', 'Value', 'Comment'].forEach(text => {
            const th = document.createElement('th');
            th.textContent = text;
            headerRow.appendChild(th);
        });
        this.headerTable.appendChild(headerRow);

        for (const [key, value] of Object.entries(data)) {
            const row = document.createElement('tr');
            const keyCell = document.createElement('td');
            const valueCell = document.createElement('td');
            const commentCell = document.createElement('td');
            keyCell.textContent = key;
            const { value: v, comment } = splitHeaderValue(value);
            valueCell.textContent = v;
            commentCell.textContent = comment;
            row.appendChild(keyCell); row.appendChild(valueCell); row.appendChild(commentCell);
            this.headerTable.appendChild(row);
        }
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    _updateGridToggleVisibility(hasWcs) {
        if (!this.gridShow) return;
        this.showGrid = false;
        this.gridCtx.clearRect(0, 0, this.gridCtx.canvas.width, this.gridCtx.canvas.height);
        this.gridShow.style.display = hasWcs ? 'block' : 'none';
        this.gridShow.textContent = 'show grid';
    }

    _updateColor() {
        if (this.options.profileColor) {
            this.profileColor = this.options.profileColor;
        } else {
            const border = getComputedStyle(document.documentElement).getPropertyValue('--vscode-editor-compositionBorder').trim();
            if (border) this.profileColor = border;
        }
        if (this.options.gridColor) {
            this.gridColor = this.options.gridColor;
        } else {
            const fg = getComputedStyle(document.documentElement).getPropertyValue('--vscode-editor-foreground').trim();
            if (fg) this.gridColor = fg;
        }
    }

    _buildSimbadUrl(raDeg, decDeg) {
        const raHours = raDeg / 15;
        const rah = Math.floor(raHours);
        const ram = Math.floor((raHours - rah) * 60);
        const ras = ((raHours - rah) * 60 - ram) * 60;
        const sign = decDeg < 0 ? '-' : '+';
        const absDec = Math.abs(decDeg);
        const decd = Math.floor(absDec);
        const decm = Math.floor((absDec - decd) * 60);
        const decs = ((absDec - decd) * 60 - decm) * 60;
        const coord = `${String(rah).padStart(2, '0')}:${String(ram).padStart(2, '0')}:${ras.toFixed(3).padStart(6, '0')}${sign}${String(decd).padStart(2, '0')}:${String(decm).padStart(2, '0')}:${decs.toFixed(2).padStart(5, '0')}`;
        const params = new URLSearchParams({ Coord: coord, Radius: '30', 'Radius.unit': 'arcsec' });
        return `https://simbad.cds.unistra.fr/simbad/sim-coo?${params.toString()}`;
    }
}

// Auto-initialize only when the VSCode webview root element is present.
// In standalone usage (demo/other projects) skip this — callers instantiate FITSViewer themselves.
const _vscodeRoot = document.getElementById('fitsViewerRoot');
if (_vscodeRoot) {
    const _viewer = new FITSViewer(_vscodeRoot);
    window.__vscodeOnViewerReady?.(_viewer);
}
