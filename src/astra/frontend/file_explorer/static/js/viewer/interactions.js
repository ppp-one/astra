import { ViewerMode } from './state.js';
import { isPngFile } from './fileTypes.js';

export function setupInteractions({
    state,
    renderer,
    domRefs,
    onRequestFullLoad,
    onRequestPreview,
}) {
    const {
        headerTable,
        searchInput,
        resetButton,
        viewerToolbar,
        hduSelect,
        stretchMin,
        stretchMax,
        stretchGamma,
        stretchMinValue,
        stretchMaxValue,
        stretchGammaValue,
        loadFullFitsButton,
        logToggleButton,
        stretchPanelToggle,
    } = domRefs;


    if (searchInput) {
        searchInput.addEventListener('input', () => {
            const query = searchInput.value.toLowerCase();
            const filtered = filterHeader(state.getHeader(), query);
            renderHeaderTable(headerTable, filtered);
        });
    }

    if (resetButton) {
        resetButton.addEventListener('click', () => {
            searchInput.value = '';
            renderHeaderTable(headerTable, state.getHeader());
        });
    }

    state.addEventListener('headerchange', (event) => {
        renderHeaderTable(headerTable, event.detail.header);
    });

    state.addEventListener('modechange', (event) => {
        const { mode } = event.detail;
        if (!loadFullFitsButton) return;
        // Use icons for better affordance (Bootstrap Icons must be available)
        if (mode === ViewerMode.PREVIEW_READY) {
            loadFullFitsButton.disabled = false;
            // icon-only button; title explains action
            loadFullFitsButton.innerHTML = '<i class="bi bi-badge-hd" aria-hidden="true"></i>';
            loadFullFitsButton.title = 'Load full FITS file';
        } else if (mode === ViewerMode.FULL_LOADING) {
            loadFullFitsButton.disabled = true;
            loadFullFitsButton.innerHTML = '<i class="bi bi-hourglass-split" aria-hidden="true"></i>';
            loadFullFitsButton.title = 'Loading full FITS...';
        } else if (mode === ViewerMode.FULL_READY) {
            loadFullFitsButton.disabled = false;
            loadFullFitsButton.innerHTML = '<i class="bi bi-arrow-clockwise" aria-hidden="true"></i>';
            loadFullFitsButton.title = 'Reload full FITS file';
        } else if (mode === ViewerMode.ERROR) {
            const currentPath = state.getFile()?.filePath || '';
            const isPng = isPngFile(currentPath);
            loadFullFitsButton.disabled = isPng;
            loadFullFitsButton.innerHTML = '<i class="bi bi-exclamation-triangle" aria-hidden="true"></i>';
            loadFullFitsButton.title = isPng
                ? 'Full FITS load is only available for FITS files'
                : 'Retry loading full FITS';
        } else if (mode === ViewerMode.PNG_LOADING || mode === ViewerMode.PNG_READY || mode === ViewerMode.IDLE) {
            loadFullFitsButton.disabled = true;
            loadFullFitsButton.innerHTML = '<i class="bi bi-badge-hd" aria-hidden="true"></i>';
            loadFullFitsButton.title = 'Full FITS load is only available for FITS files';
        }
    });

    if (loadFullFitsButton) {
        loadFullFitsButton.addEventListener('click', (e) => {
            if (e && e.stopPropagation) e.stopPropagation();
            const { filePath, hdu } = state.getFile();
            if (!filePath) return;
            loadFullFitsButton.disabled = true;
            loadFullFitsButton.innerHTML = '<i class="bi bi-hourglass-split" aria-hidden="true"></i>';
            loadFullFitsButton.title = 'Loading full FITS...';
            onRequestFullLoad({ filePath, hdu });
        });
    }

    if (hduSelect) {
        hduSelect.addEventListener('change', () => {
            const { filePath } = state.getFile();
            if (!filePath) return;
            const value = hduSelect.value;
            const nextHdu = value === '' ? null : Number(value);
            const current = state.getFile().hdu;
            if (current === nextHdu || (current == null && nextHdu == null)) {
                return;
            }
            onRequestPreview({ filePath, hdu: nextHdu });
        });
    }

    state.addEventListener('hdulistchange', (event) => {
        const items = event.detail.items || [];
        populateHduSelect(hduSelect, items, state.getFile().hdu);
    });

    state.addEventListener('filechange', () => {
        if (hduSelect) {
            hduSelect.disabled = true;
        }
        // update HDU label to reflect current file/hdu
        try {
            const labelEl = document.getElementById('hduLabel');
            if (labelEl) {
                const f = state.getFile() || {};
                const hdu = f.hdu;
                // Do not display the word "Auto"; show minimal label when no HDU selected
                labelEl.textContent = hdu === null || hdu === undefined ? 'HDU' : `HDU: #${hdu}`;
            }
        } catch (err) {
            // non-fatal
        }
        try {
            const f = state.getFile() || {};
            updateFilePathBanner(f.filePath);
        } catch (err) {
            // optional UI; ignore failures
        }
    });

    state.addEventListener('statschange', ({ detail }) => {
        updateStretchSliders({
            stats: detail.stats,
            stretchMin,
            stretchMax,
            stretchGamma,
            stretchMinValue,
            stretchMaxValue,
            stretchGammaValue,
        });
    });

    const collapseQuery = typeof window.matchMedia === 'function'
        ? window.matchMedia('(max-width: 900px)')
        : null;

    const updateCollapseClasses = () => {
        if (!viewerToolbar) return;
        if (collapseQuery && collapseQuery.matches) {
            viewerToolbar.classList.add('is-collapsible');
            if (!viewerToolbar.classList.contains('is-collapsed')) {
                viewerToolbar.classList.add('is-collapsed');
            }
            const collapsed = viewerToolbar.classList.contains('is-collapsed');
            if (stretchPanelToggle) {
                stretchPanelToggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
                stretchPanelToggle.title = collapsed ? 'Show stretch controls' : 'Hide stretch controls';
            }
        } else {
            viewerToolbar.classList.remove('is-collapsible');
            viewerToolbar.classList.remove('is-collapsed');
            if (stretchPanelToggle) {
                stretchPanelToggle.setAttribute('aria-expanded', 'true');
                stretchPanelToggle.title = 'Hide stretch controls';
            }
        }
    };

    if (collapseQuery && (typeof collapseQuery.addEventListener === 'function')) {
        collapseQuery.addEventListener('change', updateCollapseClasses);
    } else if (collapseQuery && typeof collapseQuery.addListener === 'function') {
        collapseQuery.addListener(updateCollapseClasses);
    }
    updateCollapseClasses();

    if (stretchPanelToggle) {
        stretchPanelToggle.addEventListener('click', () => {
            if (!viewerToolbar || !viewerToolbar.classList.contains('is-collapsible')) return;
            const nowCollapsed = viewerToolbar.classList.toggle('is-collapsed');
            stretchPanelToggle.setAttribute('aria-expanded', nowCollapsed ? 'false' : 'true');
            stretchPanelToggle.title = nowCollapsed ? 'Show stretch controls' : 'Hide stretch controls';
        });
    }

    let logModeActive = false;
    const setLogToggleState = (enabled) => {
        logModeActive = !!enabled;
        if (logToggleButton) {
            logToggleButton.setAttribute('aria-pressed', enabled ? 'true' : 'false');
            logToggleButton.title = enabled ? 'Disable log stretch' : 'Enable log stretch';
        }
    };
    setLogToggleState(false);

    if (logToggleButton) {
        logToggleButton.addEventListener('click', () => {
            const next = !logModeActive;
            setLogToggleState(next);
            renderer.updateStretchSettings({ mode: next ? 'log' : 'linear' });
        });
    }

    if (stretchGamma && stretchGammaValue) {
        stretchGamma.addEventListener('input', () => {
            const gamma = Number(stretchGamma.value) / 100;
            stretchGammaValue.textContent = gamma.toFixed(2);
            renderer.updateStretchSettings({ gamma });
        });
    }

    if (stretchMin && stretchMinValue) {
        stretchMin.addEventListener('input', () => {
            const stats = state.getStats();
            if (!stats) return;
            const minValue = sliderToData(Number(stretchMin.value), stats);
            stretchMinValue.textContent = formatValue(minValue);
            renderer.updateStretchSettings({ min: minValue });
        });
    }

    if (stretchMax && stretchMaxValue) {
        stretchMax.addEventListener('input', () => {
            const stats = state.getStats();
            if (!stats) return;
            const maxValue = sliderToData(Number(stretchMax.value), stats);
            stretchMaxValue.textContent = formatValue(maxValue);
            renderer.updateStretchSettings({ max: maxValue });
        });
    }
}

function sliderToData(sliderValue, stats) {
    if (!stats) return 0;
    const { dataMin, dataMax } = stats;
    const t = Math.max(0, Math.min(1000, Number(sliderValue))) / 1000;
    // If the entire data range is positive, map slider exponentially
    if (dataMin > 0 && dataMax > dataMin) {
        const logMin = Math.log(dataMin);
        const logMax = Math.log(dataMax);
        const v = Math.exp(logMin + t * (logMax - logMin));
        return v;
    }
    // If the entire data range is negative, map using negative exponential (mirror)
    if (dataMax < 0 && dataMin < dataMax) {
        // map using absolute values then reapply sign
        const aMin = Math.abs(dataMax); // note: dataMax is less negative
        const aMax = Math.abs(dataMin);
        const logMin = Math.log(aMin);
        const logMax = Math.log(aMax);
        const v = Math.exp(logMin + t * (logMax - logMin));
        return -v;
    }
    // Otherwise (range crosses zero), fallback to linear mapping
    const range = dataMax - dataMin || 1;
    return dataMin + t * range;
}

function dataToSlider(value, stats) {
    if (!stats) return 0;
    const { dataMin, dataMax } = stats;
    // Handle log mapping inverses when possible
    if (dataMin > 0 && dataMax > dataMin) {
        const logMin = Math.log(dataMin);
        const logMax = Math.log(dataMax);
        const t = (Math.log(Math.max(value, Number.EPSILON)) - logMin) / (logMax - logMin || 1);
        return Math.min(1000, Math.max(0, Math.round(t * 1000)));
    }
    if (dataMax < 0 && dataMin < dataMax) {
        const aMin = Math.abs(dataMax);
        const aMax = Math.abs(dataMin);
        const logMin = Math.log(aMin);
        const logMax = Math.log(aMax);
        const t = (Math.log(Math.max(Math.abs(value), Number.EPSILON)) - logMin) / (logMax - logMin || 1);
        return Math.min(1000, Math.max(0, Math.round(t * 1000)));
    }
    const range = dataMax - dataMin || 1;
    return Math.min(1000, Math.max(0, Math.round(((value - dataMin) / range) * 1000)));
}

function updateStretchSliders({
    stats,
    stretchMin,
    stretchMax,
    stretchGamma,
    stretchMinValue,
    stretchMaxValue,
    stretchGammaValue,
}) {
    if (!stats) return;
    if (stretchMin) {
        stretchMin.value = dataToSlider(stats.defaultMin ?? stats.dataMin, stats);
        // ensure any UI components (dual slider) and listeners react to the programmatic change
        stretchMin.dispatchEvent(new Event('input'));
        if (stretchMinValue) {
            stretchMinValue.textContent = formatValue(stats.defaultMin ?? stats.dataMin);
        }
    }
    if (stretchMax) {
        stretchMax.value = dataToSlider(stats.defaultMax ?? stats.dataMax, stats);
        // trigger input event so dual slider and renderer process the new max
        stretchMax.dispatchEvent(new Event('input'));
        if (stretchMaxValue) {
            stretchMaxValue.textContent = formatValue(stats.defaultMax ?? stats.dataMax);
        }
    }

    // If a dual slider instance exists on the DOM, explicitly set its thumbs to match the new input values
    try {
        const dualEl = document.getElementById('dualRange');
        if (dualEl && dualEl._dual && typeof dualEl._dual.set === 'function') {
            const minV = stretchMin ? stretchMin.value : null;
            const maxV = stretchMax ? stretchMax.value : null;
            if (minV !== null && maxV !== null) {
                dualEl._dual.set(minV, maxV);
            }
        }
    } catch (err) {
        // non-fatal; dual slider optional
    }
    if (stretchGamma) {
        stretchGamma.value = 100;
    }
    if (stretchGammaValue) {
        stretchGammaValue.textContent = '1.00';
    }
}

function populateHduSelect(hduSelect, items = [], selected) {
    if (!hduSelect) return;
    hduSelect.innerHTML = '';
    // Do not add an "Auto" option. The small label above the select indicates the currently-loaded HDU.
    items.forEach((item) => {
        const option = document.createElement('option');
        option.value = String(item.index);
        option.textContent = formatHduLabel(item);
        if (selected === item.index) {
            option.selected = true;
        }
        hduSelect.appendChild(option);
    });
    hduSelect.disabled = false;
    try {
        const labelEl = document.getElementById('hduLabel');
        if (labelEl) {
            // minimal text when no HDU selected
            if (selected === null || selected === undefined) {
                labelEl.textContent = 'HDU';
            } else {
                labelEl.textContent = `HDU: #${selected}`;
            }
        }
    } catch (err) {
        // ignore
    }
}

function formatHduLabel(item) {
    const parts = [`#${item.index}`];
    if (item.name) parts.push(item.name);
    if (item.shape && item.shape.length) {
        parts.push(`(${item.shape.join('×')})`);
    }
    return parts.join(' ');
}

function filterHeader(headerData = {}, query = '') {
    if (!query) return headerData;
    const lowered = query.toLowerCase();
    return Object.fromEntries(
        Object.entries(headerData).filter(([key, value]) => {
            const { valueStr, commentStr } = normalizeHeaderValue(value);
            return (
                key.toLowerCase().includes(lowered) ||
                (valueStr && valueStr.includes(lowered)) ||
                (commentStr && commentStr.includes(lowered))
            );
        })
    );
}

function normalizeHeaderValue(value) {
    if (value && typeof value === 'object' && ('value' in value || 'comment' in value)) {
        return {
            valueStr: value.value ? String(value.value).toLowerCase() : '',
            commentStr: value.comment ? String(value.comment).toLowerCase() : '',
        };
    }
    const valueStr = value === null || value === undefined ? '' : String(value);
    const parts = valueStr.split('/').map((s) => (s ? s.trim().toLowerCase() : ''));
    return {
        valueStr: parts[0] || '',
        commentStr: parts.slice(1).join('/') || '',
    };
}

function renderHeaderTable(tableEl, data = {}) {
    if (!tableEl) return;

    // Prevent scroll jump: lock height and capture scroll position
    const scrollParent = tableEl.closest('[role="dialog"]') || document.documentElement;
    const savedScrollTop = scrollParent.scrollTop;
    const prevHeight = tableEl.offsetHeight;
    if (prevHeight > 0) {
        tableEl.style.minHeight = `${prevHeight}px`;
    }

    tableEl.innerHTML = '';
    const headerRow = document.createElement('tr');
    ['Key', 'Value', 'Comment'].forEach((label) => {
        const th = document.createElement('th');
        th.textContent = label;
        // add column class so CSS widths apply
        if (label === 'Key') th.classList.add('key-col');
        if (label === 'Value') th.classList.add('value-col');
        if (label === 'Comment') th.classList.add('comment-col');
        headerRow.appendChild(th);
    });
    // insert a colgroup to strongly enforce column widths (helps when table-layout is fixed)
    try {
        const colgroup = document.createElement('colgroup');
        const c1 = document.createElement('col'); c1.style.width = '12ch';
        const c2 = document.createElement('col'); c2.style.width = '40%';
        const c3 = document.createElement('col'); c3.style.width = '60%';
        colgroup.appendChild(c1); colgroup.appendChild(c2); colgroup.appendChild(c3);
        tableEl.appendChild(colgroup);
    } catch (err) {
        // non-fatal: if DOM APIs fail, continue without colgroup
    }
    tableEl.appendChild(headerRow);

    Object.entries(data).forEach(([key, value]) => {
        const row = document.createElement('tr');
        const keyCell = document.createElement('td');
        keyCell.textContent = key;
        keyCell.classList.add('key-col');
        const valueCell = document.createElement('td');
        valueCell.classList.add('value-col');
        const commentCell = document.createElement('td');
        commentCell.classList.add('comment-col');
        if (value && typeof value === 'object' && ('value' in value || 'comment' in value)) {
            valueCell.textContent = value.value || '';
            commentCell.textContent = value.comment || '';
        } else {
            const valueStr = value === null || value === undefined ? '' : String(value);
            const parts = valueStr.split('/');
            valueCell.textContent = parts[0] || '';
            commentCell.textContent = parts.slice(1).join('/') || '';
        }
        row.appendChild(keyCell);
        row.appendChild(valueCell);
        row.appendChild(commentCell);
        tableEl.appendChild(row);
        // After the row is in the DOM, mark cells that overflow so hover shows full content.
        // If the table or modal is hidden, clientWidth may be zero — retry a few times until layout is available.
        try {
            const cells = [keyCell, valueCell, commentCell];
            let attempts = 0;
            const maxAttempts = 6; // try for up to ~300ms
            const checkOverflow = () => {
                attempts += 1;
                // If table has width, we can perform reliable checks
                if (tableEl.offsetWidth > 0 || tableEl.clientWidth > 0) {
                    cells.forEach((cell) => {
                        if ((cell.scrollWidth || 0) > (cell.clientWidth || 0) + 1) {
                            cell.title = cell.textContent || '';
                            cell.classList.add('trunc');
                        } else {
                            cell.removeAttribute('title');
                            cell.classList.remove('trunc');
                        }
                    });
                } else if (attempts < maxAttempts) {
                    // wait a bit and try again
                    requestAnimationFrame(checkOverflow);
                } else {
                    // give up; avoid falsely marking truncation when layout never becomes available
                    cells.forEach((cell) => {
                        cell.removeAttribute('title');
                        cell.classList.remove('trunc');
                    });
                }
            };
            checkOverflow();
        } catch (err) {
            // non-fatal
        }
    });

    // Restore layout
    if (prevHeight > 0) {
        tableEl.style.minHeight = '';
        if (scrollParent && savedScrollTop > 0) {
            scrollParent.scrollTop = savedScrollTop;
        }
    }
}

function updateFilePathBanner(filePath) {
    const banner = document.getElementById('filePathBanner');
    if (!banner) return;
    if (!filePath) {
        banner.textContent = 'No file loaded';
        banner.title = 'No file loaded';
        banner.classList.add('is-empty');
        banner.setAttribute('aria-label', 'No FITS file loaded');
        return;
    }
    banner.textContent = filePath;
    banner.title = filePath;
    banner.classList.remove('is-empty');
    banner.setAttribute('aria-label', `Selected FITS file ${filePath}`);
}

function formatValue(num) {
    if (!Number.isFinite(num)) return '—';
    if (Math.abs(num) >= 1000 || Math.abs(num) < 0.01) {
        return num.toExponential(2);
    }
    return num.toFixed(2);
}
