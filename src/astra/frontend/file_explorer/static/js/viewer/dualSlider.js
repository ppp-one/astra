export function initDualSlider(containerEl, minInputEl, maxInputEl) {
    if (!containerEl || !minInputEl || !maxInputEl) return;

    // create elements if not present
    const track = containerEl.querySelector('.ds-track') || document.createElement('div');
    track.className = 'ds-track';
    if (!containerEl.contains(track)) containerEl.appendChild(track);

    const fill = containerEl.querySelector('.ds-fill') || document.createElement('div');
    fill.className = 'ds-fill';
    if (!track.contains(fill)) track.appendChild(fill);

    const thumbMin = containerEl.querySelector('.ds-thumb-min') || document.createElement('div');
    thumbMin.className = 'ds-thumb ds-thumb-min';
    const thumbMax = containerEl.querySelector('.ds-thumb-max') || document.createElement('div');
    thumbMax.className = 'ds-thumb ds-thumb-max';
    if (!containerEl.contains(thumbMin)) containerEl.appendChild(thumbMin);
    if (!containerEl.contains(thumbMax)) containerEl.appendChild(thumbMax);

    // Utility: set positions given slider values (0..1000)
    function setPositionsFromInputs() {
        const minV = Number(minInputEl.value);
        const maxV = Number(maxInputEl.value);
        const minPct = Math.max(0, Math.min(1000, minV)) / 10; // 0..100
        const maxPct = Math.max(0, Math.min(1000, maxV)) / 10;
        thumbMin.style.left = `${minPct}%`;
        thumbMax.style.left = `${maxPct}%`;
        fill.style.left = `${minPct}%`;
        fill.style.width = `${Math.max(0, maxPct - minPct)}%`;
    }

    // initialize positions
    setPositionsFromInputs();

    // Map pointer x to slider value 0..1000
    function xToValue(clientX) {
        const r = track.getBoundingClientRect();
        const x = Math.max(0, Math.min(r.width, clientX - r.left));
        const t = x / r.width; // 0..1
        return Math.round(t * 1000);
    }

    let active = null; // 'min'|'max'|null

    function onPointerDown(ev) {
        ev.preventDefault();
        const target = ev.target;
        if (target === thumbMin) active = 'min';
        else if (target === thumbMax) active = 'max';
        else {
            // click on track: choose nearest
            const v = xToValue(ev.clientX);
            const dmin = Math.abs(v - Number(minInputEl.value));
            const dmax = Math.abs(v - Number(maxInputEl.value));
            active = dmin <= dmax ? 'min' : 'max';
        }
        document.addEventListener('pointermove', onPointerMove);
        document.addEventListener('pointerup', onPointerUp, { once: true });
        // immediately move
        onPointerMove(ev);
    }

    function onPointerMove(ev) {
        if (!active) return;
        const v = xToValue(ev.clientX);
        if (active === 'min') {
            const maxV = Number(maxInputEl.value);
            const newV = Math.min(v, maxV);
            minInputEl.value = String(newV);
            minInputEl.dispatchEvent(new Event('input'));
        } else {
            const minV = Number(minInputEl.value);
            const newV = Math.max(v, minV);
            maxInputEl.value = String(newV);
            maxInputEl.dispatchEvent(new Event('input'));
        }
        setPositionsFromInputs();
    }

    function onPointerUp() {
        active = null;
        document.removeEventListener('pointermove', onPointerMove);
    }

    // support clicking on track
    track.addEventListener('pointerdown', onPointerDown);
    thumbMin.addEventListener('pointerdown', onPointerDown);
    thumbMax.addEventListener('pointerdown', onPointerDown);

    // keep in sync if inputs are changed programmatically
    minInputEl.addEventListener('input', setPositionsFromInputs);
    maxInputEl.addEventListener('input', setPositionsFromInputs);

    return {
        set: (minV, maxV) => {
            minInputEl.value = String(minV);
            maxInputEl.value = String(maxV);
            minInputEl.dispatchEvent(new Event('input'));
            maxInputEl.dispatchEvent(new Event('input'));
            setPositionsFromInputs();
        },
    };
}
