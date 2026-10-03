function calculateStarFWHM(image, centerX, centerY) {
    if (!image || image.length === 0 || !image[0] || image[0].length === 0) {
        return {
            center: { x: centerX || 0, y: centerY || 0 },
            peak: 0, background: 0, backgroundSigma: 0,
            sharpness: 0, fwhm: 0, fwhmMajor: 0, fwhmMinor: 0, posAngle: 0,
        };
    }

    const height = image.length;
    const width = image[0].length;

    // Find peak and center
    let peak = 0;
    if (centerX === undefined || centerY === undefined) {
        for (let y = 0; y < height; y++)
            for (let x = 0; x < width; x++)
                if (image[y][x] > peak) { peak = image[y][x]; centerX = x; centerY = y; }
    } else {
        peak = image[Math.round(centerY)][Math.round(centerX)];
    }

    // Background: mean and sigma from border pixels
    const borderSize = Math.min(
        Math.max(5, Math.floor(Math.min(width, height) * 0.1)),
        Math.floor(Math.min(width, height) / 2)
    );
    let background = 0, backgroundCount = 0;
    function addBorder(y, x) { background += image[y][x]; backgroundCount++; }
    for (let y = 0; y < borderSize; y++)
        for (let x = 0; x < width; x++) { addBorder(y, x); addBorder(height - 1 - y, x); }
    for (let y = borderSize; y < height - borderSize; y++)
        for (let x = 0; x < borderSize; x++) { addBorder(y, x); addBorder(y, width - 1 - x); }
    background /= backgroundCount;

    let backgroundVariance = 0;
    function addBorderVar(y, x) { const d = image[y][x] - background; backgroundVariance += d * d; }
    for (let y = 0; y < borderSize; y++)
        for (let x = 0; x < width; x++) { addBorderVar(y, x); addBorderVar(height - 1 - y, x); }
    for (let y = borderSize; y < height - borderSize; y++)
        for (let x = 0; x < borderSize; x++) { addBorderVar(y, x); addBorderVar(y, width - 1 - x); }
    const backgroundSigma = Math.sqrt(backgroundVariance / backgroundCount);

    // Sub-pixel centre: background-subtracted centroid in a 5x5 box around the peak
    {
        const px = Math.round(centerX), py = Math.round(centerY);
        let sw = 0, sx = 0, sy = 0;
        for (let y = Math.max(0, py - 2); y <= Math.min(height - 1, py + 2); y++)
            for (let x = Math.max(0, px - 2); x <= Math.min(width - 1, px + 2); x++) {
                const w = image[y][x] - background;
                if (w > 0) { sw += w; sx += w * x; sy += w * y; }
            }
        if (sw > 0) { centerX = sx / sw; centerY = sy / sw; }
    }

    // Radial profile binning
    const analysisRadius = Math.min(
        Math.min(centerX, width - 1 - centerX, centerY, height - 1 - centerY),
        30
    );
    const nBins = Math.ceil(analysisRadius);
    const radii = new Array(nBins).fill(0);
    const means = new Array(nBins).fill(0);
    const counts = new Array(nBins).fill(0);

    const xMin = Math.max(0, Math.floor(centerX - analysisRadius));
    const xMax = Math.min(width - 1, Math.ceil(centerX + analysisRadius));
    const yMin = Math.max(0, Math.floor(centerY - analysisRadius));
    const yMax = Math.min(height - 1, Math.ceil(centerY + analysisRadius));

    for (let y = yMin; y <= yMax; y++) {
        const dy = y - centerY;
        for (let x = xMin; x <= xMax; x++) {
            const dx = x - centerX;
            const r = Math.sqrt(dx * dx + dy * dy);
            const bin = Math.floor(r);
            if (bin < nBins) { radii[bin] += r; means[bin] += image[y][x]; counts[bin]++; }
        }
    }

    let meanPeak = 0;
    for (let bin = 0; bin < nBins; bin++) {
        if (counts[bin] > 0) {
            means[bin] /= counts[bin];
            radii[bin] /= counts[bin];
            if (means[bin] > meanPeak) meanPeak = means[bin];
        } else {
            means[bin] = radii[bin] = NaN;
        }
    }


    // Normalize and smooth (3-bin moving average)
    const normalizedMeans = means.map(v => isNaN(v) ? NaN : (v - background) / (meanPeak - background));
    const smoothedNorm = normalizedMeans.map((v, i) => {
        if (i === 0 || i === nBins - 1) return v;
        const vals = [normalizedMeans[i - 1], v, normalizedMeans[i + 1]].filter(x => !isNaN(x));
        return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : NaN;
    });

    // Radial FWHM, first estimate: first crossing of 0.5
    let fwhmCrossing = 0;
    for (let bin = 1; bin < nBins; bin++) {
        if (!isNaN(smoothedNorm[bin - 1]) && !isNaN(smoothedNorm[bin]) &&
            smoothedNorm[bin - 1] > 0.5 && smoothedNorm[bin] <= 0.5) {
            const m = (smoothedNorm[bin] - smoothedNorm[bin - 1]) / (radii[bin] - radii[bin - 1]);
            fwhmCrossing = 2.0 * (radii[bin - 1] + (0.5 - smoothedNorm[bin - 1]) / m);
            break;
        }
    }

    // Radial FWHM, refined: least-squares fit of A * exp(-r^2 / (2 sigma^2)) to the
    // pixels around the centre (Gauss-Newton, background fixed). The crossing above
    // normalises by the brightest bin mean, so it reads high when the star is small
    // or falls between pixels. Fitting all pixels within a radius, in linear space,
    // avoids that and the bias a per-pixel noise cut would add.
    let fwhm = fwhmCrossing, amplitude = 0;
    if (fwhmCrossing > 0) {
        const fitR = Math.min(analysisRadius, Math.max(3, 1.5 * fwhmCrossing));
        const pts = [];
        for (let y = yMin; y <= yMax; y++) {
            for (let x = xMin; x <= xMax; x++) {
                const dx = x - centerX, dy = y - centerY;
                const r2 = dx * dx + dy * dy;
                if (r2 <= fitR * fitR) pts.push(r2, image[y][x] - background);
            }
        }
        let A = meanPeak - background;
        let s = 1 / (2 * (fwhmCrossing / 2.3548) ** 2); // s = 1 / (2 sigma^2)
        let ok = A > 0, converged = false;
        for (let iter = 0; iter < 20 && ok; iter++) {
            let haa = 0, has = 0, hss = 0, ga = 0, gs = 0;
            for (let i = 0; i < pts.length; i += 2) {
                const r2 = pts[i], g = Math.exp(-s * r2), e = pts[i + 1] - A * g;
                const ja = g, js = -A * r2 * g;
                haa += ja * ja; has += ja * js; hss += js * js; ga += ja * e; gs += js * e;
            }
            const det = haa * hss - has * has;
            if (!(det > 0)) { ok = false; break; }
            const dA = (hss * ga - has * gs) / det;
            const ds = (haa * gs - has * ga) / det;
            // Keep both parameters positive by limiting each step
            A = Math.max(A + dA, A * 0.5);
            s = Math.min(Math.max(s + ds, s * 0.5), s * 2);
            if (Math.abs(ds) < 1e-6 * s && Math.abs(dA) < 1e-6 * A) { converged = true; break; }
        }
        // Keep the first estimate if the fit did not converge or ran away (blends,
        // structured background); the crossing reads at most ~1.5x too high.
        const fitted = 2 * Math.sqrt(2 * Math.LN2) / Math.sqrt(2 * s);
        if (ok && converged && fitted > 0.4 * fwhmCrossing && fitted < 2 * fwhmCrossing) {
            fwhm = fitted;
            amplitude = A;
        }
    }

    // Sharpness: hot-pixel rejection — real stars retain signal at bin 1
    let sharpness = 0;
    if (nBins >= 2 && !isNaN(normalizedMeans[0]) && !isNaN(normalizedMeans[1]) && normalizedMeans[0] > 0)
        sharpness = normalizedMeans[1] / normalizedMeans[0];

    // Directional FWHM: cast 180 spokes at 1° intervals using bilinear-interpolated samples.
    // The widest and narrowest full-widths become fwhmMajor/fwhmMinor (elongation / position angle).
    const halfMax = background + (amplitude > 0 ? amplitude : meanPeak - background) * 0.5;
    const searchR = fwhm > 0 ? Math.min(fwhm * 3, analysisRadius) : analysisRadius;
    const step = 0.25;

    function sampleAt(r, cosA, sinA) {
        const px = centerX + r * cosA, py = centerY + r * sinA;
        const x0 = Math.floor(px), y0 = Math.floor(py);
        const x1 = x0 + 1, y1 = y0 + 1;
        if (x0 < 0 || y0 < 0 || x1 >= width || y1 >= height) return NaN;
        const fx = px - x0, fy = py - y0;
        return image[y0][x0] * (1 - fx) * (1 - fy) + image[y0][x1] * fx * (1 - fy) +
            image[y1][x0] * (1 - fx) * fy + image[y1][x1] * fx * fy;
    }

    const halfWidths = [];
    for (let i = 0; i < 180; i++) {
        const angle = (i / 180) * Math.PI;
        const cosA = Math.cos(angle), sinA = Math.sin(angle);
        let hw = 0;
        for (const sign of [1, -1]) {
            let prev = sampleAt(0, cosA, sinA);
            for (let r = step; r <= searchR; r += step) {
                const val = sampleAt(r * sign, cosA, sinA);
                if (isNaN(val)) break;
                if (prev > halfMax && val <= halfMax) {
                    hw += (r - step) + step * (prev - halfMax) / (prev - val);
                    break;
                }
                prev = val;
            }
        }
        if (hw > 0) halfWidths.push({ fw: hw, angle: angle * 180 / Math.PI });
    }

    let fwhmMajor = 0, fwhmMinor = 0, posAngle = 0;
    if (halfWidths.length > 0) {
        halfWidths.sort((a, b) => b.fw - a.fw);
        fwhmMajor = halfWidths[0].fw;
        fwhmMinor = halfWidths[halfWidths.length - 1].fw;
        posAngle = halfWidths[0].angle;
    }

    return {
        center: { x: centerX, y: centerY },
        peak, background, backgroundSigma,
        sharpness, fwhm, fwhmMajor, fwhmMinor, posAngle, amplitude,
    };
}

// Extract a 2D subarray centred on (centerX, centerY)
function extractSubarray(image, centerX, centerY, size, imageWidth, imageHeight) {
    const halfSize = Math.floor(size / 2);
    const startX = Math.max(0, centerX - halfSize);
    const startY = Math.max(0, centerY - halfSize);
    const endX = Math.min(imageWidth - 1, centerX + halfSize);
    const endY = Math.min(imageHeight - 1, centerY + halfSize);
    const width = endX - startX + 1;
    const height = endY - startY + 1;
    const subarray = Array.from({ length: height }, (_, y) =>
        Array.from({ length: width }, (_, x) => image[(startY + y) * imageWidth + (startX + x)])
    );
    return { array: subarray, offsetX: startX, offsetY: startY };
}

// Locate a star near (x, y) and compute its FWHM with an adaptively-sized box.
// The box is always re-centred on the detected peak before the final measurement
// so that background border pixels are symmetric around the star.
export function calculateAdaptiveFWHM(x, y, _plateScale, imageData, imageWidth, imageHeight,
    { boxSizePx = 20, boxSizeArcsec = 20, maxBoxSizePx = 200 } = {}
) {
    // Default box is boxSizePx pixels; scale to boxSizeArcsec arcsec when plate scale is known.
    let boxSize = boxSizePx;
    if (_plateScale) boxSize = Math.ceil(boxSizeArcsec / _plateScale);

    // Pass 1: rough extraction at cursor position to find the peak
    let { array, offsetX, offsetY } = extractSubarray(imageData, x, y, boxSize, imageWidth, imageHeight);
    let fwhmResult = calculateStarFWHM(array);

    // Pass 2: re-extract centred on the detected peak
    let peakX = Math.round(fwhmResult.center.x + offsetX);
    let peakY = Math.round(fwhmResult.center.y + offsetY);
    ({ array, offsetX, offsetY } = extractSubarray(imageData, peakX, peakY, boxSize, imageWidth, imageHeight));
    fwhmResult = calculateStarFWHM(array);

    // Expand box up to 3× if the star fills more than 1/5 of the current box
    for (let iter = 0; iter < 3 && fwhmResult.fwhm * 5 > boxSize; iter++) {
        boxSize = Math.min(Math.ceil(fwhmResult.fwhm * 10), Math.min(imageWidth, imageHeight) / 2, maxBoxSizePx);
        peakX = Math.round(fwhmResult.center.x + offsetX);
        peakY = Math.round(fwhmResult.center.y + offsetY);
        ({ array, offsetX, offsetY } = extractSubarray(imageData, peakX, peakY, boxSize, imageWidth, imageHeight));
        fwhmResult = calculateStarFWHM(array);
    }

    // Convert subarray-local centre back to full-image coordinates
    fwhmResult.center.x += offsetX;
    fwhmResult.center.y += offsetY;
    return fwhmResult;
}

// Draw aperture circles on the canvas. `ctx` maps image coordinates so that
// pixel (x, y) covers [x, x + 1) x [y, y + 1); the estimator puts pixel centres
// on integer indices, hence the half-pixel shift.
export function drawApertureCircles(fwhmResult, scale, ctx) {
    const { center, fwhm } = fwhmResult;
    const cx = center.x + 0.5, cy = center.y + 0.5;
    ctx.lineWidth = scale * 2;

    // FWHM circle (black)
    ctx.strokeStyle = 'rgba(0, 0, 0, 0.8)';
    ctx.beginPath();
    ctx.arc(cx, cy, fwhm / 2, 0, Math.PI * 2);
    ctx.stroke();

    // Outer aperture at 2.5 × FWHM (green)
    ctx.strokeStyle = 'rgba(26, 255, 0, 0.8)';
    ctx.beginPath();
    ctx.arc(cx, cy, fwhm * 2.5, 0, Math.PI * 2);
    ctx.stroke();
}
