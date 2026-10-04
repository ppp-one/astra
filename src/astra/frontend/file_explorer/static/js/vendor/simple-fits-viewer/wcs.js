/*
 * This file includes code adapted from Astrometry.net.
 *
 * Original source:
 *   https://github.com/dstndstn/astrometry.net/blob/master/util/sip.c
 *
 * License:
 *   3-clause BSD license (see LICENSE)
 *
 * This code was rewritten from C to JavaScript and modified
 * to fit this project.
 */

const ANGULAR_STEPS = [
    30, 20, 15, 10, 5, 2, 1,
    30 / 60, 20 / 60, 15 / 60, 10 / 60, 5 / 60, 2 / 60, 1 / 60,
    30 / 3600, 20 / 3600, 15 / 3600, 10 / 3600, 5 / 3600, 2 / 3600, 1 / 3600
];

function calculateGridTicks(range, targetTicks = 6) {
    const idealStep = range / targetTicks;

    for (const step of ANGULAR_STEPS) {
        if (step <= idealStep) {
            return step;
        }
    }

    return ANGULAR_STEPS[ANGULAR_STEPS.length - 1];
}

function intersect(x1, y1, x2, y2, width, height) {
    let hits = [];

    const dx = x1 - x2;
    const dy = y1 - y2;
    let angle = Math.atan(dy / dx);
    // left
    if (x1 * x2 < 0) {
        const t = (0 - x1) / (x2 - x1);
        const y = y1 + t * (y2 - y1);
        if (0 <= y && y <= height) hits.push({ edge: "left", x: 0, y, angle });
    }

    // right
    if ((x1 - width) * (x2 - width) < 0) {
        const t = (width - x1) / (x2 - x1);
        const y = y1 + t * (y2 - y1);
        if (0 <= y && y <= height) hits.push({ edge: "right", x: width, y, angle });
    }

    // top
    if (y1 * y2 < 0) {
        const t = (0 - y1) / (y2 - y1);
        const x = x1 + t * (x2 - x1);
        if (0 <= x && x <= width) hits.push({ edge: "top", x, y: 0, angle });
    }

    // bottom
    if ((y1 - height) * (y2 - height) < 0) {
        const t = (height - y1) / (y2 - y1);
        const x = x1 + t * (x2 - x1);
        if (0 <= x && x <= width) hits.push({ edge: "bottom", x, y: height, angle });
    }

    return hits;
}

function groupByEdge(hits) {
    const map = {
        bottom: [],
        top: [],
        left: [],
        right: []
    };

    for (const h of hits) {
        if (map[h.edge]) map[h.edge].push(h);
    }

    return map;
}

function selectHitByPriority(hits, priority, W, H) {
    const grouped = groupByEdge(hits);

    for (const edge of priority) {
        const list = grouped[edge];
        if (!list || list.length === 0) continue;

        let best = list[0];
        let bestScore = Infinity;

        for (const h of list) {
            let d;
            if (edge === "left" || edge === "right") {
                d = Math.abs(h.y - H * 0.5);
            } else {
                d = Math.abs(h.x - W * 0.5);
            }

            if (d < bestScore) {
                bestScore = d;
                best = h;
            }
        }

        return best;
    }

    return null;
}

function decideAngularUnit(stepDeg) {
    if (stepDeg >= 10) {
        return 1;
    }
    if (stepDeg >= 1) {
        return 60;
    }

    return 3600;
}

function formatRa(raDeg, stepDeg) {
    let v = ((raDeg % 360) + 360) % 360;

    const hours = v / 15;

    const scale = decideAngularUnit(stepDeg);

    let total = Math.round(hours * scale);

    if (scale === 1) {
        return `${total}h`;
    }

    if (scale === 60) {
        const h = Math.floor(total / 60);
        const m = total % 60;
        return `${h}h${m.toString().padStart(2, "0")}m`;
    }

    const h = Math.floor(total / 3600);
    total %= 3600;
    const m = Math.floor(total / 60);
    const s = total % 60;

    return `${h}h${m.toString().padStart(2, "0")}m${s.toString().padStart(2, "0")}s`;
}

function formatDec(decDeg, stepDeg) {
    const sign = decDeg < 0 ? "−" : "+";
    let v = Math.abs(decDeg);

    const scale = decideAngularUnit(stepDeg);

    let total = Math.round(v * scale);

    if (scale === 1) {
        return `${sign}${total}°`;
    }

    if (scale === 60) {
        const deg = Math.floor(total / 60);
        const min = total % 60;
        return `${sign}${deg}°${min.toString().padStart(2, "0")}′`;
    }

    // arcsec
    const deg = Math.floor(total / 3600);
    total %= 3600;
    const min = Math.floor(total / 60);
    const sec = total % 60;

    return `${sign}${deg}°${min.toString().padStart(2, "0")}′${sec.toString().padStart(2, "0")}″`;
}

const deg2arcsec = (deg) => deg * 3600.0;
const deg2rad = (deg) => deg * (Math.PI / 180);
const radec2x = (r, d) => Math.cos(d) * Math.cos(r);
const radec2y = (r, d) => Math.cos(d) * Math.sin(r);
const radec2z = (r, d) => Math.sin(d);
const radec2xyz = (r, d) => [radec2x(r, d), radec2y(r, d), radec2z(r, d)];
const radecdeg2xyz = (r, d) => radec2xyz(deg2rad(r), deg2rad(d));

function normalize(x, y, z) {
    let invl = 1.0 / Math.sqrt(x * x + y * y + z * z);
    return [x * invl, y * invl, z * invl];
}

const rad2deg = (rad) => rad * (180 / Math.PI);
const z2dec = (z) => Math.asin(z);
function xy2ra(x, y) {
    let a = Math.atan2(y, x);
    if (a < 0) {
        a += 2.0 * Math.PI;
    }
    return a;
}

function xyz2radec(x, y, z) {
    return [xy2ra(x, y), z2dec(z)];
}

function xyz2radecdeg(xyz) {
    let [ra, dec] = xyz2radec(xyz[0], xyz[1], xyz[2]);
    return [rad2deg(ra), rad2deg(dec)];
}

function wcsPixelCenterForSize(offset, size) {
    return offset + 0.5 + 0.5 * size;
}

function starCoords(s, r, tangent) {
    let x = 0;
    let y = 0;
    // As used by the sip.c code, this does the TAN projection
    // (if "tangent" is TRUE; SIN projection otherwise)
    // r: CRVAL
    // s: RA,Dec to be projected
    // ASSUME r,s are unit vectors
    // sdotr:  s dot r = |r||s| cos(theta) = cos(theta)
    let sdotr = s[0] * r[0] + s[1] * r[1] + s[2] * r[2];

    const COS_MAX_THETA = Math.cos(80 * Math.PI / 180);
    if (sdotr <= COS_MAX_THETA) {
        // Limit angular distance to avoid TAN projection divergence
        return null;
    }
    if (r[2] == 1.0) {
        // North pole
        let invs2 = 1.0 / s[2];
        if (tangent) {
            x = s[0] * invs2;
            y = s[1] * invs2;
        } else {
            x = s[0];
            y = s[1];
        }
    } else if (r[2] == -1.0) {
        // South pole
        let invs2 = 1.0 / s[2];
        if (tangent) {
            x = -s[0] * invs2;
            y = s[1] * invs2;
        } else {
            x = -s[0];
            y = s[1];
        }
    } else {
        // eta is a vector perpendicular to r pointing in the direction
        // of increasing RA.  eta_z = 0 by definition.
        let etax = -r[1];
        let etay = r[0];
        let etanorm = Math.hypot(etax, etay);
        let inven = 1.0 / etanorm;
        etax *= inven;
        etay *= inven;

        // xi =  r cross eta, a vector pointing northwards,
        // in direction of increasing DEC
        let xix = -r[2] * etay;
        let xiy = r[2] * etax;
        let xiz = r[0] * etay - r[1] * etax;

        // project s-r onto eta and xi.  No need to subtract r from s, though,
        // since eta and xi are orthogonal to r by construction.
        x = (s[0] * etax + s[1] * etay);
        y = (s[0] * xix + s[1] * xiy + s[2] * xiz);

        // The "inv_sdotr" applies the TAN scaling
        if (tangent) {
            let invsdotr = 1.0 / sdotr;
            x *= invsdotr;
            y *= invsdotr;
        }
    }
    return [x, y];
}

function invert2by2Arr(a) {
    let ainv = new Array(2).fill(0).map(() => new Array(2).fill(0));
    let det = a[0][0] * a[1][1] - a[0][1] * a[1][0];

    if (det == 0.0) {
        return null;
    }

    let invdet = 1.0 / det;
    ainv[0][0] = a[1][1] * invdet;
    ainv[0][1] = -a[0][1] * invdet;
    ainv[1][0] = -a[1][0] * invdet;
    ainv[1][1] = a[0][0] * invdet;
    return ainv;
}


function parseWCSPolynomial(header, name, order) {
    const data = Array.from({ length: 10 }, () => Array(10).fill(0));

    // Terms A_i_j with i + j <= order (the highest-order terms included)
    for (let i = 0; i <= order; i++) {
        for (let j = 0; j <= order - i; j++) {
            const key = `${name}_${i}_${j}`;
            if (header[key] !== undefined) {
                data[i][j] = parseFloat(header[key].split("/")[0]);
            }
        }
    }

    return data;
}

function parseHeaderInt(header, key, fallback = -1) {
    const raw = header[key];
    if (raw === undefined) {
        return fallback;
    }

    const parsed = parseInt(raw.split("/")[0], 10);
    return Number.isNaN(parsed) ? fallback : parsed;
}

function parseHeaderFloat(header, key, fallback = NaN) {
    const raw = header[key];
    if (raw === undefined) {
        return fallback;
    }
    const parsed = parseFloat(raw.split("/")[0]);
    return Number.isNaN(parsed) ? fallback : parsed;
}


export class WCS {
    // Parse WCS header
    // check if wcs is included
    constructor(header) {
        const rawCtype1 = header["CTYPE1"];
        const rawCtype2 = header["CTYPE2"];
        if (!rawCtype1 || !rawCtype2) {
            throw new Error("Missing CTYPE1/CTYPE2 in WCS header");
        }

        this.wcsaxes = parseHeaderInt(header, "WCSAXES", 2);
        this.ctype1 = rawCtype1.split("/")[0].trim();
        this.ctype2 = rawCtype2.split("/")[0].trim();

        const ctype1Upper = this.ctype1.toUpperCase();
        const ctype2Upper = this.ctype2.toUpperCase();
        const isSin = ctype1Upper.includes("SIN") && ctype2Upper.includes("SIN");
        const isTan = ctype1Upper.includes("TAN") && ctype2Upper.includes("TAN");

        if (isSin) {
            this.sin = true;
        } else if (isTan) {
            this.sin = false;
        } else {
            console.error(`Unsupported wcs format: ${this.ctype1}`);
        }

        const hasSipCoeffs = Boolean(
            this.ctype1.includes("-SIP") ||
            this.ctype2.includes("-SIP") ||
            header["A_ORDER"] ||
            header["B_ORDER"] ||
            header["AP_ORDER"] ||
            header["BP_ORDER"]
        );

        this.equinox = parseHeaderFloat(header, "EQUINOX", 2000);
        this.lonpole = parseHeaderFloat(header, "LONPOLE", 180.0);
        this.latpole = parseHeaderFloat(header, "LATPOLE", 0.0);

        this.crval = [];
        this.crval.push(parseHeaderFloat(header, "CRVAL1"));
        this.crval.push(parseHeaderFloat(header, "CRVAL2"));

        this.crpix = [];
        this.crpix.push(parseHeaderFloat(header, "CRPIX1"));
        this.crpix.push(parseHeaderFloat(header, "CRPIX2"));

        this.cunit1 = (header["CUNIT1"] || "deg").split("/")[0];
        this.cunit2 = (header["CUNIT2"] || "deg").split("/")[0];

        // Build CD matrix: prefer CD*, else PC*CDELT fallback
        const cd11 = parseHeaderFloat(header, "CD1_1");
        const cd12 = parseHeaderFloat(header, "CD1_2");
        const cd21 = parseHeaderFloat(header, "CD2_1");
        const cd22 = parseHeaderFloat(header, "CD2_2");
        const hasCD = [cd11, cd12, cd21, cd22].every(Number.isFinite);

        if (hasCD) {
            this.cd = [[cd11, cd12], [cd21, cd22]];
        } else {
            const cdelt1 = parseHeaderFloat(header, "CDELT1", 1.0);
            const cdelt2 = parseHeaderFloat(header, "CDELT2", 1.0);
            const pc11 = parseHeaderFloat(header, "PC1_1", 1.0);
            const pc12 = parseHeaderFloat(header, "PC1_2", 0.0);
            const pc21 = parseHeaderFloat(header, "PC2_1", 0.0);
            const pc22 = parseHeaderFloat(header, "PC2_2", 1.0);
            if ([cdelt1, cdelt2, pc11, pc12, pc21, pc22].every(Number.isFinite)) {
                this.cd = [
                    [pc11 * cdelt1, pc12 * cdelt1],
                    [pc21 * cdelt2, pc22 * cdelt2],
                ];
            } else {
                throw new Error("Missing CD or PC/CDELT WCS matrix terms");
            }
        }

        this.imagew = parseHeaderInt(header, "IMAGEW", parseHeaderInt(header, "NAXIS1", -1));
        this.imageh = parseHeaderInt(header, "IMAGEH", parseHeaderInt(header, "NAXIS2", -1));

        this.aorder = hasSipCoeffs ? parseHeaderInt(header, "A_ORDER") : -1;
        this.a = this.aorder >= 0 ? parseWCSPolynomial(header, "A", this.aorder) : null;
        this.border = hasSipCoeffs ? parseHeaderInt(header, "B_ORDER") : -1;
        this.b = this.border >= 0 ? parseWCSPolynomial(header, "B", this.border) : null;
        this.aporder = hasSipCoeffs ? parseHeaderInt(header, "AP_ORDER") : -1;
        this.ap = this.aporder >= 0 ? parseWCSPolynomial(header, "AP", this.aporder) : null;
        this.bporder = hasSipCoeffs ? parseHeaderInt(header, "BP_ORDER") : -1;
        this.bp = this.bporder >= 0 ? parseWCSPolynomial(header, "BP", this.bporder) : null;
    }

    sipGetRadecBounds(stepsize, x = 0, y = 0, w = this.imagew, h = this.imageh) {
        let [rac, decc] = this.sipGetRadecCenter(x, y, w, h);

        let [ramin, ramax, decmin, decmax] = this.sipWalkImageBoundary(stepsize, rac, decc, x, y, w, h);

        // Check for poles...
        // north pole
        if (this.sipIsInsideImage(0, 90, x, y, w, h)) {
            ramin = 0;
            ramax = 360;
            decmax = 90;
        }
        if (this.sipIsInsideImage(0, -90, x, y, w, h)) {
            ramin = 0;
            ramax = 360;
            decmin = -90;
        }

        return [ramin, ramax, decmin, decmax];
    }

    sipIsInsideImage(ra, dec, x, y, w, h) {
        let xy = this.sipRadec2Pixelxy(ra, dec);
        if (xy == null) {
            return false;
        }
        return this.tanPixelIsInsideImage(xy[0], xy[1], x, y, w, h);
    }

    sipRadec2Pixelxy(ra, dec) {
        let xy = this.tanRadec2pixelxy(ra, dec);
        if (xy == null) {
            return null;
        }
        return this.sipPixelUndistortion(xy[0], xy[1]);
    }

    sipWalkImageBoundary(stepsize, rac, decc, left, top, w, h) {
        // Walk the perimeter of the image in steps of stepsize pixels
        let ramin = rac;
        let ramax = rac;
        let decmin = decc;
        let decmax = decc;

        let xmin = left + 0.5;
        let xmax = left + w + 0.5;
        let ymin = top + 0.5;
        let ymax = top + h + 0.5;
        let offsetx = [xmin, xmax, xmax, xmin];
        let offsety = [ymin, ymin, ymax, ymax];
        let stepx = [+stepsize, 0, -stepsize, 0];
        let stepy = [0, +stepsize, 0, -stepsize];
        let nsteps = [Math.ceil(w / stepsize) + 1, Math.ceil(h / stepsize) + 1, Math.ceil(w / stepsize) + 1, Math.ceil(h / stepsize) + 1];

        for (let side = 0; side < 4; side++) {
            for (let i = 0; i < nsteps[side]; i++) {
                let x = Math.min(xmax, Math.max(xmin, offsetx[side] + i * stepx[side]));
                let y = Math.min(ymax, Math.max(ymin, offsety[side] + i * stepy[side]));

                let [ra, dec] = this.sipPixelxy2radec(x, y);
                decmin = Math.min(decmin, dec);
                decmax = Math.max(decmax, dec);
                if (ra - rac > 180) {
                    // wrap-around: racenter < 180, ra has gone < 0 but been wrapped around to > 180
                    ra -= 360;
                }
                if (rac - ra > 180) {
                    // wrap-around: racenter > 180, ra has gone > 360 but wrapped around to > 0.
                    ra += 360;
                }

                ramin = Math.min(ramin, ra);
                ramax = Math.max(ramax, ra);
            }
        }

        return [ramin, ramax, decmin, decmax];
    }

    sipGetRadecCenter(x, y, w, h) {
        let px = wcsPixelCenterForSize(x, w);
        let py = wcsPixelCenterForSize(y, h);
        return this.sipPixelxy2radec(px, py);
    }

    hasDistortions() {
        return this.aorder >= 0 || this.border >= 0 || this.aporder >= 0 || this.bporder >= 0;
    }

    sipPixelxy2radec(px, py) {
        if (this.hasDistortions()) {
            let [u, v] = this.sipDistortion(px, py);
            // Run a normal TAN conversion on the distorted pixel coords.
            return this.tanPixelxy2radec(u, v);
        } else {
            // Run a normal TAN conversion
            return this.tanPixelxy2radec(px, py);
        }
    }

    sipDistortion(px, py) {
        // Get pixel coordinates relative to reference pixel
        let u = px - this.crpix[0];
        let v = py - this.crpix[1];
        let xy = this.sipCalcDistortion(u, v);
        xy[0] += this.crpix[0];
        xy[1] += this.crpix[1];
        return xy;
    }

    // Evaluate a SIP polynomial: sum_{i+j<=order} coeffs[i][j] * u^i * v^j
    // powu/powv are pre-filled power arrays of length (order+1).
    _evalSipPolynomial(order, coeffs, powu, powv) {
        let result = 0.0;
        for (let i = 0; i <= order; i++) {
            for (let j = 0; j <= order - i; j++) {
                result += coeffs[i][j] * powu[i] * powv[j];
            }
        }
        return result;
    }

    // Fill powu[0..maxOrder] = u^0..u^maxOrder (avoids Math.pow in hot path)
    _fillPowers(u, v, maxOrder, powu, powv) {
        powu[0] = 1.0; powv[0] = 1.0;
        if (maxOrder >= 1) { powu[1] = u; powv[1] = v; }
        for (let i = 2; i <= maxOrder; i++) {
            powu[i] = powu[i - 1] * u;
            powv[i] = powv[i - 1] * v;
        }
    }

    sipCalcDistortion(u, v) {
        const maxOrder = Math.max(this.aorder, this.border);
        const powu = new Array(maxOrder + 1);
        const powv = new Array(maxOrder + 1);
        this._fillPowers(u, v, maxOrder, powu, powv);
        const fuv = this._evalSipPolynomial(this.aorder, this.a, powu, powv);
        const guv = this._evalSipPolynomial(this.border, this.b, powu, powv);
        return [u + fuv, v + guv];
    }

    sipPixelUndistortion(x, y) {
        if (!this.hasDistortions()) {
            return [x, y];
        }
        let u = x - this.crpix[0];
        let v = y - this.crpix[1];
        if (this.aporder < 0 && this.bporder < 0) {
            // No inverse (AP/BP) terms: invert the forward polynomial by fixed-point
            // iteration, u = U - f(u, v), v = V - g(u, v)
            [x, y] = this._invertForwardSip(u, v);
        } else {
            [x, y] = this.sipCalcInvDistortion(u, v);
        }
        x += this.crpix[0];
        y += this.crpix[1];
        return [x, y];
    }

    _invertForwardSip(U, V) {
        let u = U, v = V;
        for (let i = 0; i < 50; i++) {
            const [du, dv] = this.sipCalcDistortion(u, v);
            const nu = U - (du - u), nv = V - (dv - v);
            const done = Math.abs(nu - u) < 1e-8 && Math.abs(nv - v) < 1e-8;
            u = nu; v = nv;
            if (done) break;
        }
        return [u, v];
    }

    sipCalcInvDistortion(u, v) {
        const maxOrder = Math.max(this.aporder, this.bporder);
        const powu = new Array(maxOrder + 1);
        const powv = new Array(maxOrder + 1);
        this._fillPowers(u, v, maxOrder, powu, powv);
        const fuv = this._evalSipPolynomial(this.aporder, this.ap, powu, powv);
        const guv = this._evalSipPolynomial(this.bporder, this.bp, powu, powv);
        return [u + fuv, v + guv];
    }

    tanPixelxy2iwc(px, py) {
        // Get pixel coordinates relative to reference pixel
        let u = px - this.crpix[0];
        let v = py - this.crpix[1];

        // Get intermediate world coordinates
        let x = this.cd[0][0] * u + this.cd[0][1] * v;
        let y = this.cd[1][0] * u + this.cd[1][1] * v;

        return [x, y]
    }

    tanIwc2xyzarr(x, y) {
        let ix, iy, norm;
        let jx, jy, jz;
        let xyz = [0, 0, 0];

        // Mysterious factor of -1 correcting for vector directions below.
        x = -deg2rad(x);
        y = deg2rad(y);

        // Take r to be the threespace vector of crval
        let [rx, ry, rz] = radecdeg2xyz(this.crval[0], this.crval[1]);

        // FIXME -- what about *near* the poles?
        if (rx == 1.0) {
            // North pole
            ix = -1.0;
            iy = 0.0;
        } else if (rz == -1.0) {
            // South pole
            ix = -1.0;
            iy = 0.0;
        } else {
            // Form i = r cross north pole (0,0,1)
            ix = ry;
            iy = -rx;
            // iz = 0
            norm = Math.hypot(ix, iy);
            ix /= norm;
            iy /= norm;
        }

        // Form j = i cross r;   iz=0 so some terms drop out
        jx = iy * rz;
        jy = - ix * rz;
        jz = ix * ry - iy * rx;
        // norm should already be 1, but normalize anyway
        let [jx_, jy_, jz_] = normalize(jx, jy, jz);

        if (this.sin) {
            console.assert((x * x + y * y) < 1.0);
            // Figure out what factor of r we have to add in to make the resulting length = 1
            let rfrac = Math.sqrt(1.0 - (x * x + y * y));
            // Don't scale the projected x,y positions, just add in the right amount of r to
            // bring it onto the unit sphere
            xyz[0] = ix * x + jx_ * y + rx * rfrac;
            xyz[1] = iy * x + jy_ * y + ry * rfrac;
            xyz[2] = jz_ * y + rz * rfrac; // iz = 0
            return xyz;
        } else {
            // Form the point on the tangent plane relative to observation point,
            xyz[0] = ix * x + jx_ * y + rx;
            xyz[1] = iy * x + jy_ * y + ry;
            xyz[2] = jz_ * y + rz; // iz = 0
            // and normalize back onto the unit sphere
            return normalize(xyz[0], xyz[1], xyz[2]);
        }
    }

    tanPixelxy2xyzarr(px, py) {
        let [x, y] = this.tanPixelxy2iwc(px, py);
        return this.tanIwc2xyzarr(x, y);
    }

    tanPixelxy2radec(px, py) {
        let xyz = this.tanPixelxy2xyzarr(px, py);
        return xyz2radecdeg(xyz);
    }

    tanRadec2pixelxy(a, d) {
        let xyzpt = radecdeg2xyz(a, d);
        return this.tanxyzArr2Pixelxy(xyzpt);
    }

    tanxyzArr2Pixelxy(xyz) {
        let iw = this.tanxyzArr2Iwc(xyz);
        if (iw == null) {
            return null;
        }
        return this.tanIwc2Pixelxy(iw[0], iw[1]);
    }

    tanxyzArr2Iwc(xyz) {
        // FIXME be robust near the poles
        // Calculate intermediate world coordinates (x,y) on the tangent plane
        let xyzcrval = radecdeg2xyz(this.crval[0], this.crval[1]);

        let iw = starCoords(xyz, xyzcrval, !this.sin)
        if (iw == null) {
            return null;
        }

        let iwcx = rad2deg(iw[0]);
        let iwcy = rad2deg(iw[1]);
        return [iwcx, iwcy];
    }

    tanIwc2Pixelxy(x, y) {
        // Invert CD
        let cdi = invert2by2Arr(this.cd);

        // Linear pixel coordinates
        let u = cdi[0][0] * x + cdi[0][1] * y;
        let v = cdi[1][0] * x + cdi[1][1] * y;

        // Re-add crpix to get pixel coordinates
        let px = u + this.crpix[0];
        let py = v + this.crpix[1];

        return [px, py];
    }

    tanPixelIsInsideImage(x, y, top, left, w, h) {
        return (x >= top && x <= top + w && y >= left + 1 && y <= left + h);
    }

    tanPixelScale() {
        let scale = deg2arcsec(Math.sqrt(Math.abs(this.tanDetCd())));
        return scale;
    }

    tanDetCd() {
        return (this.cd[0][0] * this.cd[1][1] - this.cd[0][1] * this.cd[1][0]);
    }
}

export { calculateGridTicks, intersect, groupByEdge, selectHitByPriority, formatRa, formatDec };
