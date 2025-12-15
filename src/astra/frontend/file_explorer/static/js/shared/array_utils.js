// Common array and statistics utilities shared across viewer scripts
(function (global) {
    'use strict';

    function linearFit(x, y, badpix) {
        let sumX = 0,
            sumY = 0,
            sumXY = 0,
            sumX2 = 0,
            n = 0;
        for (let i = 0; i < x.length; i++) {
            if (!badpix[i]) {
                const xi = x[i];
                const yi = y[i];
                sumX += xi;
                sumY += yi;
                sumXY += xi * yi;
                sumX2 += xi * xi;
                n++;
            }
        }
        const denominator = n * sumX2 - sumX * sumX;
        const slope = (n * sumXY - sumX * sumY) / denominator;
        const intercept = (sumY - slope * sumX) / n;
        return { slope, intercept };
    }

    function std(arr) {
        if (!arr || arr.length === 0) return 0;
        let mean = 0;
        for (let i = 0; i < arr.length; i++) {
            mean += arr[i];
        }
        mean /= arr.length;
        let variance = 0;
        for (let i = 0; i < arr.length; i++) {
            const diff = arr[i] - mean;
            variance += diff * diff;
        }
        variance /= arr.length;
        return Math.sqrt(variance);
    }

    function partition(arr, left, right) {
        const pivotValue = arr[right];
        let pivotIndex = left;
        for (let i = left; i < right; i++) {
            if (arr[i] < pivotValue) {
                [arr[i], arr[pivotIndex]] = [arr[pivotIndex], arr[i]];
                pivotIndex++;
            }
        }
        [arr[right], arr[pivotIndex]] = [arr[pivotIndex], arr[right]];
        return pivotIndex;
    }

    function quickSelect(arr, k) {
        // operates in-place on arr
        let left = 0;
        let right = arr.length - 1;
        while (left <= right) {
            const pivotIndex = partition(arr, left, right);
            if (pivotIndex === k) {
                return arr[k];
            } else if (pivotIndex < k) {
                left = pivotIndex + 1;
            } else {
                right = pivotIndex - 1;
            }
        }
        return arr[k];
    }

    function medianValue(arr) {
        if (!arr || arr.length === 0) return 0;
        const n = arr.length;
        const k = Math.floor(n / 2);
        // make a copy to avoid mutating caller
        return quickSelect(arr.slice(), k);
    }

    const exports = {
        linearFit,
        std,
        medianValue,
        quickSelect,
        partition,
    };

    // Attach to global namespace for simple import-less use in legacy scripts
    global.arrayUtils = exports;

    // Support common module systems (minimal)
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = exports;
    }
})(typeof window !== 'undefined' ? window : this);
