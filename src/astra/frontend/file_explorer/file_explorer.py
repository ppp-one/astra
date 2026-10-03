import io
import logging
import os
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Callable, Optional, Tuple, Union
from urllib.parse import quote

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

logger = logging.getLogger("astra")


ALLOWED_EXTENSIONS = {
    ".fits",
    ".fit",
    ".fts",
    ".png",
}  # Set allowed file types (or None for all)

# Maximum number of items to return from a single directory listing. Prevents
# expensive scans that could cause long blocking I/O.
LIST_MAX_ITEMS = 1_000_000

# zlib level for HTTP gzip and zip archives. Level 1 is ~13x (int16) to ~150x
# (float32) faster than Starlette's default of 9 on FITS data, and the output
# is only a few percent larger. Level 9 made a 67 MB float32 FITS take ~100 s.
COMPRESS_LEVEL = 1

# Read size used when streaming files into a zip archive.
ZIP_CHUNK_SIZE = 1024 * 1024

# Static files (UI assets) directory used by both the app factory and router.
STATIC_DIR = Path(__file__).parent / "static"


class LazyStaticFiles:
    def __init__(self, directory_loader: Callable[[], Path], **kwargs):
        self.directory_loader = directory_loader
        self.kwargs = kwargs
        self.app = None

    async def __call__(self, scope, receive, send):
        if self.app is None:
            directory = self.directory_loader()
            self.app = StaticFiles(directory=str(directory), **self.kwargs)
        await self.app(scope, receive, send)


def _resolve_filename(name: str, fits_dir: Path):
    """Attempt to resolve a filename (relative to FITS_DIR) robustly.

    Returns a Path or None.
    """
    # direct join
    candidate = fits_dir.joinpath(*name.split("/")).resolve()
    if candidate.exists() and candidate.is_file():
        return candidate
    # try unquoting
    from urllib.parse import unquote

    unq = unquote(name)
    candidate = fits_dir.joinpath(*unq.split("/")).resolve()
    if candidate.exists() and candidate.is_file():
        logger.debug(f"Resolved by unquote: {candidate}")
        return candidate
    # try case-insensitive match on basename in parent dir
    parts = name.split("/")
    parent = fits_dir.joinpath(*parts[:-1]) if len(parts) > 1 else fits_dir
    if parent.exists() and parent.is_dir():
        target_basename = parts[-1]
        for p in parent.iterdir():
            if p.name.lower() == target_basename.lower():
                logger.debug(f"Resolved by case-insensitive match: {p}")
                return p.resolve()
    return None


def _safe_file_path(filename: str, fits_dir: Path) -> Path:
    """Resolve a filename relative to fits_dir and validate it."""
    file_path = _resolve_filename(filename, fits_dir)
    logger.debug(f"Resolved {filename!r} to {file_path}")
    if file_path is None:
        raise HTTPException(status_code=404, detail="File not found")

    try:
        file_path.relative_to(fits_dir)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid path")

    if not file_path.exists() or not file_path.is_file():
        parent = file_path.parent
        try:
            listing = [p.name for p in parent.iterdir()]
        except Exception as exc:  # pragma: no cover - diagnostic
            listing = f"(could not list parent: {exc})"
        logger.error(
            "File not found: %s; parent exists=%s; parent listing=%s",
            file_path,
            parent.exists(),
            listing,
        )
        raise HTTPException(status_code=404, detail="File not found")
    return file_path


def _select_hdu(hdul, hdu_index: Optional[int], require_data: bool = False):
    if hdu_index is not None:
        if hdu_index < 0 or hdu_index >= len(hdul):
            raise HTTPException(status_code=404, detail="Invalid HDU index")
        target = hdul[hdu_index]
        if require_data and getattr(target, "data", None) is None:
            raise HTTPException(
                status_code=400, detail="Selected HDU has no image data"
            )
        return target, hdu_index

    for idx, hdu in enumerate(hdul):
        if require_data and getattr(hdu, "data", None) is None:
            continue
        return hdu, idx
    raise HTTPException(status_code=404, detail="No suitable HDU found")


def _extract_image_array(hdu):
    try:
        import numpy as np
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Missing numpy dependency: {exc}")

    data = getattr(hdu, "data", None)
    if data is None:
        raise HTTPException(status_code=400, detail="HDU contains no image data")

    arr = np.asarray(data)
    if arr.ndim == 0:
        raise HTTPException(status_code=400, detail="HDU image data is scalar")
    if arr.ndim == 1:
        arr = arr[np.newaxis, :]
    elif arr.ndim > 2:
        slices = [0] * (arr.ndim - 2) + [slice(None), slice(None)]
        arr = arr[tuple(slices)]

    return np.ascontiguousarray(arr)


def _downsample_array(arr, max_dim: int = 512) -> Tuple[object, int]:
    try:
        import numpy as np
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Missing numpy dependency: {exc}")

    if max_dim <= 0:
        raise HTTPException(status_code=400, detail="max_dim must be positive")

    height, width = arr.shape[-2], arr.shape[-1]
    max_axis = max(height, width)
    if max_axis <= max_dim:
        return np.ascontiguousarray(arr), 1

    stride = int(np.ceil(max_axis / max_dim))
    stride = max(1, stride)

    # Average stride x stride blocks instead of keeping every stride-th pixel:
    # plain decimation drops most stars (a 4k image at stride 8 keeps 1 pixel
    # in 64). Edge rows/columns that do not fill a whole block are dropped.
    block_y = min(stride, height)
    block_x = min(stride, width)
    out_h, out_w = height // block_y, width // block_x
    blocks = arr[: out_h * block_y, : out_w * block_x].reshape(
        out_h, block_y, out_w, block_x
    )
    downsampled = blocks.mean(axis=(1, 3), dtype=np.float64)
    if np.issubdtype(arr.dtype, np.integer):
        # Whole numbers compress much better under HTTP gzip
        downsampled = np.rint(downsampled)
    return np.ascontiguousarray(downsampled, dtype=np.float32), stride


_BITPIX_DTYPES = {
    8: "uint8",
    16: "int16",
    32: "int32",
    64: "int64",
    -32: "float32",
    -64: "float64",
}


def _hdu_shape_and_dtype(header) -> tuple[list[int], str]:
    """Return the data shape (numpy order) and dtype name using only the header."""
    naxis = int(header.get("NAXIS", 0) or 0)
    if naxis == 0:
        return [], ""
    if str(header.get("XTENSION", "")).strip().upper() in ("BINTABLE", "TABLE"):
        return [int(header.get("NAXIS2", 0) or 0)], "table"

    shape = [int(header.get(f"NAXIS{i}", 0) or 0) for i in range(naxis, 0, -1)]
    bitpix = int(header.get("BITPIX", 0) or 0)
    dtype = _BITPIX_DTYPES.get(bitpix, "")
    bscale = header.get("BSCALE", 1)
    bzero = header.get("BZERO", 0)
    if bitpix > 0 and (bscale != 1 or bzero != 0):
        # Same rules astropy uses when it scales integer data
        if bscale == 1 and bitpix > 8 and bzero == 2 ** (bitpix - 1):
            dtype = f"uint{bitpix}"
        elif bscale == 1 and bitpix == 8 and bzero == -128:
            dtype = "int8"
        else:
            dtype = "float32" if bitpix <= 16 else "float64"
    return shape, dtype


def _build_preview_hdul(original_hdu, downsampled, stride: int):
    try:
        import numpy as np
        from astropy.io import fits
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Missing FITS dependencies: {exc}")

    header = original_hdu.header.copy()
    header["NAXIS1"] = downsampled.shape[1]
    header["NAXIS2"] = downsampled.shape[0]
    header["HIERARCH ASTRA PREVIEW"] = True
    header["HIERARCH ASTRA PREVIEW_DOWNSAMPLE"] = stride
    data = np.asarray(downsampled, dtype=np.float32)
    primary = fits.PrimaryHDU(data=data, header=header)
    return fits.HDUList([primary])


def _has_allowed_extension(name: str) -> bool:
    return ALLOWED_EXTENSIONS is None or os.path.splitext(name)[1] in ALLOWED_EXTENSIONS


def _resolve_dir(fits_dir: Path, path: str) -> Optional[Path]:
    """Resolve ``path`` relative to ``fits_dir``; None if invalid or outside it."""
    current_path = (fits_dir / path).resolve()
    # Ensure the resolved path is within the configured fits_dir. This prevents
    # directory traversal or symlink escapes.
    try:
        current_path.relative_to(fits_dir)
    except Exception:
        return None
    if not current_path.is_dir():
        return None
    return current_path


def _list_files_for_path(fits_dir: Path, path: str = ""):
    current_path = _resolve_dir(fits_dir, path)
    logger.info(f"Listing files in {current_path}")
    if current_path is None:
        return None, JSONResponse(content={"error": "Invalid path"}, status_code=400)

    def should_include(entry: os.DirEntry):
        # Exclude hidden files/dirs
        if entry.name.startswith("."):
            return False
        # Include directories (we avoid recursive scans here for performance).
        if entry.is_dir():
            return True
        # For files, only include allowed extensions when configured.
        return _has_allowed_extension(entry.name)

    items = []
    with os.scandir(current_path) as entries:
        iterator = (entry for entry in entries if should_include(entry))
        for idx, item in enumerate(iterator):
            if idx >= LIST_MAX_ITEMS:
                logger.warning(
                    "Directory listing for %s exceeded LIST_MAX_ITEMS (%s)",
                    current_path,
                    LIST_MAX_ITEMS,
                )
                return None, JSONResponse(
                    content={
                        "error": "Too many items in directory; please narrow your path"
                    },
                    status_code=413,
                )
            items.append(item)

    return items, None


def _collect_zip_members(fits_dir: Path, top: Path) -> list[tuple[Path, str]]:
    """Return (file, name in archive) pairs for every visible file under ``top``.

    Uses the same rules as the directory listing: hidden entries are skipped and
    only allowed extensions are included. Directory symlinks are not followed,
    and files that resolve outside ``fits_dir`` are skipped.
    """
    members = []
    for dirpath, dirnames, filenames in os.walk(top):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for name in sorted(filenames):
            if name.startswith(".") or not _has_allowed_extension(name):
                continue
            file_path = Path(dirpath) / name
            try:
                file_path.resolve().relative_to(fits_dir)
            except (OSError, ValueError):
                continue
            arcname = file_path.relative_to(top.parent).as_posix()
            members.append((file_path, arcname))
    return members


class _ZipSink:
    """Write-only buffer that lets ``zipfile`` stream into a generator."""

    def __init__(self):
        self._buffer = bytearray()

    def write(self, data) -> int:
        self._buffer += data
        return len(data)

    def flush(self):
        pass

    def __len__(self):
        return len(self._buffer)

    def take(self) -> bytes:
        data = bytes(self._buffer)
        self._buffer.clear()
        return data


def _iter_zip(members: list[tuple[Path, str]]) -> Iterator[bytes]:
    """Yield a zip archive of ``members`` chunk by chunk.

    Memory use stays around ZIP_CHUNK_SIZE whatever the file sizes. ZIP64 is
    used where needed, so archives and files larger than 4 GB work.
    """
    sink = _ZipSink()
    with zipfile.ZipFile(sink, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path, arcname in members:
            try:
                # from_file sets the file size (for the ZIP64 decision) and mtime
                zinfo = zipfile.ZipInfo.from_file(
                    file_path, arcname, strict_timestamps=False
                )
            except OSError as exc:
                logger.warning("Skipping %s in zip download: %s", file_path, exc)
                continue
            zinfo.compress_type = zipfile.ZIP_DEFLATED
            zinfo._compresslevel = COMPRESS_LEVEL  # public name only from 3.13
            with open(file_path, "rb") as src, zf.open(zinfo, mode="w") as dest:
                while chunk := src.read(ZIP_CHUNK_SIZE):
                    dest.write(chunk)
                    if len(sink) >= ZIP_CHUNK_SIZE:
                        yield sink.take()
        # Leaving the block writes the central directory
    yield sink.take()


def _attachment_header(filename: str) -> str:
    quoted = quote(filename)
    if quoted != filename:
        return f"attachment; filename*=utf-8''{quoted}"
    return f'attachment; filename="{filename}"'


def create_app(
    fits_dir: Union[Path, Callable[[], Path]], *, enable_gzip: bool = False, **kwargs
) -> FastAPI:
    """Standalone FastAPI application that serves the file explorer.

    This is mainly for local testing (`python file_explorer.py --fits-dir=...`). The
    recommended way to embed the explorer inside a bigger FastAPI service is to call
    :func:`include_file_explorer` on your existing app.
    """

    app = FastAPI(**kwargs)
    # For the standalone app it's useful to enable HTTP gzip by default
    include_file_explorer(app, fits_dir=fits_dir, prefix="", enable_gzip=enable_gzip)
    return app


def create_router(
    fits_dir: Union[Path, Callable[[], Path]], *, fits_url: Optional[str] = "/fits"
) -> APIRouter:
    """Return a router exposing the FITS explorer endpoints for ``fits_dir``.

    The router is the single source of truth for all HTTP behaviour so it can be
    reused across multiple host applications and in tests.
    """

    router = APIRouter()

    files_base = fits_url or "/fits"
    if not files_base.startswith("/"):
        files_base = "/" + files_base
    if not files_base.endswith("/"):
        files_base = files_base + "/"

    def get_fits_dir() -> Path:
        return fits_dir() if callable(fits_dir) else fits_dir

    from fastapi import Request
    from fastapi.responses import HTMLResponse

    @router.get("/")
    def root_index(request: Request):
        """Serve the static index HTML but inject a <base> tag so relative
        asset URLs (like `static/...`) resolve correctly when the router is
        included under a prefix (for example `/fits_explorer`). This lets the
        explorer work both standalone and embedded without requiring the host
        app to mount the same static paths.
        """
        index_file = STATIC_DIR / "index.html"
        if not index_file.exists():
            return JSONResponse(content={"error": "Index not found"}, status_code=404)

        try:
            html = index_file.read_text(encoding="utf-8")
        except Exception as exc:
            logger.exception("Failed to read index.html for file explorer: %s", exc)
            return JSONResponse(content={"error": "Index read error"}, status_code=500)

        # Compute base href from the incoming request path. Ensure it ends with '/'.
        base = str(request.url.path)
        if not base.endswith("/"):
            base = base + "/"

        # If a <base> tag already exists, replace it; otherwise inject it after <head>.
        # Also inject a small inline script that sets window.__ASTRA_FITS_BASE_PATH
        # so the embedded case (fetch + innerHTML) has the correct base for JS
        # even when the browser's window.location.pathname is different.
        injected = (
            f'<base href="{base}">\n'
            f'    <script>window.__ASTRA_FITS_BASE_PATH = "{base}"; '
            f'window.__ASTRA_FITS_FILES_BASE_PATH = "{files_base}";</script>'
        )
        if "<base" in html:
            # crude replace to ensure base and script match the request path
            import re

            html = re.sub(r"<base[^>]*>", injected, html, count=1)
        else:
            html = html.replace("<head>", f"<head>\n    {injected}", 1)

        return HTMLResponse(content=html, media_type="text/html")

    # Plain `def` endpoints run in the threadpool, so slow disk I/O (e.g. a NAS)
    # does not block the event loop the rest of Astra runs on.
    @router.get("/list/")
    def list_files(path: str = ""):
        fits_dir = get_fits_dir()
        items, err = _list_files_for_path(fits_dir, path)
        if err:
            return err
        if items is None:
            items = []

        files = []
        for entry in items:
            try:
                is_dir = entry.is_dir()
                mtime = entry.stat().st_mtime
            except OSError:
                continue  # removed between listing and stat
            files.append(
                {
                    "name": entry.name,
                    "is_dir": is_dir,
                    # Forward slashes on every OS, so the UI can split on "/"
                    "path": Path(entry.path).relative_to(fits_dir).as_posix(),
                    "mtime": mtime,
                }
            )

        return {
            "files": files,
            "current_path": path,
            "breadcrumbs": path.split("/") if path else [],
        }

    @router.get("/download_zip/")
    def download_zip(path: str = ""):
        """Stream a folder (with subfolders) as a zip archive."""
        fits_dir = get_fits_dir()
        top = _resolve_dir(fits_dir, path)
        if top is None:
            raise HTTPException(status_code=400, detail="Invalid path")
        members = _collect_zip_members(fits_dir, top)
        logger.info("Zip download of %s (%d files)", top, len(members))
        return StreamingResponse(
            _iter_zip(members),
            media_type="application/zip",
            headers={"Content-Disposition": _attachment_header(f"{top.name}.zip")},
        )

    @router.get("/download/{filename:path}")
    def download(filename: str):
        logger.info("Download request for %s", filename)
        file_path = _safe_file_path(filename, get_fits_dir())

        from fastapi.responses import FileResponse

        # Get just the filename for the download
        download_filename = file_path.name

        return FileResponse(
            path=str(file_path),
            filename=download_filename,
            media_type="application/octet-stream",
        )

    @router.get("/preview/{filename:path}")
    def preview(filename: str, hdu: Optional[int] = None, max_dim: int = 512):
        logger.info(
            "Preview request for %s (hdu=%s, max_dim=%s)", filename, hdu, max_dim
        )
        file_path = _safe_file_path(filename, get_fits_dir())

        try:
            from astropy.io import fits
        except Exception as exc:
            logger.exception("Missing astropy for preview generation")
            raise HTTPException(status_code=500, detail=str(exc))

        try:
            with fits.open(file_path, memmap=False) as hdul:
                hdu_obj, selected = _select_hdu(hdul, hdu, require_data=True)
                image_arr = _extract_image_array(hdu_obj)
                downsampled, stride = _downsample_array(image_arr, max_dim=max_dim)
                preview_hdul = _build_preview_hdul(hdu_obj, downsampled, stride)

            buf = io.BytesIO()
            preview_hdul.writeto(buf, overwrite=True)
            headers = {
                "Content-Disposition": f"inline; filename=preview_{file_path.name}",
                "X-Astra-Preview-HDU": str(selected),
                "X-Astra-Preview-Stride": str(stride),
            }
            return Response(
                content=buf.getvalue(), media_type="application/fits", headers=headers
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Failed to build preview")
            raise HTTPException(status_code=500, detail=str(exc))

    @router.get("/hdu_list/{filename:path}")
    def hdu_list(filename: str):
        logger.info("HDU list request for %s", filename)
        file_path = _safe_file_path(filename, get_fits_dir())

        try:
            from astropy.io import fits
        except Exception as exc:
            logger.exception("Missing astropy for hdu list")
            raise HTTPException(status_code=500, detail=str(exc))

        try:
            items = []
            with fits.open(file_path, memmap=False) as hdul:
                for idx, hdu in enumerate(hdul):
                    # Read shape and type from the header only. Touching
                    # hdu.data here would read every HDU's data from disk.
                    shape, dtype = _hdu_shape_and_dtype(hdu.header)
                    name = getattr(hdu, "name", "") or hdu.header.get("EXTNAME", "")
                    header_preview = {}
                    try:
                        for card in list(hdu.header.cards)[:5]:
                            if card.keyword and card.keyword.strip():
                                header_preview[str(card.keyword)] = {
                                    "value": ""
                                    if card.value is None
                                    else str(card.value),
                                    "comment": (card.comment or ""),
                                }
                    except Exception:
                        header_preview = {}

                    items.append(
                        {
                            "index": idx,
                            "name": name,
                            "has_data": bool(shape),
                            "dtype": dtype,
                            "shape": shape,
                            "naxis": hdu.header.get("NAXIS", 0),
                            "header_preview": header_preview,
                        }
                    )

            return {"items": items}
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Failed to enumerate HDUs")
            raise HTTPException(status_code=500, detail=str(exc))

    @router.get("/header/{filename:path}")
    def header(filename: str, hdu: Optional[int] = None):
        logger.info(
            f"Header request received for filename param: {filename!r}, hdu={hdu}"
        )
        file_path = _safe_file_path(filename, get_fits_dir())

        try:
            from astropy.io import fits
        except Exception as e:
            logger.exception("Missing astropy for header extraction")
            raise HTTPException(status_code=500, detail=str(e))

        try:
            with fits.open(file_path, memmap=False) as hdul:
                hdu_obj, _ = _select_hdu(hdul, hdu, require_data=False)
                hdr = getattr(hdu_obj, "header", None)
                if hdr is None:
                    raise RuntimeError("No header found")

                header_dict = {}
                try:
                    for card in hdr.cards:
                        key = card.keyword
                        if key is None or str(key).strip() == "":
                            continue
                        val = card.value
                        comment = getattr(card, "comment", None) or ""
                        header_dict[str(key)] = {
                            "value": "" if val is None else str(val),
                            "comment": str(comment),
                        }
                except Exception:
                    try:
                        for k, v in hdr.items():
                            header_dict[str(k)] = {
                                "value": "" if v is None else str(v),
                                "comment": "",
                            }
                    except Exception:
                        header_dict = {}

                return header_dict
        except Exception as e:
            logger.exception("Failed to read FITS header")
            raise HTTPException(status_code=500, detail=str(e))

    return router


def include_file_explorer(
    app: FastAPI,
    fits_dir: Union[Path, Callable[[], Path]],
    *,
    prefix: str = "/fits_explorer",
    static_url: Optional[str] = None,
    fits_url: Optional[str] = "/fits",
    enable_gzip: bool = True,
):
    """Register the file explorer on an existing FastAPI app.

    Args:
        app: Host FastAPI application.
        fits_dir: Directory containing FITS files to expose.
        prefix: URL prefix for the explorer routes (defaults to ``/fits_explorer``).
        static_url: URL path to mount the explorer's static assets. By default this
            is derived from ``prefix`` (``{prefix}/static``) or ``/static`` for the
            standalone case (``prefix=""``).
        fits_url: URL path under which raw FITS files are exposed. Defaults to
            ``/fits``; set to ``None`` to skip mounting.
    """

    if prefix and not prefix.startswith("/"):
        raise ValueError("prefix must start with '/' or be an empty string")

    if static_url is None:
        base = prefix.rstrip("/")
        static_url = f"{base}/static" if base else "/static"

    mount_name_suffix = (prefix.strip("/") or "root").replace("/", "-")

    if static_url:
        if not static_url.startswith("/"):
            raise ValueError("static_url must start with '/' if provided")
        app.mount(
            static_url,
            StaticFiles(directory=str(STATIC_DIR), html=False),
            name=f"file_explorer_static_{mount_name_suffix}",
        )

    if fits_url:
        if not fits_url.startswith("/"):
            raise ValueError("fits_url must start with '/' if provided")

        if callable(fits_dir):
            app.mount(
                fits_url,
                LazyStaticFiles(directory_loader=fits_dir, html=False),
                name=f"file_explorer_fits_{mount_name_suffix}",
            )
        else:
            app.mount(
                fits_url,
                StaticFiles(directory=str(fits_dir), html=False),
                name=f"file_explorer_fits_{mount_name_suffix}",
            )

    if enable_gzip:
        try:
            # GZip transport compression of HTTP response body. Note this applies
            # to every route of the host app, not only the explorer.
            app.add_middleware(
                GZipMiddleware, minimum_size=500, compresslevel=COMPRESS_LEVEL
            )
        except Exception:
            # If middleware cannot be added for any reason, don't fail the
            # include step; log and continue.
            logger.exception("Failed to add GZipMiddleware")

    app.include_router(create_router(fits_dir, fits_url=fits_url), prefix=prefix)


def parse_arguments():
    import argparse

    parser = argparse.ArgumentParser(description="Run ASTRA file explorer")
    parser.add_argument("--fits-dir", help="Directory to serve as FITS root")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8001, type=int)

    return parser.parse_args()


if __name__ == "__main__":
    import uvicorn

    args = parse_arguments()
    uvicorn.run(create_app(Path(args.fits_dir)), host=args.host, port=args.port)
