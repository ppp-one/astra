import io
import zipfile

import numpy as np
import pytest
from astropy.io import fits
from fastapi.testclient import TestClient
from starlette.middleware.gzip import GZipMiddleware

from astra.frontend.file_explorer import file_explorer
from astra.frontend.file_explorer.file_explorer import (
    COMPRESS_LEVEL,
    _downsample_array,
    _iter_zip,
    create_app,
)


@pytest.fixture
def fits_root(tmp_path):
    root = tmp_path / "images"
    night = root / "night1"
    (night / "sub").mkdir(parents=True)
    (night / ".hidden").mkdir()

    image = np.arange(64 * 48, dtype=np.uint16).reshape(48, 64)
    fits.PrimaryHDU(image).writeto(night / "a.fits")
    fits.PrimaryHDU(image[:8, :8]).writeto(night / "sub" / "b.fits")
    (night / "notes.txt").write_text("not exposed")
    (night / ".secret.fits").write_bytes(b"hidden")
    (night / ".hidden" / "c.fits").write_bytes(b"hidden")
    return root


@pytest.fixture
def client(fits_root):
    return TestClient(create_app(fits_root.resolve()))


def test_list_returns_visible_entries_with_posix_paths(client):
    response = client.get("/list/", params={"path": "night1"})
    assert response.status_code == 200
    files = {f["name"]: f for f in response.json()["files"]}
    assert set(files) == {"a.fits", "sub"}
    assert files["a.fits"]["path"] == "night1/a.fits"
    assert files["a.fits"]["is_dir"] is False
    assert files["sub"]["is_dir"] is True
    assert files["a.fits"]["mtime"] > 0


def test_list_root_and_traversal(client):
    root = client.get("/list/").json()["files"]
    assert [f["path"] for f in root] == ["night1"]
    assert client.get("/list/", params={"path": "../"}).status_code == 400


def test_downsample_averages_blocks():
    arr = np.arange(16, dtype=np.uint16).reshape(4, 4)
    out, stride = _downsample_array(arr, max_dim=2)
    assert stride == 2
    assert out.dtype == np.float32
    # Means of the 2x2 blocks, rounded for integer input
    np.testing.assert_array_equal(out, [[2, 4], [10, 12]])


def test_downsample_drops_partial_edge_blocks():
    arr = np.ones((5, 9), dtype=np.float32)
    out, stride = _downsample_array(arr, max_dim=4)
    assert stride == 3
    assert out.shape == (1, 3)


def test_downsample_keeps_single_row_images():
    arr = np.ones((1, 100), dtype=np.float32)
    out, stride = _downsample_array(arr, max_dim=10)
    assert stride == 10
    assert out.shape == (1, 10)


def test_preview_is_block_mean(client, fits_root):
    response = client.get("/preview/night1/a.fits", params={"max_dim": 16})
    assert response.status_code == 200
    assert response.headers["X-Astra-Preview-Stride"] == "4"
    with fits.open(io.BytesIO(response.content)) as hdul:
        preview = hdul[0].data
    source = fits.getdata(fits_root / "night1" / "a.fits").astype(np.float64)
    expected = np.rint(source.reshape(12, 4, 16, 4).mean(axis=(1, 3)))
    np.testing.assert_array_equal(preview, expected)


def test_hdu_list_reads_shapes_from_headers(client, fits_root):
    path = fits_root / "night1" / "mef.fits"
    fits.HDUList(
        [
            fits.PrimaryHDU(),
            fits.ImageHDU(np.zeros((3, 4), np.uint16), name="SCI"),
            fits.BinTableHDU.from_columns(
                [fits.Column(name="x", format="J", array=np.arange(5))], name="TAB"
            ),
        ]
    ).writeto(path)

    items = client.get("/hdu_list/night1/mef.fits").json()["items"]
    assert [(i["name"], i["shape"], i["has_data"]) for i in items] == [
        ("PRIMARY", [], False),
        ("SCI", [3, 4], True),
        ("TAB", [5], True),
    ]
    assert items[1]["dtype"] == "uint16"


def test_download_zip_contains_visible_tree(client, fits_root):
    response = client.get("/download_zip/", params={"path": "night1"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert 'filename="night1.zip"' in response.headers["content-disposition"]

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        assert zf.testzip() is None
        assert sorted(zf.namelist()) == ["night1/a.fits", "night1/sub/b.fits"]
        original = (fits_root / "night1" / "a.fits").read_bytes()
        assert zf.read("night1/a.fits") == original
        assert zf.getinfo("night1/a.fits").compress_type == zipfile.ZIP_DEFLATED


def test_download_zip_rejects_bad_paths(client):
    assert client.get("/download_zip/", params={"path": "../"}).status_code == 400
    assert (
        client.get("/download_zip/", params={"path": "night1/a.fits"}).status_code
        == 400
    )
    assert client.get("/download_zip/", params={"path": "nope"}).status_code == 400


def test_iter_zip_streams_in_bounded_chunks(tmp_path, monkeypatch):
    chunk_size = 16 * 1024
    monkeypatch.setattr(file_explorer, "ZIP_CHUNK_SIZE", chunk_size)
    data = np.random.default_rng(0).bytes(1_000_000)  # incompressible
    path = tmp_path / "big.fits"
    path.write_bytes(data)

    chunks = list(_iter_zip([(path, "big.fits")]))
    assert len(chunks) > 10
    assert max(len(c) for c in chunks) < 3 * chunk_size
    with zipfile.ZipFile(io.BytesIO(b"".join(chunks))) as zf:
        assert zf.read("big.fits") == data


def test_gzip_uses_fast_compression(fits_root):
    app = create_app(fits_root.resolve(), enable_gzip=True)
    gzip = [m for m in app.user_middleware if m.cls is GZipMiddleware]
    assert len(gzip) == 1
    assert gzip[0].kwargs["compresslevel"] == COMPRESS_LEVEL == 1
