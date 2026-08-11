"""Focused tests for S3-to-HTTPS CZI resolution."""

from __future__ import annotations

import sys
import threading
from types import ModuleType

import pytest

from napari_bfs_nd2 import remote_czi


def test_s3_czi_path_is_signed_and_cached(monkeypatch):
    calls = []

    class FakeS3FileSystem:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def sign(self, path, *, expiration):
            calls.append(("sign", path, expiration))
            return "https://bucket.s3.amazonaws.com/image.czi?signature=secret"

    fake_s3fs = ModuleType("s3fs")
    fake_s3fs.S3FileSystem = FakeS3FileSystem
    monkeypatch.setitem(sys.modules, "s3fs", fake_s3fs)
    monkeypatch.setattr(remote_czi, "_s3_storage_options", lambda: {"profile": "microscopy"})
    monkeypatch.setattr(remote_czi.time, "monotonic", lambda: 100.0)
    remote_czi._s3_url_cache.clear()

    first = remote_czi._resolve_czi_path("s3://bucket/image.czi")
    second = remote_czi._resolve_czi_path("s3://bucket/image.czi")

    assert first == second
    assert calls == [
        ("init", {"profile": "microscopy"}),
        ("sign", "s3://bucket/image.czi", remote_czi._S3_URL_EXPIRATION_SECONDS),
    ]


def test_non_s3_czi_path_is_unchanged():
    path = "https://example.org/image.czi?X-Amz-Signature=secret"
    assert remote_czi._resolve_czi_path(path) == path


def test_s3_open_error_does_not_expose_signed_url(monkeypatch):
    signed_url = "https://bucket.s3.amazonaws.com/image.czi?X-Amz-Signature=secret"
    monkeypatch.setattr(
        remote_czi._pylibCZIrw,
        "czi_reader",
        lambda *_args: (_ for _ in ()).throw(RuntimeError(f"failed: {signed_url}")),
    )

    reader = remote_czi.CZIReader.__new__(remote_czi.CZIReader)
    reader.source_path = "s3://bucket/image.czi"
    reader.path = signed_url
    reader._cache_size_mb = 64
    reader._is_remote = True
    reader._pool_lock = threading.Lock()
    reader._pool_readers = []
    reader._reader = None

    with pytest.raises(RuntimeError, match="s3://bucket/image.czi") as excinfo:
        reader._open_reader()

    assert "X-Amz-Signature" not in str(excinfo.value)
