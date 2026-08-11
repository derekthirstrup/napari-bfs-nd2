"""Focused tests for standalone fsspec integration."""

from __future__ import annotations

import sys
from types import ModuleType

from napari_bfs_nd2 import remote_nd2


def test_lazy_s3_open_uses_storage_options(monkeypatch):
    calls = []
    sentinel_file = object()

    class OpenFile:
        def open(self):
            return sentinel_file

    def fake_open(url, **kwargs):
        calls.append((url, kwargs))
        return OpenFile()

    fake_fsspec = ModuleType("fsspec")
    fake_fsspec.open = fake_open
    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)
    monkeypatch.setattr(remote_nd2, "_s3_storage_options", lambda: {"profile": "microscopy"})

    reader = remote_nd2.RemoteND2Reader.__new__(remote_nd2.RemoteND2Reader)
    reader._file = None
    reader._is_local = False
    reader.url = "s3://bucket/acquisition.nd2"
    reader._block_size = 16 * 1024**2
    reader._ensure_file_open()

    assert reader._file._file is sentinel_file
    assert calls == [
        (
            "s3://bucket/acquisition.nd2",
            {"mode": "rb", "block_size": 16 * 1024**2, "profile": "microscopy"},
        )
    ]

