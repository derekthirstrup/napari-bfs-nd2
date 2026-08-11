"""Tests for the standalone napari ND2/CZI reader."""

from __future__ import annotations

import sys
from types import ModuleType

import numpy as np
import pytest

from napari_bfs_nd2 import reader as ndr


@pytest.mark.parametrize(
    "path",
    [
        "image.nd2",
        "IMAGE.ND2",
        "s3://bucket/folder/image.nd2",
        "s3://bucket/folder/image.czi",
        "https://example.org/image.czi?X-Amz-Signature=secret",
        "https://example.org/image.nd2?X-Amz-Signature=secret",
        ["one.nd2", "two.nd2"],
        ["one.nd2", "two.czi"],
    ],
)
def test_napari_get_reader_supported_path_returns_reader(path):
    assert ndr.napari_get_reader(path) is ndr.napari_read


@pytest.mark.parametrize("path", ["image.tif", "s3://bucket/image.zarr", [], ["one.nd2", "two.tif"]])
def test_napari_get_reader_unsupported_path_returns_none(path):
    assert ndr.napari_get_reader(path) is None


def test_read_nd2_block_uses_scene_time_and_z(monkeypatch):
    calls = []

    class FakeReader:
        height = 2
        width = 3

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read_zstack(self, **kwargs):
            calls.append((self.path, kwargs))
            return np.full((1, 2, 3), 17, dtype=np.uint16)

    monkeypatch.setattr(ndr, "RemoteND2Reader", FakeReader)
    result = ndr._read_nd2_block(
        np.empty((1, 1, 1), dtype=np.uint8),
        path="s3://bucket/image.nd2",
        channel=2,
        block_id=(4, 5, 6, 0, 0),
    )

    assert result.shape == (1, 1, 1, 2, 3)
    assert calls == [
        (
            "s3://bucket/image.nd2",
            {"t": 5, "c": 2, "s": 4, "max_workers": 1, "crop": (6, 7, 0, 2, 0, 3)},
        )
    ]


def test_napari_read_nd2_builds_lazy_scene_aware_channel_layers(monkeypatch):
    map_calls = []

    class FakeIndex:
        def map_blocks(self, function, **kwargs):
            map_calls.append((function, kwargs))
            return f"lazy-channel-{kwargs['channel']}"

    fake_dask_array = ModuleType("dask.array")
    fake_dask_array.empty = lambda *args, **kwargs: FakeIndex()
    fake_dask = ModuleType("dask")
    fake_dask.array = fake_dask_array
    monkeypatch.setitem(sys.modules, "dask", fake_dask)
    monkeypatch.setitem(sys.modules, "dask.array", fake_dask_array)

    class FakeReader:
        num_scenes = 2
        num_timepoints = 3
        num_z = 4
        num_channels = 2
        height = 5
        width = 6
        dtype = np.dtype(np.uint16)
        voxel_size_z_um = 0.5
        voxel_size_y_um = 0.16
        voxel_size_x_um = 0.16
        channels = ["TL", "EGFP"]
        scene_names = ["D9_1", "E10_1"]
        scene_positions = [(100.0, 200.0), (300.0, 400.0)]
        time_interval_ms = 60000.0

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(ndr, "RemoteND2Reader", FakeReader)
    layers = ndr.napari_read_nd2("s3://bucket/experiment.nd2")

    assert [layer[0] for layer in layers] == ["lazy-channel-0", "lazy-channel-1"]
    assert [layer[1]["name"] for layer in layers] == ["experiment TL", "experiment EGFP"]
    assert layers[0][1]["axis_labels"] == ("scene", "time", "z", "y", "x")
    assert layers[0][1]["scale"] == (1.0, 1.0, 0.5, 0.16, 0.16)
    assert layers[1][1]["colormap"] == "green"
    assert layers[0][1]["metadata"]["napari_bfs_nd2"]["scene_names"] == ("D9_1", "E10_1")
    assert all(layer[2] == "image" for layer in layers)
    assert [call[1]["channel"] for call in map_calls] == [0, 1]
    assert all(call[1]["chunks"] == (1, 1, 1, 5, 6) for call in map_calls)


def test_napari_read_nd2_rejects_unsupported_path():
    with pytest.raises(ValueError, match="only accepts ND2 files"):
        ndr.napari_read_nd2("s3://bucket/image.tif")


def test_napari_read_nd2_dask_slice_reads_only_requested_plane(monkeypatch):
    pytest.importorskip("dask.array")
    calls = []

    class FakeReader:
        num_scenes = 2
        num_timepoints = 3
        num_z = 4
        num_channels = 1
        height = 5
        width = 6
        dtype = np.dtype(np.uint16)
        voxel_size_z_um = 0.5
        voxel_size_y_um = 0.16
        voxel_size_x_um = 0.16
        channels = ["H2B"]
        scene_names = ["P0", "P1"]
        scene_positions = [(0.0, 0.0), (1.0, 1.0)]
        time_interval_ms = 1000.0

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read_zstack(self, *, t, c, s, max_workers, crop):
            calls.append((s, t, c, crop))
            value = 1000 * s + 100 * t + 10 * crop[0] + c
            return np.full((1, self.height, self.width), value, dtype=self.dtype)

    monkeypatch.setattr(ndr, "RemoteND2Reader", FakeReader)
    data = ndr.napari_read_nd2("s3://bucket/experiment.nd2")[0][0]
    plane = data[1, 2, 3].compute()

    assert data.shape == (2, 3, 4, 5, 6)
    assert plane.shape == (5, 6)
    assert np.all(plane == 1230)
    assert calls == [(1, 2, 0, (3, 4, 0, 5, 0, 6))]


def test_read_czi_block_uses_scene_time_z_and_pads(monkeypatch):
    calls = []

    class FakeReader:
        dtype = np.dtype(np.uint16)

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def scene_shape(self, scene):
            assert scene == 4
            return 2, 3

        def read_zstack(self, **kwargs):
            calls.append((self.path, kwargs))
            return np.full((1, 2, 3), 23, dtype=self.dtype)

    monkeypatch.setattr(ndr, "CZIReader", FakeReader)
    result = ndr._read_czi_block(
        np.empty((1, 1, 1), dtype=np.uint8),
        path="s3://bucket/image.czi",
        channel=2,
        output_height=4,
        output_width=5,
        block_id=(4, 5, 6, 0, 0),
    )

    assert result.shape == (1, 1, 1, 4, 5)
    assert np.all(result[0, 0, 0, :2, :3] == 23)
    assert np.all(result[0, 0, 0, 2:, :] == 0)
    assert calls == [
        (
            "s3://bucket/image.czi",
            {"t": 5, "c": 2, "s": 4, "max_workers": 1, "crop": (6, 7, 0, 2, 0, 3)},
        )
    ]


def test_napari_read_czi_builds_lazy_scene_aware_channel_layers(monkeypatch):
    map_calls = []

    class FakeIndex:
        def map_blocks(self, function, **kwargs):
            map_calls.append((function, kwargs))
            return f"lazy-czi-{kwargs['channel']}"

    fake_dask_array = ModuleType("dask.array")
    fake_dask_array.empty = lambda *args, **kwargs: FakeIndex()
    fake_dask = ModuleType("dask")
    fake_dask.array = fake_dask_array
    monkeypatch.setitem(sys.modules, "dask", fake_dask)
    monkeypatch.setitem(sys.modules, "dask.array", fake_dask_array)

    class FakeReader:
        num_scenes = 2
        num_timepoints = 3
        num_z = 4
        num_channels = 2
        dtype = np.dtype(np.uint16)
        pixel_size_z = 0.5
        pixel_size_y = 0.17
        pixel_size_xy = 0.16

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def scene_shape(self, scene):
            return [(5, 6), (4, 8)][scene]

        def get_xml_metadata(self):
            return '<Metadata><Channels><Channel Name="TL"/><Channel Name="EGFP"/></Channels></Metadata>'

    monkeypatch.setattr(ndr, "CZIReader", FakeReader)
    layers = ndr.napari_read_czi("s3://bucket/experiment.czi")

    assert [layer[0] for layer in layers] == ["lazy-czi-0", "lazy-czi-1"]
    assert [layer[1]["name"] for layer in layers] == ["experiment TL", "experiment EGFP"]
    assert layers[0][1]["axis_labels"] == ("scene", "time", "z", "y", "x")
    assert layers[0][1]["scale"] == (1.0, 1.0, 0.5, 0.17, 0.16)
    assert layers[1][1]["colormap"] == "green"
    metadata = layers[0][1]["metadata"]["napari_bfs_nd2"]
    assert metadata["format"] == "czi"
    assert metadata["scene_shapes_yx"] == ((5, 6), (4, 8))
    assert [call[1]["channel"] for call in map_calls] == [0, 1]
    assert all(call[1]["chunks"] == (1, 1, 1, 5, 8) for call in map_calls)


def test_napari_read_czi_dask_slice_reads_only_requested_plane(monkeypatch):
    pytest.importorskip("dask.array")
    calls = []

    class FakeReader:
        num_scenes = 2
        num_timepoints = 3
        num_z = 4
        num_channels = 1
        dtype = np.dtype(np.uint16)
        pixel_size_z = 0.5
        pixel_size_y = 0.16
        pixel_size_xy = 0.16

        def __init__(self, path):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def scene_shape(self, scene):
            return 5, 6

        def get_xml_metadata(self):
            return '<Metadata><Channel Name="H2B"/></Metadata>'

        def read_zstack(self, *, t, c, s, max_workers, crop):
            calls.append((s, t, c, crop))
            value = 1000 * s + 100 * t + 10 * crop[0] + c
            return np.full((1, 5, 6), value, dtype=self.dtype)

    monkeypatch.setattr(ndr, "CZIReader", FakeReader)
    data = ndr.napari_read_czi("s3://bucket/experiment.czi")[0][0]
    plane = data[1, 2, 3].compute()

    assert data.shape == (2, 3, 4, 5, 6)
    assert np.all(plane == 1230)
    assert calls == [(1, 2, 0, (3, 4, 0, 5, 0, 6))]
