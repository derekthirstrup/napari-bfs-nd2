"""npe2 reader for plane-lazy local and remote ND2/CZI images."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from typing import Any
from xml.etree import ElementTree

import numpy as np

from .remote_czi import CZIReader
from .remote_nd2 import RemoteND2Reader

PathLike = str | os.PathLike[str]
PathOrPaths = PathLike | Sequence[PathLike]
LayerData = tuple[Any, ...]
ReaderFunction = Callable[[PathOrPaths], list[LayerData]]
_SUPPORTED_EXTENSIONS = {".nd2", ".czi"}


def _clean_path(path: PathLike) -> str:
    """Strip query/fragment data while retaining the displayable source path."""
    return str(path).split("?", 1)[0].split("#", 1)[0]


def _file_extension(path: PathLike) -> str:
    clean_path = _clean_path(path).lower()
    return next((extension for extension in _SUPPORTED_EXTENSIONS if clean_path.endswith(extension)), "")


def _is_supported_path(path: PathLike) -> bool:
    return bool(_file_extension(path))


def _image_name(path: str) -> str:
    clean_path = _clean_path(path).rstrip("/\\")
    file_name = clean_path.replace("\\", "/").rsplit("/", 1)[-1]
    extension = _file_extension(file_name)
    return file_name[: -len(extension)] if extension else file_name


def _channel_colormap(channel: int) -> str:
    return "green" if channel == 1 else "gray"


def napari_get_reader(path: PathOrPaths) -> ReaderFunction | None:
    """Return this plugin's reader when every supplied path is ND2 or CZI."""
    paths = (path,) if isinstance(path, (str, os.PathLike)) else path
    try:
        if paths and all(_is_supported_path(item) for item in paths):
            return napari_read
    except TypeError:
        pass
    return None


def _read_nd2_block(
    _block: np.ndarray,
    *,
    path: str,
    channel: int,
    block_id: tuple[int, ...],
) -> np.ndarray:
    """Read one ND2 scene/time/Z plane for a Dask block."""
    scene, timepoint, z = block_id[:3]
    with RemoteND2Reader(path) as reader:
        crop = (z, z + 1, 0, reader.height, 0, reader.width)
        stack = reader.read_zstack(
            t=timepoint,
            c=channel,
            s=scene,
            max_workers=1,
            crop=crop,
        )
    return stack[np.newaxis, np.newaxis]


def _read_czi_block(
    _block: np.ndarray,
    *,
    path: str,
    channel: int,
    output_height: int,
    output_width: int,
    block_id: tuple[int, ...],
) -> np.ndarray:
    """Read one CZI scene/time/Z plane and pad varying scene sizes."""
    scene, timepoint, z = block_id[:3]
    with CZIReader(path) as reader:
        height, width = reader.scene_shape(scene)
        crop = (z, z + 1, 0, height, 0, width)
        stack = reader.read_zstack(
            t=timepoint,
            c=channel,
            s=scene,
            max_workers=1,
            crop=crop,
        )

    if height != output_height or width != output_width:
        padded = np.zeros((1, output_height, output_width), dtype=stack.dtype)
        padded[:, :height, :width] = stack
        stack = padded
    return stack[np.newaxis, np.newaxis]


def _czi_channel_names(xml: str, count: int) -> tuple[str, ...]:
    """Extract Zeiss channel names from XML, falling back to stable labels."""
    names: list[str] = []
    try:
        root = ElementTree.fromstring(xml)
        for element in root.iter():
            if element.tag.rsplit("}", 1)[-1] != "Channel":
                continue
            name = element.get("Name") or element.get("Id")
            if name and name not in names:
                names.append(name)
            if len(names) == count:
                break
    except ElementTree.ParseError:
        pass
    names.extend(f"Channel {channel}" for channel in range(len(names), count))
    return tuple(names[:count])


def _read_nd2_layers(path: str) -> list[LayerData]:
    import dask.array as da

    layers: list[LayerData] = []
    with RemoteND2Reader(path) as reader:
        index = da.empty(
            (reader.num_scenes, reader.num_timepoints, reader.num_z),
            chunks=(1, 1, 1),
            dtype=np.uint8,
        )
        scale = (
            1.0,
            1.0,
            reader.voxel_size_z_um or 1.0,
            reader.voxel_size_y_um or 1.0,
            reader.voxel_size_x_um or 1.0,
        )
        scene_names = (
            tuple(reader.scene_names)
            if len(reader.scene_names) == reader.num_scenes
            else tuple(f"Scene_{scene}" for scene in range(reader.num_scenes))
        )
        source = _clean_path(path)
        image_name = _image_name(path)

        for channel in range(reader.num_channels):
            data = index.map_blocks(
                _read_nd2_block,
                path=path,
                channel=channel,
                dtype=reader.dtype,
                chunks=(1, 1, 1, reader.height, reader.width),
                new_axis=(3, 4),
                meta=np.empty((0, 0, 0, 0, 0), dtype=reader.dtype),
            )
            channel_name = reader.channels[channel] if channel < len(reader.channels) else f"Channel {channel}"
            name = image_name if reader.num_channels == 1 else f"{image_name} {channel_name}"
            attributes = {
                "name": name,
                "axis_labels": ("scene", "time", "z", "y", "x"),
                "scale": scale,
                "cache": True,
                "colormap": _channel_colormap(channel),
                "blending": "additive",
                "rendering": "mip",
                "metadata": {
                    "napari_bfs_nd2": {
                        "format": "nd2",
                        "source": source,
                        "channel": channel,
                        "channel_name": channel_name,
                        "scene_names": scene_names,
                        "scene_positions_um": tuple(reader.scene_positions),
                        "time_interval_ms": reader.time_interval_ms,
                    }
                },
            }
            layers.append((data, attributes, "image"))
    return layers


def _read_czi_layers(path: str) -> list[LayerData]:
    import dask.array as da

    layers: list[LayerData] = []
    with CZIReader(path) as reader:
        index = da.empty(
            (reader.num_scenes, reader.num_timepoints, reader.num_z),
            chunks=(1, 1, 1),
            dtype=np.uint8,
        )
        scene_shapes = tuple(reader.scene_shape(scene) for scene in range(reader.num_scenes))
        output_height = max(height for height, _ in scene_shapes)
        output_width = max(width for _, width in scene_shapes)
        channel_names = _czi_channel_names(reader.get_xml_metadata(), reader.num_channels)
        scale = (
            1.0,
            1.0,
            reader.pixel_size_z or 1.0,
            reader.pixel_size_y or reader.pixel_size_xy or 1.0,
            reader.pixel_size_xy or 1.0,
        )
        scene_names = tuple(f"Scene_{scene}" for scene in range(reader.num_scenes))
        source = _clean_path(path)
        image_name = _image_name(path)

        for channel in range(reader.num_channels):
            data = index.map_blocks(
                _read_czi_block,
                path=path,
                channel=channel,
                output_height=output_height,
                output_width=output_width,
                dtype=reader.dtype,
                chunks=(1, 1, 1, output_height, output_width),
                new_axis=(3, 4),
                meta=np.empty((0, 0, 0, 0, 0), dtype=reader.dtype),
            )
            channel_name = channel_names[channel]
            name = image_name if reader.num_channels == 1 else f"{image_name} {channel_name}"
            attributes = {
                "name": name,
                "axis_labels": ("scene", "time", "z", "y", "x"),
                "scale": scale,
                "cache": True,
                "colormap": _channel_colormap(channel),
                "blending": "additive",
                "rendering": "mip",
                "metadata": {
                    "napari_bfs_nd2": {
                        "format": "czi",
                        "source": source,
                        "channel": channel,
                        "channel_name": channel_name,
                        "scene_names": scene_names,
                        "scene_shapes_yx": scene_shapes,
                    }
                },
            }
            layers.append((data, attributes, "image"))
    return layers


def napari_read(path: PathOrPaths) -> list[LayerData]:
    """Return plane-lazy ND2 or CZI image layers for napari."""
    paths = (path,) if isinstance(path, (str, os.PathLike)) else tuple(path)
    if not paths:
        raise ValueError("No image path was provided.")

    layers: list[LayerData] = []
    for item in paths:
        path_str = str(item)
        extension = _file_extension(path_str)
        if extension == ".nd2":
            layers.extend(_read_nd2_layers(path_str))
        elif extension == ".czi":
            layers.extend(_read_czi_layers(path_str))
        else:
            raise ValueError(f"napari-bfs-nd2 only accepts ND2 and CZI files, got: {path_str!r}")
    return layers


def napari_read_nd2(path: PathOrPaths) -> list[LayerData]:
    """Backward-compatible ND2-only reader entry point."""
    paths = (path,) if isinstance(path, (str, os.PathLike)) else tuple(path)
    if not paths or not all(_file_extension(item) == ".nd2" for item in paths):
        raise ValueError("napari_read_nd2 only accepts ND2 files.")
    return napari_read(paths)


def napari_read_czi(path: PathOrPaths) -> list[LayerData]:
    """CZI-only reader entry point."""
    paths = (path,) if isinstance(path, (str, os.PathLike)) else tuple(path)
    if not paths or not all(_file_extension(item) == ".czi" for item in paths):
        raise ValueError("napari_read_czi only accepts CZI files.")
    return napari_read(paths)
