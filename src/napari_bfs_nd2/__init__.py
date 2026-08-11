"""Standalone napari reader for local and remote ND2/CZI files."""

from .reader import napari_get_reader, napari_read, napari_read_czi, napari_read_nd2

__all__ = ["napari_get_reader", "napari_read", "napari_read_czi", "napari_read_nd2"]
__version__ = "0.3.0"
