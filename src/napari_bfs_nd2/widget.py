"""napari dock widget for opening local and remote microscopy images."""

from typing import Any

from .reader import _is_supported_path

PLUGIN_NAME = "napari-bfs-nd2"


def open_image_path(viewer: Any, image_path: str) -> Any:
    """Open an ND2/CZI path through this plugin's npe2 reader."""
    normalized_path = image_path.strip()
    if not normalized_path:
        raise ValueError("Enter a local, S3, or HTTP(S) ND2/CZI path.")
    if not _is_supported_path(normalized_path):
        raise ValueError("The image path must end in .nd2 or .czi.")
    return viewer.open(normalized_path, plugin=PLUGIN_NAME)


def make_remote_image_widget() -> Any:
    """Create a dock widget with a pasteable microscopy image path."""
    import napari
    from magicgui.widgets import Container, Label, LineEdit, PushButton

    viewer = napari.current_viewer()
    if viewer is None:
        raise RuntimeError("Open the widget from an active napari viewer.")

    path_entry = LineEdit(
        label="Image path or URI",
        tooltip="Paste a local path, s3:// URI, HTTP URL, or presigned HTTPS URL.",
    )
    open_button = PushButton(text="Open image")
    status = Label(value="Paste an .nd2 or .czi path, then choose Open image.")

    def _open_image() -> None:
        try:
            layers = open_image_path(viewer, path_entry.value)
        except Exception as error:  # napari should stay usable after a failed remote open
            status.value = f"Error: {error}"
            return

        layer_count = len(layers) if layers is not None else 0
        noun = "layer" if layer_count == 1 else "layers"
        status.value = f"Opened {layer_count} {noun}."

    open_button.changed.connect(_open_image)
    return Container(widgets=[path_entry, open_button, status])
