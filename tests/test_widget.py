"""Tests for the path-opening widget command."""

from __future__ import annotations

import inspect
import sys
from types import ModuleType

import pytest

from napari_bfs_nd2.widget import PLUGIN_NAME, make_remote_image_widget, open_image_path


class FakeViewer:
    def __init__(self):
        self.calls = []

    def open(self, path, *, plugin):
        self.calls.append((path, plugin))
        return ["layer"]


def test_widget_factory_accepts_no_arguments_for_napari_08():
    assert not inspect.signature(make_remote_image_widget).parameters


@pytest.mark.parametrize(
    "path",
    [
        "s3://bucket/image.nd2",
        "https://example.org/image.czi?X-Amz-Signature=secret",
        "/data/image.ND2",
    ],
)
def test_open_image_path_uses_plugin_reader(path):
    viewer = FakeViewer()

    result = open_image_path(viewer, f"  {path}  ")

    assert result == ["layer"]
    assert viewer.calls == [(path, PLUGIN_NAME)]


@pytest.mark.parametrize("path", ["", "   ", "s3://bucket/image.tif"])
def test_open_image_path_rejects_invalid_path(path):
    viewer = FakeViewer()

    with pytest.raises(ValueError):
        open_image_path(viewer, path)

    assert viewer.calls == []


def test_widget_button_opens_the_pasted_path(monkeypatch):
    class Signal:
        def connect(self, callback):
            self.callback = callback

        def emit(self):
            self.callback()

    class Container:
        def __init__(self, *, widgets):
            self.widgets = widgets

    class Label:
        def __init__(self, *, value):
            self.value = value

    class LineEdit:
        def __init__(self, *, label, tooltip):
            self.label = label
            self.tooltip = tooltip
            self.value = ""

    class PushButton:
        def __init__(self, *, text):
            self.text = text
            self.changed = Signal()

    fake_widgets = ModuleType("magicgui.widgets")
    fake_widgets.Container = Container
    fake_widgets.Label = Label
    fake_widgets.LineEdit = LineEdit
    fake_widgets.PushButton = PushButton
    fake_magicgui = ModuleType("magicgui")
    fake_magicgui.widgets = fake_widgets
    viewer = FakeViewer()
    fake_napari = ModuleType("napari")
    fake_napari.current_viewer = lambda: viewer
    monkeypatch.setitem(sys.modules, "napari", fake_napari)
    monkeypatch.setitem(sys.modules, "magicgui", fake_magicgui)
    monkeypatch.setitem(sys.modules, "magicgui.widgets", fake_widgets)

    widget = make_remote_image_widget()
    path_entry, open_button, status = widget.widgets
    path_entry.value = "s3://bucket/image.czi"

    open_button.changed.emit()

    assert viewer.calls == [("s3://bucket/image.czi", PLUGIN_NAME)]
    assert status.value == "Opened 1 layer."


def test_widget_requires_an_active_viewer(monkeypatch):
    fake_napari = ModuleType("napari")
    fake_napari.current_viewer = lambda: None
    monkeypatch.setitem(sys.modules, "napari", fake_napari)

    with pytest.raises(RuntimeError, match="active napari viewer"):
        make_remote_image_widget()
