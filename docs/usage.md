# Using the plugin

## Path widget

1. Start napari.
2. Choose **Plugins > BFS Remote ND2/CZI Reader > Open ND2/CZI path**.
3. Paste one local, S3, HTTP, or presigned HTTPS path.
4. Choose **Open image**.

Examples:

```text
/data/experiment/acquisition.nd2
C:\microscopy\experiment\acquisition.czi
s3://my-bucket/experiment/acquisition.nd2
https://data.example.org/experiment/acquisition.czi
```

The widget explicitly selects `napari-bfs-nd2`, which avoids a reader-selection
dialog when another plugin also supports ND2 or CZI.

## File Open and drag-and-drop

The plugin registers filename patterns for `.nd2` and `.czi`. The same reader is
therefore considered when you:

- choose **File > Open Files**;
- drag a local file into the napari canvas; or
- call `viewer.open()`.

Remote URI entry is most reliable through the widget or Python console because
native file dialogs generally expect a local filesystem path.

## napari Python console

Automatic reader selection:

```python
viewer.open("s3://my-bucket/experiment/acquisition.nd2")
```

Explicit reader selection:

```python
viewer.open(
    "s3://my-bucket/experiment/acquisition.czi",
    plugin="napari-bfs-nd2",
)
```

Open several acquisitions:

```python
paths = [
    "s3://my-bucket/experiment/field-001.nd2",
    "s3://my-bucket/experiment/field-002.nd2",
]

for path in paths:
    viewer.open(path, plugin="napari-bfs-nd2")
```

## Standalone Python script

```python
import napari


def main() -> None:
    viewer = napari.Viewer()
    viewer.open(
        "s3://my-bucket/experiment/acquisition.nd2",
        plugin="napari-bfs-nd2",
    )
    napari.run()


if __name__ == "__main__":
    main()
```

## Reader function without a viewer

The `npe2` reader can be called directly when another workflow needs napari
LayerData tuples:

```python
from napari_bfs_nd2 import napari_read

layer_data = napari_read("s3://my-bucket/experiment/acquisition.nd2")
data, attributes, layer_type = layer_data[0]

print(data.shape)                  # lazy Dask array
print(attributes["axis_labels"])
print(attributes["scale"])
print(layer_type)                  # "image"
```

Computing a single plane only requests that plane:

```python
plane = data[0, 0, 0].compute()
```

## Low-level Z-stack extraction

For workflows that need a NumPy Z-stack rather than napari layers:

```python
from napari_bfs_nd2.remote_nd2 import RemoteND2Reader

with RemoteND2Reader("s3://my-bucket/acquisition.nd2") as reader:
    stack = reader.read_zstack(t=0, c=0, s=0, max_workers=16)

print(stack.shape)  # (z, y, x)
```

CZI uses the same high-level extraction shape:

```python
from napari_bfs_nd2.remote_czi import CZIReader

with CZIReader("s3://my-bucket/acquisition.czi") as reader:
    stack = reader.read_zstack(t=0, c=0, s=0)
```

An optional crop is `(z_start, z_end, y_start, y_end, x_start, x_end)` with
exclusive end indices:

```python
crop = (10, 30, 200, 712, 300, 812)

with RemoteND2Reader("s3://my-bucket/acquisition.nd2") as reader:
    stack = reader.read_zstack(t=0, c=0, s=0, crop=crop)
```

## Presigned URLs

Presigned HTTP(S) URLs can be passed directly. Do not log, publish, or place
them in screenshots: their query strings grant time-limited access to the
object.
