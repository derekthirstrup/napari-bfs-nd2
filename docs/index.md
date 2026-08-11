# Remote ND2 and CZI in napari

`napari-bfs-nd2` opens Nikon ND2 and Zeiss CZI microscopy datasets directly
from local storage, HTTP(S), or Amazon S3. Large files are represented as lazy
Dask arrays, so napari fetches image planes as they are viewed instead of
downloading the complete dataset first.

## Quick start

Install the plugin in napari's environment:

```bash
python -m pip install napari-bfs-nd2
```

Start napari, choose **Plugins > BFS Remote ND2/CZI Reader > Open ND2/CZI
path**, paste a path, and choose **Open image**.

```text
s3://my-bucket/experiment/acquisition.nd2
```

The same reader works through Python:

```python
import napari

viewer = napari.Viewer()
viewer.open("s3://my-bucket/experiment/acquisition.czi")
napari.run()
```

## What the plugin contributes

| Contribution | Purpose |
| --- | --- |
| `npe2` reader | Claims `.nd2` and `.czi` paths for File Open, drag-and-drop, and `viewer.open()` |
| Dock widget | Provides a pasteable local/S3/HTTP path field and Open button |
| Lazy image layers | Creates one `(scene, time, z, y, x)` Dask array per channel |

## Choose a guide

- [Installation](installation.md): plugin manager, pip, wheels, updates, and removal.
- [Using the plugin](usage.md): widget, File Open, Python, and low-level readers.
- [S3 authentication](s3-authentication.md): profiles, environment credentials, roles, and public data.
- [Data model and performance](data-model.md): dimensions, metadata, chunking, and format-specific behavior.
- [Troubleshooting](troubleshooting.md): plugin discovery, credentials, range requests, and dependency errors.
- [Development](development.md): editable installs, tests, manifest validation, and documentation.

!!! note "Standalone package"

    The remote reader code is adapted from BFS2, but `napari-bfs-nd2` is an
    independent package and does not install or import BFS2.
