# napari-bfs-nd2

[![Tests](https://github.com/derekthirstrup/napari-bfs-nd2/actions/workflows/test.yml/badge.svg)](https://github.com/derekthirstrup/napari-bfs-nd2/actions/workflows/test.yml)
[![Documentation](https://github.com/derekthirstrup/napari-bfs-nd2/actions/workflows/docs.yml/badge.svg)](https://derekthirstrup.github.io/napari-bfs-nd2/)
[![PyPI](https://img.shields.io/pypi/v/napari-bfs-nd2.svg)](https://pypi.org/project/napari-bfs-nd2/)
[![Python](https://img.shields.io/pypi/pyversions/napari-bfs-nd2.svg)](https://pypi.org/project/napari-bfs-nd2/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

A standalone [napari](https://napari.org/) plugin for plane-lazy Nikon ND2 and
Zeiss CZI images stored locally, over HTTP(S), or in Amazon S3.

The plugin adapts the high-performance remote microscopy readers from
[BFS2](https://github.com/derekthirstrup/BFS), but BFS2 is not an installation
dependency.

## Features

- Native second-generation (`npe2`) napari reader and dock-widget contributions.
- Paste an `s3://`, HTTP(S), presigned HTTPS, or local image path into napari.
- Automatic loading through drag-and-drop, **File > Open**, and `viewer.open()`.
- ND2 byte-range access through `fsspec`/`s3fs`.
- CZI byte-range access through short-lived, automatically refreshed signed URLs.
- One Dask-backed napari image layer per channel.
- Scene, time, Z, Y, and X axes with physical voxel scale when metadata provides it.
- Plane-level lazy loading: viewing one plane does not download the full dataset.

## Installation

### napari plugin manager

After the package is available on PyPI and napari Hub:

1. Start napari.
2. Choose **Plugins > Install/Uninstall Plugins**.
3. Search for `napari-bfs-nd2` and choose **Install**.
4. Restart napari if the new plugin menu is not immediately visible.

### pip

Install into the same Python environment that launches napari:

```bash
python -m pip install napari-bfs-nd2
```

If napari is not already installed in that environment:

```bash
python -m pip install "napari-bfs-nd2[napari]"
```

For an unreleased wheel:

```bash
python -m pip install /path/to/napari_bfs_nd2-0.3.0-py3-none-any.whl
```

Verify discovery:

```bash
python -m npe2 list | grep napari-bfs-nd2
```

See the [complete installation guide](https://derekthirstrup.github.io/napari-bfs-nd2/installation/)
for environment-specific instructions and uninstall steps.

## Graphical use

1. Start napari.
2. Choose **Plugins > BFS Remote ND2/CZI Reader > Open ND2/CZI path**.
3. Paste a supported path, for example:

   ```text
   s3://my-bucket/experiment/acquisition.nd2
   ```

4. Choose **Open image**.

The status line reports whether layers were added or displays the access/read
error. Drag-and-drop and **File > Open** also invoke the reader automatically for
`.nd2` and `.czi` paths.

## Programmatic use

The normal napari API automatically selects this reader by extension:

```python
import napari

viewer = napari.Viewer()
viewer.open("s3://my-bucket/experiment/acquisition.nd2")
viewer.open("s3://my-bucket/experiment/acquisition.czi")
napari.run()
```

Select the plugin explicitly if another installed plugin claims the same
extension:

```python
viewer.open(
    "s3://my-bucket/experiment/acquisition.nd2",
    plugin="napari-bfs-nd2",
)
```

## Supported inputs

| Format | Local | HTTP(S) | S3 | Remote strategy |
| --- | ---: | ---: | ---: | --- |
| Nikon ND2 | Yes | Yes | Yes | Direct `fsspec` byte-range reads |
| Zeiss CZI | Yes | Yes | Yes | libCZI HTTP ranges; S3 is translated to a signed HTTPS URL |

Remote HTTP servers must support byte-range requests. Query strings on
presigned URLs are supported.

## Layer and metadata model

Each channel becomes an image layer with axes:

```text
(scene, time, z, y, x)
```

The Dask chunk shape is one scene/time/Z plane, so napari requests only the
plane needed for the current view. Physical Z/Y/X scale, channel names, scene
information, and the original source path are added when available. CZI scenes
with different Y/X sizes are zero-padded to the largest scene dimensions.

## Private S3 data

The plugin uses the standard AWS credential chain. Configure credentials before
starting napari, for example:

```bash
export AWS_PROFILE=microscopy
napari
```

Environment credentials, named profiles, shared credential files, and
role/web-identity environment variables are supported. If no credential source
is detected, the plugin tries anonymous access for public buckets. Never paste
AWS secrets into the widget or commit them to this repository.

See [S3 authentication](https://derekthirstrup.github.io/napari-bfs-nd2/s3-authentication/)
for all supported configurations and diagnostics.

## Documentation

- [Installation](https://derekthirstrup.github.io/napari-bfs-nd2/installation/)
- [Using the plugin](https://derekthirstrup.github.io/napari-bfs-nd2/usage/)
- [S3 authentication](https://derekthirstrup.github.io/napari-bfs-nd2/s3-authentication/)
- [Data model and performance](https://derekthirstrup.github.io/napari-bfs-nd2/data-model/)
- [Troubleshooting](https://derekthirstrup.github.io/napari-bfs-nd2/troubleshooting/)
- [Development](https://derekthirstrup.github.io/napari-bfs-nd2/development/)

## Development

```bash
git clone https://github.com/derekthirstrup/napari-bfs-nd2.git
cd napari-bfs-nd2
python -m pip install -e ".[test,docs]"
pytest
python -m npe2 validate src/napari_bfs_nd2/napari.yaml
mkdocs serve
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development workflow.

## Attribution and license

The `RemoteND2Reader` and `CZIReader` implementations are selectively extracted
and adapted from BFS2. See [NOTICE](NOTICE) for attribution details.

This project is distributed under the [BSD 3-Clause License](LICENSE).
