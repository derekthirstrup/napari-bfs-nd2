# Data model and performance

## Layer dimensions

The plugin creates one napari image layer per microscopy channel. Each layer is
a five-dimensional Dask array:

```text
(scene, time, z, y, x)
```

Singleton dimensions are retained so scripts can use the same indexing model
for single-scene, single-timepoint, and single-Z datasets.

## Lazy plane loading

The Dask chunk shape is:

```text
(1, 1, 1, y, x)
```

Selecting one scene/time/Z coordinate in napari computes one image plane. This
keeps startup focused on metadata parsing and avoids transferring the complete
dataset.

!!! important

    Operations that explicitly materialize the entire Dask array, such as
    `data.compute()`, will read the entire channel. Prefer spatial or temporal
    slicing before computing.

## Channel layers

- Each channel is a separate additive image layer.
- Channel names are read from file metadata when available.
- The second channel uses a green colormap; other channels initially use gray.
- Multichannel viewing is controlled with ordinary napari layer visibility and
  blending settings.

## Physical scale

Layer scale is expressed as:

```text
(1, 1, z_um, y_um, x_um)
```

Missing physical values fall back to `1.0`. Scene and time coordinates remain
index dimensions; acquisition timing and scene position metadata are stored in
the layer metadata instead.

## Layer metadata

Plugin-specific metadata is stored at:

```python
layer.metadata["napari_bfs_nd2"]
```

Common fields include:

| Field | Meaning |
| --- | --- |
| `format` | `"nd2"` or `"czi"` |
| `source` | Original local/S3/HTTP path without a URL query string |
| `channel` | Zero-based channel index |
| `channel_name` | Name extracted from file metadata or a stable fallback |
| `scene_names` | Scene labels in displayed order |

ND2 additionally reports scene positions and time interval when available. CZI
reports the original `(y, x)` shape for each scene.

## Different CZI scene sizes

One Dask array must have one rectangular Y/X shape. If CZI scenes differ in
size, the plugin pads smaller scenes with zeros on the bottom and right to match
the largest scene. Original scene shapes are retained in
`scene_shapes_yx` metadata.

## Remote strategies

### ND2

- Parses the ND2 chunk map and metadata once per source path.
- Reads exact remote byte ranges for requested planes.
- Caches parsed metadata in the process.
- Opens independent reader connections for Dask tasks.

### CZI

- Uses libCZI's HTTP range stream for remote data.
- Converts S3 paths to short-lived HTTPS URLs.
- Caches metadata and reuses bounded reader pools.
- Limits reader cache/concurrency to prevent unbounded memory growth.

## Practical performance guidance

- Keep napari and the S3 bucket in nearby regions when possible.
- Use wired networking for large time series.
- Avoid eagerly computing entire channels.
- Close unused layers before opening several large acquisitions.
- For batch extraction, use `read_zstack()` with a crop and an appropriate
  worker count instead of materializing the napari layer.
