# Troubleshooting

## Plugin is not visible

Confirm that the package is installed in the interpreter used by napari:

```bash
python -c "import sys; print(sys.executable)"
python -m pip show napari-bfs-nd2
python -m npe2 list
```

If napari is launched from another environment or bundled application, install
the plugin through napari's plugin manager. Restart all napari processes after
installing or upgrading.

## Reader-selection dialog appears

Another plugin also claims `.nd2` or `.czi`. Use the path widget or select this
plugin explicitly:

```python
viewer.open(path, plugin="napari-bfs-nd2")
```

## Access denied for S3

1. Confirm the complete URI, including capitalization.
2. Verify credentials with `aws s3api head-object`.
3. Confirm that `AWS_PROFILE` is present in the process that launches napari.
4. For temporary credentials, verify `AWS_SESSION_TOKEN` is also set.
5. Confirm `s3:GetObject` permission for the object prefix.

See [S3 authentication](s3-authentication.md) for configuration examples.

## Public S3 object is not readable

Verify anonymous access independently:

```python
import s3fs

fs = s3fs.S3FileSystem(anon=True)
print(fs.info("s3://bucket/path/image.nd2"))
```

A URL visible in an authenticated browser is not necessarily publicly readable
through the S3 API.

## HTTP path fails

The server must support HTTP byte-range requests. Check the response headers:

```bash
curl -I https://example.org/path/image.nd2
```

Look for `Accept-Ranges: bytes`. Redirects, expiring authentication cookies, or
servers that always return the entire object can prevent lazy reads.

## Presigned URL fails later

Presigned URLs expire. Generate a new URL and reopen the layer. Native `s3://`
paths are preferable when AWS credentials are available because CZI URLs are
then refreshed automatically.

## CZI dependency fails to install

CZI support depends on `pylibczirw`. Ensure the Python version and operating
system have a compatible wheel. Upgrade pip before retrying:

```bash
python -m pip install --upgrade pip
python -m pip install napari-bfs-nd2
```

Include the operating system, Python version, and complete installer error in a
bug report.

## Image opens but a plane is blank

- Confirm the selected scene, time, and Z coordinates.
- Check whether the channel contains data at that coordinate.
- For CZI with unequal scene dimensions, bottom/right zero padding is expected.
- Inspect `layer.metadata["napari_bfs_nd2"]` for original scene sizes.

## Memory use increases during full-volume operations

The reader is lazy, but downstream operations may request full arrays. Slice
before computing:

```python
plane = layer.data[scene, time, z].compute()
subvolume = layer.data[scene, time, z0:z1, y0:y1, x0:x1].compute()
```

## Reporting a bug

Use the repository's bug-report form and include:

- operating system;
- Python, napari, and plugin versions;
- ND2 or CZI;
- local, HTTP, or S3 access;
- dimensions if known; and
- the complete traceback with credentials, query strings, and bucket-sensitive
  details removed.
