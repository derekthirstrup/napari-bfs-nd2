# Development

## Set up an editable environment

```bash
git clone https://github.com/derekthirstrup/napari-bfs-nd2.git
cd napari-bfs-nd2
python -m venv .venv
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install development dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[test,docs,napari]" ruff build
```

## Repository layout

```text
src/napari_bfs_nd2/
  napari.yaml       npe2 reader and widget contributions
  reader.py         napari LayerData and lazy Dask construction
  widget.py         pasteable path dock widget
  remote_nd2.py     local/fsspec ND2 implementation
  remote_czi.py     local/HTTP/S3 CZI implementation
tests/              unit and lazy-read behavior tests
docs/               GitHub Pages source
```

## Run checks

```bash
ruff check .
pytest
python -m npe2 validate src/napari_bfs_nd2/napari.yaml
mkdocs build --strict
python -m build
```

The test suite avoids requiring a graphical display for reader and widget logic.
The GitHub Actions matrix runs supported Python versions.

## Preview documentation

```bash
mkdocs serve
```

Open the local URL printed by MkDocs. The Pages workflow builds with strict
mode, so unresolved documentation links fail CI.

## Add a reader-facing change

1. Keep `napari_get_reader()` inexpensive and non-throwing.
2. Defer image transfer until the returned reader function is invoked.
3. Preserve plane-sized Dask chunks unless a measured alternative is better.
4. Add focused tests for metadata, dimensions, and requested-byte behavior.
5. Avoid placing presigned URL query strings in metadata or exceptions.

## Add a widget change

The widget is an `npe2` contribution. It receives the active
`napari.viewer.Viewer` through a string annotation so napari can inject the
viewer without making napari a required runtime import for the module.

Keep path opening routed through:

```python
viewer.open(path, plugin="napari-bfs-nd2")
```

This ensures graphical and programmatic loading exercise the same reader.

## Pull requests

Create a focused branch, add or update tests, run all relevant checks, and
explain user-visible behavior in the PR description. See
[CONTRIBUTING.md](https://github.com/derekthirstrup/napari-bfs-nd2/blob/main/CONTRIBUTING.md)
for the full checklist.
