# Contributing

Contributions that improve compatibility, performance, documentation, or test
coverage are welcome.

## Report a problem

Use the structured GitHub bug-report form. Include the smallest reproducible
example and the complete traceback after removing:

- AWS keys or tokens;
- presigned URL query strings;
- private bucket/object names when necessary; and
- identifying microscopy metadata.

Do not attach proprietary microscopy files to a public issue. A synthetic or
shareable minimal file is preferable.

## Development setup

```bash
git clone https://github.com/derekthirstrup/napari-bfs-nd2.git
cd napari-bfs-nd2
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[test,docs,napari]" ruff build
```

On Windows, activate with `.venv\Scripts\Activate.ps1`.

## Before opening a pull request

Run the relevant checks:

```bash
ruff check .
pytest
python -m npe2 validate src/napari_bfs_nd2/napari.yaml
mkdocs build --strict
python -m build
```

The complete suite should pass before review. New behavior should include
focused tests, and user-facing behavior should update the README or docs.

## Design expectations

- Keep reader discovery cheap and avoid network I/O in `napari_get_reader()`.
- Preserve lazy loading; do not eagerly read entire datasets during open.
- Keep signed URL query strings out of metadata, logs, and user-facing errors.
- Use public napari APIs in the widget.
- Do not add `napari[all]` or a Qt backend to base dependencies.
- Document format-specific differences rather than silently normalizing away
  important acquisition metadata.

## Pull request scope

Use a focused branch and a concise commit. The PR description should explain:

- what changed;
- why it is needed;
- the user-visible impact;
- test data or assumptions; and
- checks that were run.

Avoid mixing unrelated refactors with a reader fix.
