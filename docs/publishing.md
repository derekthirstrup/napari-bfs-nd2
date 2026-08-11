# Publishing

## GitHub Pages

The `docs.yml` workflow builds this MkDocs site and deploys it through GitHub
Pages whenever documentation-related files change on `main`.

One-time repository configuration:

1. Open **Settings > Pages**.
2. Under **Build and deployment**, select **GitHub Actions** as the source.
3. Merge the documentation workflow into `main`.
4. Verify the deployment at
   `https://derekthirstrup.github.io/napari-bfs-nd2/`.

## PyPI trusted publishing

The `release.yml` workflow uses GitHub OpenID Connect rather than a stored PyPI
API token.

One-time configuration:

1. Create the `napari-bfs-nd2` project or pending publisher on PyPI.
2. Configure `derekthirstrup/napari-bfs-nd2` as the publisher.
3. Set the workflow filename to `release.yml`.
4. Set the GitHub environment to `pypi`.
5. Create that environment in GitHub and optionally require reviewer approval.

## Release checklist

1. Update the version in `pyproject.toml` and
   `src/napari_bfs_nd2/__init__.py`.
2. Update `CHANGELOG.md`.
3. Run:

   ```bash
   ruff check .
   pytest
   python -m npe2 validate src/napari_bfs_nd2/napari.yaml
   mkdocs build --strict
   python -m build
   ```

4. Merge the release changes into `main`.
5. Create a GitHub release whose tag matches the package version, such as
   `v0.3.0`.
6. Approve the `pypi` environment deployment if protection rules require it.
7. Verify the files and metadata on PyPI.
8. Confirm the package appears on napari Hub and installs from napari's plugin
   manager.

The napari Hub derives its listing from PyPI metadata, the README, and the
`npe2` manifest. Publishing a PyPI package with the `Framework :: napari`
classifier makes it eligible for automatic indexing.
