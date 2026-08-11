# Releasing to PyPI and napari hub

The package metadata and `npe2` manifest are ready for napari hub discovery.
The hub automatically indexes a PyPI release that includes the
`Framework :: napari` classifier.

## One-time setup

1. Create `derekthirstrup/napari-bfs-nd2` as a public GitHub repository.
2. In **Settings > Pages**, set the deployment source to **GitHub Actions**.
3. On PyPI, create a pending Trusted Publisher for the GitHub repository. Use
   workflow name `release.yml` and environment name `pypi`.
4. Create the `pypi` GitHub environment and, preferably, require approval before
   deployment.

## Release

1. Update the version in `pyproject.toml` and
   `src/napari_bfs_nd2/__init__.py`.
2. Run the test and packaging checks:

   ```bash
   python -m pip install -e ".[test]"
   pytest
   python -m npe2 validate src/napari_bfs_nd2/napari.yaml
   mkdocs build --strict
   python -m build
   ```

3. Commit and push the release.
4. Publish a GitHub release tagged with the same version, for example `v0.3.0`.

The release workflow builds the wheel/source distribution and publishes both to
PyPI through OpenID Connect; it does not require a long-lived PyPI token. The
napari hub should index the PyPI release automatically.
