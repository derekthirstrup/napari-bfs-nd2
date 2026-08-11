# Changelog

All notable changes to this project are documented here.

## [0.3.0] - Unreleased

### Added

- `npe2` dock widget for opening pasted local, S3, or HTTP(S) ND2/CZI paths.
- napari Hub metadata and PyPI trusted-publishing workflow.
- GitHub Pages documentation, repository community files, and user guides.
- Tests for widget path validation and reader dispatch.

### Changed

- Expanded package metadata with project, documentation, issue, and changelog
  URLs.
- Documented automatic `viewer.open()` loading and explicit plugin selection.

## [0.2.0]

### Added

- Zeiss CZI support for local, HTTP(S), and S3 paths.
- Scene-aware lazy CZI layers and physical scale metadata.

## [0.1.0]

### Added

- Standalone plane-lazy ND2 reader adapted from BFS2.
- Local, HTTP(S), and S3 ND2 access through `fsspec`.

[0.3.0]: https://github.com/derekthirstrup/napari-bfs-nd2/releases/tag/v0.3.0
[0.2.0]: https://github.com/derekthirstrup/napari-bfs-nd2/releases/tag/v0.2.0
[0.1.0]: https://github.com/derekthirstrup/napari-bfs-nd2/releases/tag/v0.1.0
