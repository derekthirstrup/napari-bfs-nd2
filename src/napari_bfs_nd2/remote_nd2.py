"""Stream ND2 data from local paths, S3, and HTTP byte ranges.

This module is adapted from BFS2's fsspec-aware ND2 implementation and is
distributed under the BSD 3-Clause license included with this package.
"""

from __future__ import annotations

import logging
import re
import struct
import threading
import zlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import requests

logger = logging.getLogger(__name__)

# Module-level cache for ND2 metadata (chunkmap, dimensions, etc.)
# Key: URL, Value: dict with metadata
_nd2_metadata_cache: dict[str, dict[str, Any]] = {}
# Guards _nd2_metadata_cache against concurrent ThreadPool init (mirrors the
# CZI metadata cache's _cache_lock in czi_reader.py — code-review finding I8).
_nd2_cache_lock = threading.Lock()

# Module-level cache is cleared by clear_file_caches() in load_data.py when switching files

# Module-level HTTP session pool for remote file access
# This prevents connection pool proliferation that causes slowdown over time
# Key: base_url (scheme + host), Value: requests.Session

_http_session_pool: dict[str, requests.Session] = {}
_http_session_lock = threading.Lock()


def _get_http_session(url: str, max_connections: int = 64) -> requests.Session:
    """Get or create a shared HTTP session for the given URL's host.

    This prevents connection pool proliferation by reusing sessions per host.
    Each session has a large connection pool that's shared across all readers.

    Args:
        url: Full URL to extract host from
        max_connections: Max connections in the pool (default 64, supports 2 concurrent Z-stacks)

    Returns:
        Shared requests.Session for this host
    """
    from urllib.parse import urlparse

    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # Extract base URL (scheme + host) as the pool key
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    with _http_session_lock:
        if base_url not in _http_session_pool:
            # Create new session with large connection pool
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(
                pool_connections=max_connections,
                pool_maxsize=max_connections,
                max_retries=retries,
                pool_block=False,  # Don't block when pool is full
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            _http_session_pool[base_url] = session
            logging.getLogger(__name__).info(
                f"Created shared HTTP session for {base_url} (pool_size={max_connections})"
            )
        return _http_session_pool[base_url]


def _fetch_byte_range(url: str, offset: int, size: int) -> bytes:
    """Fetch the byte range ``[offset, offset + size)`` from a remote URL.

    HTTP/HTTPS reads use a pooled :class:`requests.Session` with a ``Range``
    header. Every other scheme (``s3://``, ``gs://``, ``az://`` ...) is routed
    through fsspec, which signs/authenticates requests via the standard
    credential chain. A plain ``requests.get`` cannot fetch an ``s3://`` URL,
    so those reads previously failed at read time even though metadata parsing
    (which already used fsspec) succeeded — see code-review finding I1.

    The http(s) body is read in ONE pass via ``stream=True`` + ``raw.read()``
    rather than ``resp.content``. ``resp.content`` materialises the body through
    ``iter_content`` in 10 KB chunks and joins them, which for the 8-30 MB ranges
    this reader issues is thousands of chunked reads plus a concatenation per
    request, on every thread. Measured on an S3 ND2 with
    ``scripts/bench_nd2_reader.py --phases fetch`` (identical ranges, same pooled
    session, best of 3):

        streams      8     16     32     64     96    128
        resp.content 352   363    391    418    356   329  MB/s
        raw.read()   576   590    788    845    916   838  MB/s

    2.3x at the wide end, at lower CPU, and it tracks the raw-``urlopen``
    baseline (795-862 MB/s) that ``resp.content`` fell half short of. A fresh
    session per call was *slower* (269-277), so connection pooling was never the
    problem.
    """
    if url.startswith(("http://", "https://")):
        session = _get_http_session(url)
        headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
        with session.get(url, headers=headers, timeout=60, stream=True) as resp:
            resp.raise_for_status()
            # A server/proxy that ignores Range answers 200 OK with the *entire*
            # object instead of the requested window. For the multi-GB/TB files this
            # reader targets that silently buffers the whole file into memory, so
            # reject any non-partial response rather than returning the wrong bytes.
            # Checked BEFORE reading the body, so a 200 costs nothing: with
            # stream=True nothing has been transferred yet.
            if resp.status_code != 206:
                raise OSError(
                    f"Remote server did not honor Range request for {url} "
                    f"(status {resp.status_code}, expected 206 Partial Content); "
                    "refusing to buffer the full object."
                )
            # decode_content=True so a Content-Encoding'd body is still returned
            # decoded, matching what resp.content did.
            return resp.raw.read(decode_content=True)

    import fsspec

    storage_options = _s3_storage_options() if url.startswith("s3://") else {}
    fs, path = fsspec.core.url_to_fs(url, **storage_options)
    return fs.cat_file(path, start=offset, end=offset + size)


def _chunk_needed_bytes(chunk_bytes: bytes, offset: int) -> int:
    """Authoritative on-disk length of an ND2 chunk from its 16-byte header.

    Parses ``name_len`` and ``data_len`` from the chunk header and returns
    ``16 + name_len + data_len`` — the length ``nd2.read_nd2_chunk`` trusts.
    Used to detect (and top up) a short read when the cached chunkmap ``size``
    under-reports the true chunk length. Returns ``len(chunk_bytes)`` unchanged
    when fewer than 16 header bytes are available to parse.
    """
    if len(chunk_bytes) < 16:
        return len(chunk_bytes)
    signature = struct.unpack("<I", chunk_bytes[0:4])[0]
    if signature != 0x0ABECEDA:
        raise ValueError(f"Invalid ND2 signature at offset {offset}: {hex(signature)}")
    name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
    data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
    return 16 + name_length + data_length


def _fetch_chunk_remote(url: str, offset: int, known_total: int | None) -> bytes:
    """Fetch exactly one ND2 chunk (16-byte header + name + data) from a URL.

    When the chunkmap already knows the chunk length (``known_total``) we fetch
    exactly that many bytes in a single ranged read. Otherwise we read the
    16-byte header first to compute ``16 + name_len + data_len`` and then fetch
    exactly that range. This avoids the previous fixed ``16 + 8192 + frame +
    1024`` estimate, which over-read every chunk and could truncate chunks with
    long names or larger-than-expected payloads — see code-review finding I2.

    The cached chunkmap ``size`` is only a hint: some ND2 writers under-report
    it (e.g. it excludes trailing padding), so a single ``known_total`` read can
    come up short and truncate the frame. The chunk header's ``data_length`` is
    authoritative (it is what ``nd2.read_nd2_chunk`` trusts), so a short first
    read is topped up with the MISSING TAIL ONLY.

    That top-up used to re-fetch the whole range, and on a file whose chunkmap
    under-reports every chunk that doubled the bytes for every frame. Measured
    with ``scripts/bench_nd2_reader.py --phases scaling`` on an S3 ND2: a read
    Z-bounded to 48 of the file's 153 slices issued 96 requests and moved 8.0x
    the bytes it returned — exactly 2 requests per slice, times the 4x from
    keeping one of four interleaved components. A full 153-slice stack pays the
    same ratio at 306 requests. Fetching only the shortfall makes the second
    request a few KB instead of a second full frame.
    """
    if known_total is not None and known_total >= 16:
        chunk_bytes = _fetch_byte_range(url, offset, known_total)
        needed = _chunk_needed_bytes(chunk_bytes, offset)
        # Loop rather than top up once: a server may answer a range request with FEWER
        # bytes than asked for without erroring, and a single top-up would then return a
        # still-short chunk silently — the same "Frame data too small" failure this path
        # exists to prevent. Each pass asks only for what is still missing, so the common
        # case (one complete tail) costs exactly one extra request as before.
        while len(chunk_bytes) < needed:
            tail = _fetch_byte_range(url, offset + len(chunk_bytes), needed - len(chunk_bytes))
            if not tail:
                # Nothing more to be had (truncated object): fall back to the
                # whole-range refetch so the caller still sees a coherent read
                # rather than a silently short chunk.
                return _fetch_byte_range(url, offset, needed)
            chunk_bytes += tail
        return chunk_bytes

    header = _fetch_byte_range(url, offset, 16)
    if len(header) < 16:
        raise ValueError(f"ND2 chunk header truncated at offset {offset}: got {len(header)} bytes")
    signature = struct.unpack("<I", header[0:4])[0]
    if signature != 0x0ABECEDA:
        raise ValueError(f"Invalid ND2 signature at offset {offset}: {hex(signature)}")
    name_length = struct.unpack("<I", header[4:8])[0]
    data_length = struct.unpack("<Q", header[8:16])[0]
    return _fetch_byte_range(url, offset, 16 + name_length + data_length)


def _read_chunk_from_handle(f: Any, offset: int, known_total: int | None) -> bytes:
    """Read exactly one ND2 chunk from an open local file handle.

    Mirrors :func:`_fetch_chunk_remote` for local/UNC files: an exact read
    using the cached chunkmap length when known, otherwise a two-step
    header-then-exact read. Replaces the over-reading fixed estimate (I2).

    The cached chunkmap ``size`` is only a hint and is occasionally smaller than
    the true ``name_len + data_len`` (some ND2 writers under-report it), which
    would truncate the frame. The chunk header's ``data_length`` is
    authoritative, so when the single ``known_total`` read comes up short we read
    the remaining bytes (the handle is already positioned right after them).
    """
    f.seek(offset)
    if known_total is not None and known_total >= 16:
        chunk_bytes = f.read(known_total)
        needed = _chunk_needed_bytes(chunk_bytes, offset)
        if len(chunk_bytes) < needed:
            chunk_bytes += f.read(needed - len(chunk_bytes))
        return chunk_bytes

    header = f.read(16)
    if len(header) < 16:
        raise ValueError(f"ND2 chunk header truncated at offset {offset}: got {len(header)} bytes")
    signature = struct.unpack("<I", header[0:4])[0]
    if signature != 0x0ABECEDA:
        raise ValueError(f"Invalid ND2 signature at offset {offset}: {hex(signature)}")
    name_length = struct.unpack("<I", header[4:8])[0]
    data_length = struct.unpack("<Q", header[8:16])[0]
    return header + f.read(name_length + data_length)


def _read_and_decode_chunk_mp(args: tuple) -> tuple[int, np.ndarray]:
    """Module-level function for ProcessPoolExecutor (can't pickle methods).

    Args:
        args: Tuple of (output_z_idx, file_path, offset, chunk_size, height, width, dtype,
                        component_count, channels_interleaved, channel_to_component, channel_idx,
                        crop_yx) where crop_yx is optional (y_min, y_max, x_min, x_max) or None

    Returns:
        Tuple of (output_z_idx, decoded_frame)
    """
    # Handle both old format (11 args) and new format with crop (12 args)
    if len(args) == 12:
        (
            output_z_idx,
            file_path,
            offset,
            chunk_size,
            height,
            width,
            dtype_str,
            component_count,
            channels_interleaved,
            channel_to_component,
            channel_idx,
            crop_yx,
        ) = args
    else:
        (
            output_z_idx,
            file_path,
            offset,
            chunk_size,
            height,
            width,
            dtype_str,
            component_count,
            channels_interleaved,
            channel_to_component,
            channel_idx,
        ) = args
        crop_yx = None

    dtype = np.dtype(dtype_str)

    # Read exactly one chunk. ``chunk_size`` now carries the cached chunkmap
    # total length (16 + name + data) or None when unknown, in which case the
    # helper reads the header first and then exactly the right range (I2).
    with open(file_path, "rb") as f:
        chunk_bytes = _read_chunk_from_handle(f, offset, chunk_size)

    # Parse and decode using shared helper
    z_idx, frame = _parse_and_decode_chunk(
        output_z_idx,
        chunk_bytes,
        height,
        width,
        dtype,
        component_count,
        channels_interleaved,
        channel_to_component,
        channel_idx,
    )

    # Apply Y/X cropping if specified
    if crop_yx is not None:
        y_min, y_max, x_min, x_max = crop_yx
        frame = frame[y_min:y_max, x_min:x_max].copy()

    return (z_idx, frame)


def _fetch_and_decode_chunk_mp(args: tuple) -> tuple[int, np.ndarray]:
    """Module-level function for ProcessPoolExecutor with HTTP fetch.

    Each process creates its own HTTP session to avoid GIL contention.

    Args:
        args: Tuple of (output_z_idx, url, offset, chunk_size, height, width, dtype,
                        component_count, channels_interleaved, channel_to_component, channel_idx,
                        crop_yx) where crop_yx is optional (y_min, y_max, x_min, x_max) or None

    Returns:
        Tuple of (output_z_idx, decoded_frame)
    """
    # Handle both old format (11 args) and new format with crop (12 args)
    if len(args) == 12:
        (
            output_z_idx,
            url,
            offset,
            chunk_size,
            height,
            width,
            dtype_str,
            component_count,
            channels_interleaved,
            channel_to_component,
            channel_idx,
            crop_yx,
        ) = args
    else:
        (
            output_z_idx,
            url,
            offset,
            chunk_size,
            height,
            width,
            dtype_str,
            component_count,
            channels_interleaved,
            channel_to_component,
            channel_idx,
        ) = args
        crop_yx = None

    dtype = np.dtype(dtype_str)

    # Fetch exactly one chunk through the fsspec-aware helper so that s3:// URLs
    # are read via s3fs rather than a raw requests.get that cannot sign them
    # (I1). ``chunk_size`` carries the cached chunkmap total or None (I2).
    chunk_bytes = _fetch_chunk_remote(url, offset, chunk_size)

    # Parse and decode using shared helper
    z_idx, frame = _parse_and_decode_chunk(
        output_z_idx,
        chunk_bytes,
        height,
        width,
        dtype,
        component_count,
        channels_interleaved,
        channel_to_component,
        channel_idx,
    )

    # Apply Y/X cropping if specified
    if crop_yx is not None:
        y_min, y_max, x_min, x_max = crop_yx
        frame = frame[y_min:y_max, x_min:x_max].copy()

    return (z_idx, frame)


def _decode_chunk_only(args: tuple) -> tuple[int, np.ndarray]:
    """Module-level decode function for ProcessPoolExecutor (I/O already done).

    Used in pipeline mode where I/O threads feed raw bytes to decode processes.

    Args:
        args: Tuple of (z_idx, chunk_bytes, height, width, dtype_str,
                        component_count, channels_interleaved, channel_to_component, channel_idx)

    Returns:
        Tuple of (z_idx, decoded_frame)
    """
    (
        z_idx,
        chunk_bytes,
        height,
        width,
        dtype_str,
        component_count,
        channels_interleaved,
        channel_to_component,
        channel_idx,
    ) = args

    dtype = np.dtype(dtype_str)

    return _parse_and_decode_chunk(
        z_idx,
        chunk_bytes,
        height,
        width,
        dtype,
        component_count,
        channels_interleaved,
        channel_to_component,
        channel_idx,
    )


def _parse_and_decode_chunk(
    z_idx: int,
    chunk_bytes: bytes,
    height: int,
    width: int,
    dtype: np.dtype,
    component_count: int,
    channels_interleaved: bool,
    channel_to_component: list,
    channel_idx: int,
) -> tuple[int, np.ndarray]:
    """Shared helper for parsing and decoding ND2 chunk data."""
    # Parse ND2 chunk header
    if len(chunk_bytes) < 16:
        raise ValueError(f"Chunk too small: {len(chunk_bytes)} bytes")

    signature = struct.unpack("<I", chunk_bytes[0:4])[0]
    if signature != 0x0ABECEDA:
        raise ValueError(f"Invalid ND2 signature at Z={z_idx}: {hex(signature)}")

    name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
    data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
    data_start = 16 + name_length
    raw_bytes = chunk_bytes[data_start : data_start + data_length]

    # Decode frame (skip 8-byte header, decompress if needed)
    frame_header_size = 8
    if len(raw_bytes) > frame_header_size:
        raw_bytes = raw_bytes[frame_header_size:]

    # Check compression type
    if len(raw_bytes) >= 4:
        # LZ4 magic: 0x04 0x22 0x4D 0x18
        if raw_bytes[0:4] == b"\x04\x22\x4d\x18":
            try:
                import lz4.frame

                raw_bytes = lz4.frame.decompress(raw_bytes)
            except ImportError:
                raise ImportError(
                    "LZ4 compression detected but lz4 package not installed. Run: pip install lz4"
                ) from None
        # zlib magic: 0x78 (0x9c, 0x01, 0x5e, 0xda)
        elif raw_bytes[0] == 0x78:
            try:
                raw_bytes = zlib.decompress(raw_bytes)
            except zlib.error:
                pass

    # Convert to numpy array
    bytes_per_pixel = dtype.itemsize
    pixels_per_frame = height * width * component_count
    expected_bytes = pixels_per_frame * bytes_per_pixel

    if len(raw_bytes) < expected_bytes:
        raise ValueError(f"Frame data too small: got {len(raw_bytes)}, expected {expected_bytes}")

    frame = np.frombuffer(raw_bytes[:expected_bytes], dtype=dtype)

    if component_count > 1:
        frame = frame.reshape((height, width, component_count))
    else:
        frame = frame.reshape((height, width))

    # Extract single channel if interleaved
    if channels_interleaved and frame.ndim == 3:
        if channel_to_component and channel_idx < len(channel_to_component):
            comp_idx = channel_to_component[channel_idx]
        else:
            comp_idx = channel_idx
        frame = frame[:, :, comp_idx]

    return (z_idx, frame)


def clear_nd2_metadata_cache(url: str | None = None) -> None:
    """Clear the ND2 metadata cache.

    Args:
        url: If provided, clear only cache for this URL. If None, clear all.
    """
    global _nd2_metadata_cache
    with _nd2_cache_lock:
        if url is None:
            _nd2_metadata_cache.clear()
        elif url in _nd2_metadata_cache:
            del _nd2_metadata_cache[url]


if TYPE_CHECKING:
    import dask.array as da


def _s3_storage_options() -> dict[str, Any]:
    """fsspec storage options for S3 reads.

    Uses the standard AWS credential chain (env vars, ``~/.aws``, a named
    profile, or an instance/role provider) when credentials are available,
    and falls back to anonymous access (``anon=True``) only when none are
    found — preserving public-bucket reads.

    Forcing ``anon=True`` unconditionally (the previous behavior) made
    private buckets unreadable even when ``AWS_ACCESS_KEY_ID`` /
    ``AWS_SECRET_ACCESS_KEY`` were provided — e.g. a Beaker job's
    ``--env-secrets``. Returning ``{}`` lets s3fs/botocore sign requests.
    """
    import os

    has_creds = bool(
        os.environ.get("AWS_ACCESS_KEY_ID")
        or os.environ.get("AWS_PROFILE")
        or os.environ.get("AWS_ROLE_ARN")
        or os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
        or os.path.exists(os.path.expanduser("~/.aws/credentials"))
    )
    return {} if has_creds else {"anon": True}


class _NamedFileWrapper:
    """Wrapper that adds a 'name' attribute to a file-like object.

    The nd2 library expects file objects to have a 'name' attribute,
    but fsspec's HTTPFile doesn't have one. This wrapper adds it.
    """

    def __init__(self, file_obj: Any, name: str) -> None:
        self._file = file_obj
        self.name = name

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._file, attr)

    def __enter__(self) -> _NamedFileWrapper:
        return self

    def __exit__(self, *args: Any) -> None:
        self._file.close()

def scenes_present_in_file(
    declared_scenes: int,
    num_timepoints: int,
    num_z: int,
    channel_factor: int,
    sequence_count: int,
) -> int:
    """Clamp a declared scene count to the scenes the file can actually serve.

    ``XYPosLoop`` records the positions NIS-Elements was *configured* to acquire.
    That is not the same as the positions a given file holds: an interrupted
    acquisition, or one split across several ``.nd2`` parts, carries the full
    configured count in every part while each part stores a fraction of the
    frames. Advertising a scene that cannot be read is strictly worse than
    reporting the true count -- the index computed for it runs past the end of
    the chunkmap and the read dies with a bare "Frame not found", after the UI
    has already offered the scene and expanded a row for it.

    Returns ``declared_scenes`` unchanged when the frame count is unknown
    (``sequence_count`` of 0) or already consistent, so a file whose metadata is
    trustworthy is never altered.
    """
    frames_per_scene = num_timepoints * num_z * channel_factor
    if frames_per_scene <= 0 or sequence_count <= 0:
        return declared_scenes
    present = sequence_count // frames_per_scene
    if present == 0:
        # Fewer frames than one whole scene: the file is truncated mid-scene.
        # Reported as a single (incomplete) scene rather than zero -- the frames
        # that exist are readable and worth salvaging, `_get_frame_index` coerces
        # a zero scene count back to 1 anyway, and `metadata_extractor` builds its
        # scene list with `range(num_scenes)`, so zero would hide the partial data
        # entirely while the reader still indexed as though one scene existed.
        return min(declared_scenes, 1) if declared_scenes > 0 else declared_scenes
    if declared_scenes > present:
        return present
    return declared_scenes


def z_slices_from_unlabeled_frames(
    loop_counts: dict[str, int],
    num_timepoints: int,
    num_z: int,
    frames_per_tz: int,
    sequence_count: int,
    *,
    stub_experiment_record: bool,
) -> int:
    """Return the Z count for frames a *corrupt* experiment record left unlabelled, else 0.

    NIS-Elements writes the experiment record when an acquisition closes. One
    that never finalized leaves a typeless stub -- ``ImageMetadataLV!`` is
    present but decodes to ``eType=0`` (``LoopType.Unknown``) carrying a bare
    ``uiCount`` and no nesting -- so no loop survives and nothing states whether
    the frames are Z slices or timepoints. (``nd2`` itself refuses such a file
    outright: ``LoopType.Unknown`` passes its ``loop_type > max(LoopType)``
    guard, matches no branch, and raises ``NotImplementedError``.)

    Charge those frames to Z rather than T. These are 3D acquisitions, and
    reading a Z stack as an N-timepoint movie collapses it to ``Z=1`` and drops
    the whole volume -- the values are right but the axis labels are swapped.
    Z *spacing* dies with the loop and must come from ``voxel_z_um``.

    ``stub_experiment_record`` is load-bearing and means specifically: an
    ``ImageMetadataLV!`` chunk exists, and every loop in it was typeless. It has
    to distinguish three cases that all leave ``loop_counts`` empty:

    * **no record at all** -- the long-supported 2D-timelapse case, where the
      unexplained frames really are timepoints. Must not be touched.
    * **a record carrying a loop of a real type we don't map to an axis**
      (PolarLoop, CustomLoop) -- a valid file, likewise not a stub.
    * **a record whose loops are all eType=0** -- the truncated 3D acquisition
      this exists for.

    Returns 0 whenever it does not apply, so the caller keeps its own value --
    and a file that recorded any loop, notably a real timelapse whose
    ``TimeLoop`` / ``NETimeLoop`` / ``ManTimeLoop`` lands in ``loop_counts`` as
    ``T``, is never touched.
    """
    if not stub_experiment_record or loop_counts:
        return 0
    if num_timepoints != 1 or num_z != 1:
        return 0
    if frames_per_tz <= 0 or sequence_count <= 0:
        return 0
    # Floor division on purpose: a file truncated mid-group keeps its complete
    # Z slices and drops the partial one. Refusing to answer for a non-divisible
    # count would fall through to the caller's `num_timepoints == 1` branch,
    # which infers the same number as *timepoints* -- the exact swap this fixes.
    possible_z = sequence_count // frames_per_tz
    return possible_z if possible_z > 1 else 0


class RemoteND2Reader:
    """Read ND2 files from S3/HTTP using byte-range requests, or from local/network paths.

    This reader enables streaming access to large ND2 files (1TB+) without
    downloading the entire file. It uses nd2's low-level parsing functions
    that only require seek() and read() operations, which work with fsspec's
    HTTP file implementation via byte-range requests.

    Also supports local and network paths (e.g., UNC paths like \\\\server\\share\\file.nd2)
    for benchmarking and comparison purposes.

    Key features:
    - Only downloads the frames you actually request (remote) or seeks to them (local)
    - Metadata extraction downloads ~KB of data
    - Each frame download is independent (no sequential access required)
    - Thread-safe: create one reader per thread for parallel access

    Example:
        # Remote S3/HTTP
        reader = RemoteND2Reader("https://s3.../file.nd2")

        # Local or network path
        reader = RemoteND2Reader(r"\\\\server\\share\\file.nd2")

        print(f"Dimensions: {reader.num_timepoints}T x {reader.num_channels}C x {reader.num_z}Z")
        print(f"Frame size: {reader.height} x {reader.width}")

        # Read a single Z-stack (all Z slices for one timepoint/channel)
        z_stack = reader.read_zstack(t=0, c=0)  # Returns (Z, Y, X) array

        # Or get a lazy dask array for the full dataset
        dask_array = reader.to_dask()  # Returns (T, C, Z, Y, X) dask array
    """

    def __init__(self, path: str, block_size: int = 8 * 1024 * 1024) -> None:
        """Initialize ND2 reader for local or remote files.

        Args:
            path: Local path, network path (UNC), or HTTP/HTTPS URL to the ND2 file
            block_size: fsspec block size for HTTP reads (default 8MB)
        """
        self.url = path  # Keep as 'url' for backward compatibility
        self._block_size = block_size

        # Detect if this is a local/network path or remote URL
        self._is_local = not (path.startswith("http://") or path.startswith("https://") or path.startswith("s3://"))
        self._file: Any = None
        self._chunkmap: dict[bytes, tuple[int, int]] = {}
        self._version: tuple[int, int] = (0, 0)

        # Dimension info (populated during init)
        self.width: int = 0
        self.height: int = 0
        self.num_z: int = 1
        self.num_channels: int = 1
        self.num_timepoints: int = 1
        self.num_scenes: int = 1  # Multipoint/XY positions
        self.dtype: np.dtype = np.dtype(np.uint16)
        self.bits_per_component: int = 16
        self.component_count: int = 1  # Components per pixel (usually 1)
        self.sequence_count: int = 0  # Total frames in file
        self.channels: list[str] = []

        # Scene info (populated during init)
        self.scene_names: list[str] = []  # e.g., ["D9_1", "E10_1"] from metadata or fallback
        self.scene_positions: list[tuple[float, float]] = []  # (X, Y) in µm for each scene
        self.estimated_well_ids: list[str] = []  # e.g., ["D9", "E10"] estimated from XY positions

        # Time info
        self.time_interval_ms: float | None = None  # Interval between timepoints in ms

        # Physical voxel sizes in micrometers (parsed during init, None if absent).
        # Parsed here so callers don't reopen the file via get_nd2_metadata_remote().
        self.voxel_size_x_um: float | None = None
        self.voxel_size_y_um: float | None = None
        self.voxel_size_z_um: float | None = None

        # Loop order for frame index calculation
        self._loop_order: list[str] = []  # e.g., ['Z', 'T'] or ['T', 'Z']
        self._loop_counts: dict[str, int] = {}

        # Channel intensity statistics (populated by _verify_channel_mapping)
        # Contains: component (str), name (str), mode, p99, p99.9 for each channel
        self.channel_stats: list[dict[str, str | float]] = []

        # Initialize: parse metadata and build frame mapping
        self._initialize()

    def _initialize(self) -> None:
        """Parse chunkmap and metadata from remote file.

        Uses module-level cache to avoid re-parsing metadata for the same URL.
        """
        global _nd2_metadata_cache
        import fsspec
        from nd2._parse._chunk_decode import get_chunkmap, get_version

        # Check cache first (read under lock — I8)
        # Cache is cleared by clear_file_caches() when switching files
        with _nd2_cache_lock:
            cached = _nd2_metadata_cache.get(self.url)
        if cached is not None:
            # Load from cache
            self._chunkmap = cached["chunkmap"]
            self._version = cached["version"]
            self.width = cached["width"]
            self.height = cached["height"]
            self.num_z = cached["num_z"]
            self.num_channels = cached["num_channels"]
            self.num_timepoints = cached["num_timepoints"]
            self.num_scenes = cached["num_scenes"]
            self.dtype = cached["dtype"]
            self.bits_per_component = cached["bits_per_component"]
            self.component_count = cached["component_count"]
            self.sequence_count = cached["sequence_count"]
            self.channels = cached["channels"]
            self.scene_names = cached["scene_names"]
            self.scene_positions = cached["scene_positions"]
            self.estimated_well_ids = cached["estimated_well_ids"]
            self.time_interval_ms = cached["time_interval_ms"]
            self.voxel_size_x_um = cached.get("voxel_size_x_um")
            self.voxel_size_y_um = cached.get("voxel_size_y_um")
            self.voxel_size_z_um = cached.get("voxel_size_z_um")
            self._loop_order = cached["loop_order"]
            self._loop_counts = cached["loop_counts"]
            self._channels_interleaved = cached.get("channels_interleaved", False)
            self._channel_to_component = cached.get("channel_to_component", [])
            self.channel_stats = cached.get("channel_stats", [])

            # File handle is lazy - only opened when read_frame is called
            # read_zstack uses direct HTTP and doesn't need fsspec file
            self._file = None

            # Log cached mapping for debugging
            if self._channel_to_component:
                mapping_str = ", ".join(
                    f"C{i}({self.channels[i] if i < len(self.channels) else '?'})->comp{self._channel_to_component[i]}"
                    for i in range(len(self._channel_to_component))
                )
                logger.info(f"Loaded cached channel mapping: {mapping_str}")

            logger.info(
                f"RemoteND2Reader (cached metadata): {self.num_timepoints}T x {self.num_scenes}S x "
                f"{self.num_channels}C x {self.num_z}Z x {self.height}Y x {self.width}X"
            )
            return

        # Open file - fsspec for remote, direct open for local
        if self._is_local:
            logger.debug(f"Opening local ND2: {self.url}")
            f = open(self.url, "rb")
            self._file = _NamedFileWrapper(f, self.url)
        else:
            logger.debug(f"Opening remote ND2: {self.url}")
            storage_options = _s3_storage_options() if self.url.startswith("s3://") else {}
            f = fsspec.open(
                self.url,
                mode="rb",
                block_size=self._block_size,
                **storage_options,
            ).open()
            self._file = _NamedFileWrapper(f, self.url)

        # Verify ND2 format and get version
        self._version = get_version(self._file)
        logger.debug(f"ND2 version: {self._version}")

        # Get chunkmap (reads from end of file - small download)
        self._chunkmap = get_chunkmap(self._file)
        logger.debug(f"Chunkmap has {len(self._chunkmap)} chunks")

        # Parse metadata to get dimensions
        self._parse_metadata()

        # Parse experiment loops for frame index mapping
        self._parse_experiment_loops()

        # Determine if channels are interleaved (component_count > 1) or separate frames
        self._channels_interleaved = self.component_count > 1 and self.component_count == self.num_channels

        # Verify and potentially correct channel-to-component mapping using intensity patterns
        if self._channels_interleaved and self.num_channels > 1:
            self._verify_channel_mapping()

        # Cache the metadata (write under lock — I8)
        meta = {
            "chunkmap": self._chunkmap,
            "version": self._version,
            "width": self.width,
            "height": self.height,
            "num_z": self.num_z,
            "num_channels": self.num_channels,
            "num_timepoints": self.num_timepoints,
            "num_scenes": self.num_scenes,
            "dtype": self.dtype,
            "bits_per_component": self.bits_per_component,
            "component_count": self.component_count,
            "sequence_count": self.sequence_count,
            "channels": self.channels,
            "scene_names": self.scene_names,
            "scene_positions": self.scene_positions,
            "estimated_well_ids": self.estimated_well_ids,
            "time_interval_ms": self.time_interval_ms,
            "voxel_size_x_um": self.voxel_size_x_um,
            "voxel_size_y_um": self.voxel_size_y_um,
            "voxel_size_z_um": self.voxel_size_z_um,
            "loop_order": self._loop_order,
            "loop_counts": self._loop_counts,
            "channels_interleaved": self._channels_interleaved,
            "channel_to_component": self._channel_to_component,
            "channel_stats": self.channel_stats,
        }
        with _nd2_cache_lock:
            _nd2_metadata_cache[self.url] = meta

        # Metadata is fully parsed and cached: close the parse-time file handle
        # so it doesn't linger for the reader's lifetime (code-review finding
        # I12). read_frame() lazily reopens via _ensure_file_open() when needed;
        # read_zstack() uses direct byte-range reads and never needs it.
        self.close()

        logger.info(
            f"RemoteND2Reader initialized: {self.num_timepoints}T x {self.num_scenes}S x "
            f"{self.num_channels}C x {self.num_z}Z x {self.height}Y x {self.width}X, dtype={self.dtype}, "
            f"component_count={self.component_count}, channels_interleaved={self._channels_interleaved}"
        )

    def _parse_metadata(self) -> None:
        """Extract dimensions and dtype from metadata chunks."""
        from nd2._parse._chunk_decode import read_nd2_chunk
        from nd2._parse._clx_lite import json_from_clx_lite_variant
        from nd2._parse._clx_xml import json_from_clx_variant

        def decode_chunk(data: bytes) -> dict:
            if data.startswith(b"<"):
                return json_from_clx_variant(data, strip_prefix=False)
            return json_from_clx_lite_variant(data, strip_prefix=False)

        # Read ImageAttributesLV! chunk for dimensions
        if b"ImageAttributesLV!" in self._chunkmap:
            offset, _ = self._chunkmap[b"ImageAttributesLV!"]
            attr_data = read_nd2_chunk(self._file, offset)
            try:
                raw = decode_chunk(attr_data)
                raw = raw.get("SLxImageAttributes", raw)

                self.width = raw.get("uiWidth", 0)
                self.height = raw.get("uiHeight", 0)
                self.bits_per_component = raw.get("uiBpcSignificant", 16)
                self.component_count = raw.get("uiComp", 1)
                self.sequence_count = raw.get("uiSequenceCount", 0)

                # Check for stride/widthBytes info
                width_bytes = raw.get("uiWidthBytes", 0)
                if width_bytes > 0:
                    logger.debug(f"ND2 metadata: uiWidthBytes={width_bytes}")
                # Log all raw attributes for debugging
                logger.debug(f"ImageAttributes keys: {list(raw.keys())}")

                # Determine dtype from bits per component
                if self.bits_per_component <= 8:
                    self.dtype = np.dtype(np.uint8)
                elif self.bits_per_component <= 16:
                    self.dtype = np.dtype(np.uint16)
                else:
                    self.dtype = np.dtype(np.float32)
            except Exception as e:
                logger.warning(f"Failed to parse ImageAttributesLV!: {e}")

        # Read ImageMetadataSeqLV|0! for channel info and component mapping
        self._channel_to_component: list[int] = []  # Maps channel index to component index
        if b"ImageMetadataSeqLV|0!" in self._chunkmap:
            offset, _ = self._chunkmap[b"ImageMetadataSeqLV|0!"]
            meta_data = read_nd2_chunk(self._file, offset)
            try:
                raw = decode_chunk(meta_data)
                raw = raw.get("SLxPictureMetadata", raw)

                # XY voxel size (µm). nd2 uses top-level dCalibration for both X and Y.
                xy_calibration = raw.get("dCalibration")
                if xy_calibration is not None and float(xy_calibration) > 0:
                    self.voxel_size_x_um = float(xy_calibration)
                    self.voxel_size_y_um = float(xy_calibration)

                planes = raw.get("sPicturePlanes", {})
                plane_list = planes.get("sPlaneNew", planes.get("sPlane", {}))

                # Also check for sPlaneID which may have the actual component ordering
                plane_ids = planes.get("sPlaneID", {})

                if isinstance(plane_list, dict):
                    self.channels = []
                    self._channel_to_component = []

                    # Get the actual keys to understand ordering
                    plane_keys = list(plane_list.keys())
                    logger.debug(f"sPicturePlanes keys (unsorted): {plane_keys}")

                    for key in sorted(plane_list.keys()):
                        plane = plane_list[key]
                        name = plane.get("sDescription", f"Channel {key}")
                        self.channels.append(name)

                        # Get component index from the key - extract numeric suffix
                        # Keys can be '0', '1' or 'a0', 'a1' etc.
                        # The numeric part of the key IS the component index
                        match = re.search(r"(\d+)$", key)
                        if match:
                            comp_idx = int(match.group(1))
                        else:
                            # Fallback: use channel index (sequential order)
                            comp_idx = len(self.channels) - 1
                        self._channel_to_component.append(comp_idx)

                        # Log ALL plane fields for debugging
                        logger.debug(f"Plane key='{key}' -> name='{name}', mapped_comp={comp_idx}")
                        for pk, pv in sorted(plane.items()):
                            if not isinstance(pv, (dict, list)) or len(str(pv)) < 200:
                                logger.debug(f"  {pk}: {pv}")

                    self.num_channels = len(self.channels)

                    # Log plane IDs if available
                    if plane_ids:
                        logger.info(f"sPlaneID: {plane_ids}")

                    # Log the channel-to-component mapping
                    mapping_str = ", ".join(
                        f"C{i}({self.channels[i]})->comp{self._channel_to_component[i]}"
                        for i in range(len(self.channels))
                    )
                    logger.debug(f"Channel-to-component mapping: {mapping_str}")
            except Exception as e:
                logger.warning(f"Failed to parse ImageMetadataSeqLV|0!: {e}")

    def _parse_experiment_loops(self) -> None:
        """Parse experiment loops to understand frame ordering.

        This method extracts T (timepoints), Z (slices), and scene count from
        various ND2 metadata sources:

        1. JobrunFrameLoopInfoZJSON - Contains loop definitions with type info
           (type=4 = TimeLapse)
        2. CustomData|X/Y/Z! - Position arrays that reveal Z-slices and scenes
        3. ImageMetadataLV! - Traditional loop parameters (fallback)
        """
        from nd2._parse._chunk_decode import read_nd2_chunk
        from nd2._parse._clx_lite import json_from_clx_lite_variant
        from nd2._parse._clx_xml import json_from_clx_variant

        def decode_chunk(data: bytes) -> dict:
            if data.startswith(b"<"):
                return json_from_clx_variant(data, strip_prefix=False)
            return json_from_clx_lite_variant(data, strip_prefix=False)

        self._loop_order = []
        self._loop_counts = {}
        # A loop whose type we recognise as real but do not map to an axis
        # (PolarLoop, CustomLoop, ...). Tracked separately from _loop_counts so
        # such a file is never mistaken for an unfinalized stub, whose defining
        # feature is that every loop it carries is *typeless* (eType=0).
        unparsed_loop_type_seen = False
        # Set when an eType=0 loop with a real count is actually observed.
        typeless_loop_seen = False

        # Method 1: Try JobrunFrameLoopInfoZJSON for timepoints
        timepoints_from_json = self._parse_loop_info_json()
        if timepoints_from_json:
            self.num_timepoints = timepoints_from_json
            logger.debug(f"Timepoints from JobrunFrameLoopInfoZJSON: {self.num_timepoints}")

        # Method 2: Try position arrays for Z-slices and scenes
        z_from_positions, scenes_from_positions = self._parse_position_arrays()
        if z_from_positions:
            self.num_z = int(z_from_positions)  # Ensure native Python int
            logger.debug(f"Z-slices from position arrays: {self.num_z}")
        if scenes_from_positions:
            self.num_scenes = int(scenes_from_positions)  # Ensure native Python int
            logger.debug(f"Scenes from position arrays: {self.num_scenes}")

        # Estimate well IDs from scene positions (requires scene_positions to be populated)
        if self.scene_positions:
            self._estimate_well_ids()

        # Method 3: Fallback to ImageMetadataLV! for any missing dimensions
        # Uses recursive parsing to handle nested ppNextLevelEx loop structures
        if b"ImageMetadataLV!" in self._chunkmap:
            offset, _ = self._chunkmap[b"ImageMetadataLV!"]
            exp_data = read_nd2_chunk(self._file, offset)

            try:
                raw = decode_chunk(exp_data)
                exp = raw.get("SLxExperiment", raw)

                def parse_loops_recursive(loop_dict: dict) -> None:
                    """Recursively parse loops to extract T, Z, XY counts.

                    ND2 LoopType enum (from nd2.structures.LoopType):
                      0=Unknown, 1=TimeLoop, 2=XYPosLoop, 3=XYDiscrLoop,
                      4=ZStackLoop, 5=PolarLoop, 6=SpectLoop, 7=CustomLoop,
                      8=NETimeLoop, 9=ManTimeLoop, 10=ZStackLoopAccurate
                    """
                    if not isinstance(loop_dict, dict):
                        return

                    # eType is numeric in v3+ files, Type is string in older files
                    loop_type = loop_dict.get("eType", loop_dict.get("Type", ""))
                    loop_pars = loop_dict.get("uLoopPars", {})
                    count = int(loop_pars.get("uiCount", 1)) if isinstance(loop_pars, dict) else 1

                    loop_type_str = str(loop_type)

                    # ZStackLoop: eType=4 or 10(Accurate), or string match
                    if loop_type_str in ("4", "10") or "ZStack" in loop_type_str or "ZSeries" in loop_type_str:
                        self._loop_order.append("Z")
                        self._loop_counts["Z"] = count
                        if self.num_z == 1 and count > 1:
                            self.num_z = count
                            logger.debug(f"Z-slices from ImageMetadataLV!: {count}")
                        # Z voxel size (µm): prefer dZStep, else derive from dZLow/dZHigh.
                        if self.voxel_size_z_um is None and isinstance(loop_pars, dict):
                            z_step = loop_pars.get("dZStep")
                            if z_step is not None and float(z_step) != 0:
                                self.voxel_size_z_um = abs(float(z_step))
                            else:
                                z_low = loop_pars.get("dZLow", 0.0)
                                z_high = loop_pars.get("dZHigh", 0.0)
                                if count > 1 and z_high != z_low:
                                    self.voxel_size_z_um = abs(float(z_high) - float(z_low)) / (count - 1)
                    # TimeLoop: eType=1,8(NE),9(Man), or string match
                    elif (
                        loop_type_str in ("1", "8", "9")
                        or "TimeLoop" in loop_type_str
                        or "NETimeLoop" in loop_type_str
                        or "TimeSeries" in loop_type_str
                    ):
                        self._loop_order.append("T")
                        self._loop_counts["T"] = count
                        if count > 1 and count != self.num_timepoints:
                            logger.debug(
                                f"Updating timepoints from {self.num_timepoints} to {count} (from ImageMetadataLV!)"
                            )
                            self.num_timepoints = count
                    # XYPosLoop: eType=2 or 3(Discr), or string match
                    elif (
                        loop_type_str in ("2", "3")
                        or "XYPosLoop" in loop_type_str
                        or "XYDiscr" in loop_type_str
                        or "MultiPoint" in loop_type_str
                    ):
                        self._loop_order.append("P")
                        self._loop_counts["P"] = count
                        if count > 0 and count != self.num_scenes:
                            logger.debug(f"Setting num_scenes from XYPosLoop: {count} (was {self.num_scenes})")
                            self.num_scenes = count
                    # SpectLoop: eType=6, or string match
                    elif (
                        loop_type_str == "6"
                        or "WavelengthLoop" in loop_type_str
                        or "Spect" in loop_type_str
                        or "Lambda" in loop_type_str
                    ):
                        self._loop_order.append("W")
                        self._loop_counts["W"] = count
                        logger.info(f"Wavelength loop found: count={count}, type={loop_type_str}")
                    elif loop_type_str and loop_type_str not in ("0", ""):
                        # A real loop we simply don't map to an axis. The file is
                        # NOT an unfinalized stub, so the Z fallback below must
                        # not claim its frames.
                        nonlocal unparsed_loop_type_seen
                        unparsed_loop_type_seen = True
                        logger.debug(f"Unknown loop type: {loop_type_str} with count {count}")
                    elif loop_type_str == "0" and isinstance(loop_pars, dict) and int(loop_pars.get("uiCount", 0)) > 0:
                        # LoopType.Unknown carrying a real count: the typeless
                        # stub NIS leaves when an acquisition never finalized.
                        # Recorded positively rather than inferred from "no loop
                        # matched" -- an ImageMetadataLV! that fails to decode,
                        # or holds no loop at all, also leaves _loop_counts empty
                        # but is not this, and must not have frames claimed as Z.
                        #
                        # Both halves must be EXPLICIT. `loop_type` defaults to ""
                        # and `count` to 1, so accepting "" or the default count
                        # would let any dict that merely lacks eType/uiCount — a
                        # nested non-loop container, a partially decoded record —
                        # license the Z fallback and mislabel T frames as Z.
                        nonlocal typeless_loop_seen
                        typeless_loop_seen = True
                        logger.debug(f"Typeless (eType=0) loop with count {count}")

                    # Recurse into nested loops (ppNextLevelEx)
                    next_levels = loop_dict.get("ppNextLevelEx", {})
                    if isinstance(next_levels, dict):
                        for level in next_levels.values():
                            parse_loops_recursive(level)

                if isinstance(exp, dict):
                    parse_loops_recursive(exp)

            except Exception as e:
                logger.warning(f"Failed to parse ImageMetadataLV!: {e}")

        # Whether scene count was set authoritatively by XYPosLoop in ImageMetadataLV!
        scenes_from_loop = "P" in self._loop_counts

        # Verify dimensions make sense with total frame count
        # sequence_count (uiSequenceCount) may or may not include channels:
        # - Interleaved (component_count >= num_channels): sequence_count = T * Z * S
        # - Separate frames (component_count == 1): sequence_count = T * C * Z * S
        if self.sequence_count > 0:
            channels_are_separate = self.component_count < self.num_channels and self.num_channels > 1
            channel_factor = self.num_channels if channels_are_separate else 1

            expected_frames = self.num_timepoints * self.num_z * channel_factor
            if hasattr(self, "num_scenes") and self.num_scenes > 1:
                expected_frames *= self.num_scenes

            if expected_frames != self.sequence_count:
                frames_per_tz = max(self.num_z, 1) * channel_factor
                if hasattr(self, "num_scenes") and self.num_scenes > 1:
                    frames_per_tz *= self.num_scenes

                # Try to infer missing dimension
                unlabeled_z = z_slices_from_unlabeled_frames(
                    self._loop_counts,
                    self.num_timepoints,
                    self.num_z,
                    frames_per_tz,
                    self.sequence_count,
                    # A typeless (eType=0) loop was actually seen, and no loop of
                    # a real type was. Both halves matter: without the first, an
                    # ImageMetadataLV! that fails to decode or holds no loop
                    # would be read as a stub; without the second, a valid file
                    # whose only loop is one we don't map to an axis (PolarLoop,
                    # CustomLoop) would be, and either would have its frames
                    # wrongly claimed as Z.
                    stub_experiment_record=(typeless_loop_seen and not unparsed_loop_type_seen),
                )
                if unlabeled_z:
                    self.num_z = int(unlabeled_z)  # Ensure native Python int
                    logger.warning(
                        f"{Path(getattr(self, 'url', '<unknown>')).name}: ND2 experiment metadata is unusable (no loop "
                        f"survived; the file was likely never finalized). Assigning its "
                        f"{self.num_z} frames to Z, not T. Z spacing is not recoverable from "
                        f"this file -- set voxel_z_um in processing_parameters.csv."
                    )
                elif self.num_timepoints > 1 and self.num_z == 1:
                    # Z might be missing, calculate it
                    possible_z = self.sequence_count // (self.num_timepoints * channel_factor)
                    if hasattr(self, "num_scenes") and self.num_scenes > 1:
                        possible_z //= self.num_scenes
                    if possible_z > 1:
                        self.num_z = int(possible_z)  # Ensure native Python int
                        logger.debug(f"Inferred Z-slices: {self.num_z}")
                elif self.num_timepoints == 1:
                    # T might be missing, calculate it
                    possible_t = self.sequence_count // frames_per_tz
                    if possible_t > 1:
                        self.num_timepoints = int(possible_t)  # Ensure native Python int
                        logger.debug(f"Inferred timepoints: {self.num_timepoints}")

            # Validate num_scenes against the frames the file actually holds.
            #
            # XYPosLoop in ImageMetadataLV! records the positions NIS-Elements was
            # CONFIGURED to acquire, which is not the same as the positions this file
            # contains: an interrupted acquisition, or one split across several .nd2
            # parts, carries the full configured count in every part. This used to be
            # exempted from the check on the grounds that XYPosLoop is authoritative,
            # but advertising a scene that cannot be read is strictly worse than
            # reporting the true count -- `_get_frame_index` then computes an index
            # past the end of the chunkmap and the read dies with a bare
            # "Frame not found ... Available frames: 0-N", after the UI has already
            # offered the scene, listed it, and expanded a per-scene row for it.
            #
            # The frame count wins. Scenes that were configured but never written are
            # not readable by any code path, so trimming to what exists loses nothing
            # and keeps every consumer (preview, scene picker, per-scene expansion)
            # agreeing with the file.
            frames_per_scene = self.num_timepoints * self.num_z * channel_factor
            actual_scenes = scenes_present_in_file(
                self.num_scenes,
                self.num_timepoints,
                self.num_z,
                channel_factor,
                self.sequence_count,
            )
            if actual_scenes != self.num_scenes:
                logger.warning(
                    f"Correcting num_scenes from {self.num_scenes} to {actual_scenes} "
                    f"(sequence_count={self.sequence_count}, frames_per_scene={frames_per_scene})"
                    + (
                        " — the file declares more positions than it contains, which is "
                        "normal for an interrupted acquisition or one split across "
                        "several .nd2 parts."
                        if scenes_from_loop
                        else ""
                    )
                )
                self.num_scenes = actual_scenes
                # Trim scene-related lists to match
                if hasattr(self, "scene_names") and len(self.scene_names) > self.num_scenes:
                    self.scene_names = self.scene_names[: self.num_scenes]
                if hasattr(self, "scene_positions") and len(self.scene_positions) > self.num_scenes:
                    self.scene_positions = self.scene_positions[: self.num_scenes]
                if (
                    hasattr(self, "estimated_well_ids")
                    and self.estimated_well_ids
                    and len(self.estimated_well_ids) > self.num_scenes
                ):
                    self.estimated_well_ids = self.estimated_well_ids[: self.num_scenes]

        # If XYPosLoop gave a higher scene count than position clustering found,
        # rebuild scene lists from position arrays using the known num_z chunk size
        if scenes_from_loop and self.num_scenes > len(getattr(self, "scene_names", [])) and self.num_z > 0:
            self._rebuild_scene_lists_from_positions()

        # Parse time interval after we have dimensions
        self._parse_time_interval()

        logger.debug(
            f"Final dimensions: T={self.num_timepoints}, Z={self.num_z}, "
            f"scenes={getattr(self, 'num_scenes', 1)}, loop_order={self._loop_order}, "
            f"time_interval={self.time_interval_ms}ms"
        )

    def _parse_loop_info_json(self) -> int | None:
        """Parse JobrunFrameLoopInfoZJSON chunk for timepoint count.

        Returns:
            Number of timepoints if found, None otherwise.
        """
        import json

        chunk_key = b"CustomData|JobrunFrameLoopInfoZJSON!"
        if chunk_key not in self._chunkmap:
            return None

        from nd2._parse._chunk_decode import read_nd2_chunk

        try:
            offset, _ = self._chunkmap[chunk_key]
            data = read_nd2_chunk(self._file, offset)

            # Decompress if needed (usually zlib compressed)
            if len(data) >= 2 and data[0] == 0x78:
                data = zlib.decompress(data)

            parsed = json.loads(data.decode("utf-8"))

            # Extract timepoint count from frameLoops
            frame_loops = parsed.get("frameLoops", [])
            if frame_loops:
                # Find max loop index for loop '0' (typically TimeLapse)
                max_t = 0
                for fl in frame_loops:
                    loop_indexes = fl.get("loopIndexes", {})
                    for loop_id, idx in loop_indexes.items():
                        if loop_id == "0":  # Main loop (usually time)
                            max_t = max(max_t, idx)
                return max_t + 1  # Convert 0-based to count

        except Exception as e:
            logger.debug(f"Failed to parse JobrunFrameLoopInfoZJSON: {e}")

        return None

    def _parse_time_interval(self) -> None:
        """Parse time interval between timepoints from various metadata sources.

        Tries multiple sources:
        1. ImageMetadataLV! experiment loop parameters (dPeriod)
        2. CustomData|AcqTimesCache! acquisition times array

        Populates self.time_interval_ms with the interval in milliseconds.
        """
        from nd2._parse._chunk_decode import read_nd2_chunk
        from nd2._parse._clx_lite import json_from_clx_lite_variant
        from nd2._parse._clx_xml import json_from_clx_variant

        def decode_chunk(data: bytes) -> dict:
            if data.startswith(b"<"):
                return json_from_clx_variant(data, strip_prefix=False)
            return json_from_clx_lite_variant(data, strip_prefix=False)

        # Method 1: Try ImageMetadataLV! for loop period
        if b"ImageMetadataLV!" in self._chunkmap:
            try:
                offset, _ = self._chunkmap[b"ImageMetadataLV!"]
                exp_data = read_nd2_chunk(self._file, offset)
                raw = decode_chunk(exp_data)
                exp = raw.get("SLxExperiment", raw)

                if isinstance(exp, dict):
                    # Look for time loop period in uLoopPars
                    loops = exp.get("uLoopPars", {})
                    if isinstance(loops, dict):
                        # dPeriod is the loop interval in milliseconds
                        period = loops.get("dPeriod")
                        if period is not None and period > 0:
                            self.time_interval_ms = float(period)
                            logger.debug(f"Time interval from ImageMetadataLV! dPeriod: {self.time_interval_ms} ms")
                            return

                    # Also check ppNextLevelEx for nested loop structures
                    next_levels = exp.get("ppNextLevelEx", {})
                    if isinstance(next_levels, dict):
                        for level in next_levels.values():
                            if isinstance(level, dict):
                                level_type = str(level.get("eType", ""))
                                if "TimeLoop" in level_type or level_type in ("1", "8", "9"):
                                    loop_pars = level.get("uLoopPars", {})
                                    period = loop_pars.get("dPeriod")
                                    if period is not None and period > 0:
                                        self.time_interval_ms = float(period)
                                        logger.debug(f"Time interval from nested loop: {self.time_interval_ms} ms")
                                        return

            except Exception as e:
                logger.debug(f"Failed to parse time interval from ImageMetadataLV!: {e}")

        # Method 2: Try AcqTimesCache for actual acquisition times
        if b"CustomData|AcqTimesCache!" in self._chunkmap and self.num_timepoints > 1:
            try:
                offset, _ = self._chunkmap[b"CustomData|AcqTimesCache!"]
                data = read_nd2_chunk(self._file, offset)

                # AcqTimesCache is a double array of acquisition times in ms
                times = np.frombuffer(data, dtype=np.float64)

                if len(times) >= 2:
                    # Calculate interval from first two timepoints
                    # Note: frames per timepoint = num_scenes * num_z
                    frames_per_t = self.num_scenes * self.num_z if self.num_scenes > 1 else self.num_z

                    if len(times) > frames_per_t:
                        # Time of first frame of second timepoint minus first frame of first timepoint
                        interval = times[frames_per_t] - times[0]
                        if interval > 0:
                            self.time_interval_ms = float(interval)
                            logger.debug(f"Time interval from AcqTimesCache: {self.time_interval_ms} ms")
                            return

            except Exception as e:
                logger.debug(f"Failed to parse time interval from AcqTimesCache: {e}")

    def _verify_channel_mapping(self) -> None:
        """Verify and correct channel-to-component mapping for HTTP byte-range reads.

        The nd2 library's read_nd2_chunk() internally reorders components, but
        read_zstack() uses direct HTTP byte-range for performance. This method
        compares pixel data between the two methods to find the exact mapping.

        Also computes intensity statistics (mode, P99, P99.9) for threshold selection.
        """
        from nd2._parse._chunk_decode import read_nd2_chunk

        logger.debug("Verifying channel-to-component mapping for HTTP byte-range reads...")

        # Initialize channel statistics storage
        self.channel_stats: list[dict[str, str | float]] = []

        try:
            frame_idx = 0
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()

            if chunk_key not in self._chunkmap:
                logger.debug("Could not find first frame for channel verification")
                return

            offset, _ = self._chunkmap[chunk_key]

            # METHOD 1: Read using nd2 library (returns correctly ordered channels)
            self._ensure_file_open()
            nd2_bytes = read_nd2_chunk(self._file, offset)
            expected_pixels = self.height * self.width * self.component_count
            expected_bytes = expected_pixels * self.dtype.itemsize
            logger.debug(f"nd2 library returned {len(nd2_bytes)} bytes (expected {expected_bytes} for pixels)")

            # read_nd2_chunk returns raw bytes including 8-byte frame header
            # _decode_frame will skip header and decompress if needed
            nd2_frame = self._decode_frame(nd2_bytes)

            if nd2_frame.ndim != 3 or nd2_frame.shape[2] != self.num_channels:
                logger.debug(f"nd2 frame shape {nd2_frame.shape} doesn't match expected format")
                return

            # METHOD 2: Read using byte-range (raw component order).
            # Exact-length read via the cached chunkmap; fsspec-aware so s3://
            # is read through s3fs rather than a raw requests.get (I1/I2).
            known_total = self._chunk_total_bytes(chunk_key)
            if self._is_local:
                with open(self.url, "rb") as f:
                    chunk_bytes = _read_chunk_from_handle(f, offset, known_total)
            else:
                chunk_bytes = _fetch_chunk_remote(self.url, offset, known_total)

            if len(chunk_bytes) < 12:
                logger.debug("Chunk too small for verification")
                return

            # ND2 chunk header structure:
            # - 4 bytes: signature (0x0ABECEDA)
            # - 4 bytes: name_length
            # - 8 bytes: data_length
            # - name_length bytes: name
            # - data_length bytes: data
            signature = struct.unpack("<I", chunk_bytes[0:4])[0]
            if signature != 0x0ABECEDA:
                logger.debug(f"Invalid ND2 signature: {hex(signature)}")
                return

            name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
            data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
            data_start = 16 + name_length
            raw_bytes = chunk_bytes[data_start : data_start + data_length]
            logger.debug(f"HTTP chunk: name_len={name_length}, data_len={data_length}, raw_bytes len={len(raw_bytes)}")

            # Use same decode logic as nd2 path (skips 8-byte header, decompresses)
            http_frame = self._decode_frame(raw_bytes)

            # Compare center pixels to verify data matches
            cy, cx = self.height // 2, self.width // 2
            nd2_center = [nd2_frame[cy, cx, c] for c in range(self.component_count)]
            http_center = [http_frame[cy, cx, c] for c in range(self.component_count)]
            logger.debug(f"Center pixel ({cy},{cx}): nd2={nd2_center}, http={http_center}")

            if nd2_center == http_center:
                logger.debug("nd2 and HTTP frames match at center pixel")

            if http_frame.ndim != 3 or http_frame.shape[2] != self.num_channels:
                logger.debug(f"HTTP frame shape {http_frame.shape} doesn't match expected format")
                return

            # Calculate statistics for each raw component
            raw_stats: list[tuple[int, float, float, float]] = []  # (comp, median, p99, p999)
            for comp in range(http_frame.shape[2]):
                comp_data = http_frame[:, :, comp]
                median_val = float(np.median(comp_data))
                p99 = float(np.percentile(comp_data, 99))
                p999 = float(np.percentile(comp_data, 99.9))
                raw_stats.append((comp, median_val, p99, p999))
                ch_name = self.channels[comp] if comp < len(self.channels) else f"Ch{comp}"
                logger.debug(
                    f"Raw component {comp} ({ch_name}): mode≈{median_val:.1f}, P99={p99:.1f}, P99.9={p999:.1f}"
                )

            # Calculate statistics for each nd2 channel (correct order)
            nd2_stats: list[tuple[int, float, float, float]] = []  # (ch, median, p99, p999)
            for ch_idx in range(nd2_frame.shape[2]):
                ch_data = nd2_frame[:, :, ch_idx]
                median_val = float(np.median(ch_data))
                p99 = float(np.percentile(ch_data, 99))
                p999 = float(np.percentile(ch_data, 99.9))
                nd2_stats.append((ch_idx, median_val, p99, p999))

            # COMPARE: Match channels by statistics (P99 values are most distinctive)
            # Use Hungarian algorithm approach: for each nd2 channel, find closest raw component
            new_mapping: list[int] = []
            used_comps: set[int] = set()

            # Sort channels by how distinctive their P99 is (TL first, then by variance from mean)
            # This ensures we match the most distinctive channels first
            p99_values = [s[2] for s in nd2_stats]
            p99_mean = np.mean(p99_values)
            channel_order = sorted(range(len(nd2_stats)), key=lambda i: abs(nd2_stats[i][2] - p99_mean), reverse=True)

            temp_mapping: dict[int, int] = {}
            for ch_idx in channel_order:
                nd2_p99 = nd2_stats[ch_idx][2]
                ch_name = self.channels[ch_idx] if ch_idx < len(self.channels) else f"Ch{ch_idx}"

                best_comp = -1
                best_diff = float("inf")

                for comp, _, raw_p99, _ in raw_stats:
                    if comp in used_comps:
                        continue
                    diff = abs(nd2_p99 - raw_p99)
                    if diff < best_diff:
                        best_diff = diff
                        best_comp = comp

                temp_mapping[ch_idx] = best_comp
                used_comps.add(best_comp)
                logger.debug(
                    f"Channel {ch_idx} ({ch_name}) P99={nd2_p99:.1f} matches "
                    f"raw component {best_comp} P99={raw_stats[best_comp][2]:.1f} (diff={best_diff:.1f})"
                )

            # Convert to ordered list
            new_mapping = [temp_mapping[i] for i in range(len(nd2_stats))]

            # Validate mapping (each component should be used exactly once)
            if len(set(new_mapping)) == len(new_mapping) and -1 not in new_mapping:
                # Check if mapping differs from original
                if new_mapping != self._channel_to_component:
                    old_mapping_str = ", ".join(
                        f"C{i}({self.channels[i]})->comp{self._channel_to_component[i]}"
                        for i in range(len(self.channels))
                    )
                    logger.warning(f"Original mapping was incorrect: {old_mapping_str}")

                    self._channel_to_component = new_mapping

                    new_mapping_str = ", ".join(
                        f"C{i}({self.channels[i]})->comp{self._channel_to_component[i]}"
                        for i in range(len(self.channels))
                    )
                    logger.debug(f"Corrected mapping: {new_mapping_str}")
                else:
                    logger.debug("Channel mapping verified correct")
            else:
                logger.warning(f"Invalid mapping detected: {new_mapping}, keeping original")

            # Store channel stats indexed by channel (using corrected mapping)
            # Stats come from nd2 library read which has correct channel order (nd2_frame already loaded)
            for ch_idx in range(nd2_frame.shape[2]):
                ch_data = nd2_frame[:, :, ch_idx]
                median_val = float(np.median(ch_data))
                p99 = float(np.percentile(ch_data, 99))
                p999 = float(np.percentile(ch_data, 99.9))
                ch_name = self.channels[ch_idx] if ch_idx < len(self.channels) else f"Ch{ch_idx}"

                self.channel_stats.append(
                    {
                        "component": self._channel_to_component[ch_idx],
                        "name": ch_name,
                        "mode": median_val,
                        "p99": p99,
                        "p99.9": p999,
                    }
                )

                logger.debug(f"Channel {ch_idx} ({ch_name}): mode≈{median_val:.1f}, P99={p99:.1f}, P99.9={p999:.1f}")

        except Exception as e:
            logger.warning(f"Could not verify channel mapping: {e}")
            import traceback

            traceback.print_exc()

    def _rebuild_scene_lists_from_positions(self) -> None:
        """Rebuild scene_names/scene_positions when XYPosLoop count > clustered count.

        Position array clustering can merge nearby scenes (within 100um grid).
        When the authoritative XYPosLoop count is higher, re-derive scenes by
        splitting the position arrays into chunks of num_z frames each.
        """
        from nd2._parse._chunk_decode import read_nd2_chunk

        x_key = b"CustomData|X!"
        y_key = b"CustomData|Y!"
        if x_key not in self._chunkmap or y_key not in self._chunkmap:
            return

        try:
            offset, _ = self._chunkmap[x_key]
            x_arr = np.frombuffer(read_nd2_chunk(self._file, offset), dtype=np.float64)
            offset, _ = self._chunkmap[y_key]
            y_arr = np.frombuffer(read_nd2_chunk(self._file, offset), dtype=np.float64)

            if len(x_arr) == 0:
                return

            # Each scene has num_z * num_timepoints frames per scene.
            # For separate-channel files, multiply by channel count.
            channels_are_separate = self.component_count < self.num_channels and self.num_channels > 1
            channel_factor = self.num_channels if channels_are_separate else 1
            frames_per_scene = self.num_z * self.num_timepoints * channel_factor
            point_names = self._parse_multipoint_names()

            self.scene_positions = []
            self.scene_names = []
            for s in range(self.num_scenes):
                start = s * frames_per_scene
                end = min(start + frames_per_scene, len(x_arr))
                if start >= len(x_arr):
                    # No more position data — remaining scenes not acquired
                    continue
                chunk_x = x_arr[start:end]
                chunk_y = y_arr[start:end]
                # Skip scenes where all positions are NaN (unchecked in NIS-Elements)
                if np.all(np.isnan(chunk_x)) or np.all(np.isnan(chunk_y)):
                    continue
                mean_x = float(np.nanmean(chunk_x))
                mean_y = float(np.nanmean(chunk_y))
                self.scene_positions.append((mean_x, mean_y))

                # Match to a point name
                best_name = None
                best_dist = float("inf")
                for (px, py), pname in point_names.items():
                    dist = ((mean_x - px) ** 2 + (mean_y - py) ** 2) ** 0.5
                    if dist <= best_dist:
                        best_dist = dist
                        best_name = pname

                if best_name and best_dist < 500:
                    self.scene_names.append(best_name)
                else:
                    self.scene_names.append(f"Scene_{s} (X={mean_x:.0f}, Y={mean_y:.0f})")

            # Correct num_scenes to actual acquired count
            acquired = len(self.scene_positions)
            if acquired < self.num_scenes:
                logger.info(
                    f"Filtered phantom scenes: {acquired} acquired out of {self.num_scenes} planned (XYPosLoop count)"
                )
                self.num_scenes = acquired

            logger.info(f"Rebuilt scene lists from position arrays: {self.num_scenes} scenes")

            # Re-estimate well IDs with the new positions
            if self.scene_positions:
                self._estimate_well_ids()

        except Exception as e:
            logger.warning(f"Failed to rebuild scene lists: {e}")

    def _parse_multipoint_names(self) -> dict[tuple[float, float], str]:
        """Parse point names from ImageMetadataLV! XYPosLoop.

        Returns:
            Dict mapping (rounded_x, rounded_y) to point name (e.g., 'D9_1', 'E10_1')
        """
        from nd2._parse._chunk_decode import read_nd2_chunk
        from nd2._parse._clx_lite import json_from_clx_lite_variant
        from nd2._parse._clx_xml import json_from_clx_variant

        def decode_chunk(data: bytes) -> dict:
            if data.startswith(b"<"):
                return json_from_clx_variant(data, strip_prefix=False)
            return json_from_clx_lite_variant(data, strip_prefix=False)

        point_names: dict[tuple[float, float], str] = {}

        if b"ImageMetadataLV!" not in self._chunkmap:
            return point_names

        try:
            offset, _ = self._chunkmap[b"ImageMetadataLV!"]
            exp_data = read_nd2_chunk(self._file, offset)
            raw = decode_chunk(exp_data)
            exp = raw.get("SLxExperiment", raw)

            if not isinstance(exp, dict):
                return point_names

            # Look in ppNextLevelEx for XYPosLoop (eType=2)
            next_levels = exp.get("ppNextLevelEx", {})
            if isinstance(next_levels, dict):
                for level in next_levels.values():
                    if isinstance(level, dict):
                        level_type = level.get("eType", "")
                        # eType=2 is XYPosLoop/Multipoint
                        if str(level_type) == "2":
                            loop_pars = level.get("uLoopPars", {})
                            # Get the actual count - only use that many points
                            point_count = loop_pars.get("uiCount", 0)
                            points = loop_pars.get("Points", {})
                            if isinstance(points, dict) and point_count > 0:
                                # Points dict may have stale entries - only use the last 'point_count' entries
                                # Sort by key to get consistent order, then take last N
                                sorted_keys = sorted(points.keys())
                                active_keys = (
                                    sorted_keys[-point_count:] if len(sorted_keys) > point_count else sorted_keys
                                )
                                for key in active_keys:
                                    point_data = points[key]
                                    if isinstance(point_data, dict):
                                        pos_name = point_data.get("dPosName", "")
                                        pos_x = point_data.get("dPosX")
                                        pos_y = point_data.get("dPosY")
                                        if pos_name and pos_x is not None and pos_y is not None:
                                            # Filter out stale entries - they have rounded positions
                                            # Active entries have precise positions with many decimal places
                                            # Check if position has significant fractional part (not just .0 or .1)
                                            x_frac = abs(pos_x) % 1
                                            y_frac = abs(pos_y) % 1
                                            # Stale entries are rounded to ~0.1, active have precision like 0.871989
                                            is_precise = (x_frac > 0.2 or x_frac < 0.001) or (
                                                y_frac > 0.2 or y_frac < 0.001
                                            )
                                            # Actually simpler: check if the fractional part has more than 1 significant digit
                                            x_str = f"{pos_x:.6f}"
                                            y_str = f"{pos_y:.6f}"
                                            x_decimals = x_str.split(".")[-1].rstrip("0")
                                            y_decimals = y_str.split(".")[-1].rstrip("0")
                                            is_precise = len(x_decimals) > 1 or len(y_decimals) > 1

                                            if is_precise:
                                                pkey = (float(pos_x), float(pos_y))
                                                point_names[pkey] = pos_name
                                            else:
                                                logger.debug(
                                                    f"Skipping stale point {pos_name} at ({pos_x}, {pos_y}) - rounded position"
                                                )
                            break  # Found XYPosLoop, no need to continue

            logger.info(f"Parsed {len(point_names)} multipoint names from metadata: {point_names}")
            return point_names

        except Exception as e:
            logger.debug(f"Failed to parse multipoint names: {e}")
            return point_names

    def _estimate_well_id_from_position(self, x_um: float, y_um: float) -> str:
        """Estimate well ID from XY stage position using 96-well plate geometry.

        Uses the standard 96-well plate layout:
        - Rows: A-H (8 rows)
        - Columns: 1-12 (12 columns)
        - Well spacing: 9000 µm (9 mm)
        - A1 offset from plate corner: 14.3mm x 11.36mm
        - Plate dimensions: 126.6mm x 85.7mm

        The stage coordinates are typically centered on the plate, so we need
        to calculate the well position relative to A1.

        Args:
            x_um: X stage position in micrometers
            y_um: Y stage position in micrometers

        Returns:
            Well ID string (e.g., "A1", "D9", "H12")
        """
        # 96-well plate parameters (in µm)
        WELL_SPACING_UM = 9000.0
        PLATE_WIDTH_UM = 126600.0  # 126.6 mm
        PLATE_HEIGHT_UM = 85700.0  # 85.7 mm
        A1_OFFSET_X_UM = 14300.0  # 14.3 mm from left edge to A1 center
        A1_OFFSET_Y_UM = 11360.0  # 11.36 mm from bottom edge to A1 center

        # Row labels (A at top = row 0, H at bottom = row 7)
        # Note: In ND2, Y increases downward typically, and rows go A->H top to bottom
        ROWS = ["H", "G", "F", "E", "D", "C", "B", "A"]  # Reversed for Y axis
        COLS = [str(i) for i in range(1, 13)]  # 1-12

        # Calculate plate center (stage coordinates are typically centered)
        plate_center_x = PLATE_WIDTH_UM / 2
        plate_center_y = PLATE_HEIGHT_UM / 2

        # A1 position relative to plate center
        a1_x = A1_OFFSET_X_UM - plate_center_x
        a1_y = A1_OFFSET_Y_UM - plate_center_y

        # ND2 stage coordinates: negative X goes right, negative Y goes up
        # Invert signs to match plate coordinate system
        stage_x = -x_um
        stage_y = -y_um

        # Calculate well indices relative to A1
        col_idx = round((stage_x - a1_x) / WELL_SPACING_UM)
        row_idx = round((stage_y - a1_y) / WELL_SPACING_UM)

        # Clamp to valid range
        col_idx = max(0, min(11, col_idx))
        row_idx = max(0, min(7, row_idx))

        # Get well ID
        row_letter = ROWS[row_idx]
        col_number = COLS[col_idx]

        return f"{row_letter}{col_number}"

    def _estimate_well_ids(self) -> None:
        """Estimate well IDs for all scenes from their XY positions.

        Populates self.estimated_well_ids with well IDs like "D9", "E10".
        """
        self.estimated_well_ids = []

        for x_um, y_um in self.scene_positions:
            well_id = self._estimate_well_id_from_position(x_um, y_um)
            self.estimated_well_ids.append(well_id)

        logger.debug(f"Estimated well IDs: {self.estimated_well_ids}")

    def _parse_position_arrays(self) -> tuple[int | None, int | None]:
        """Parse CustomData|X/Y/Z! arrays to determine Z-slices and scenes.

        ND2 files store per-frame position data in these arrays. By analyzing
        when positions change, we can determine:
        - Z-slices: Number of frames before XY position changes
        - Scenes: Number of unique XY position clusters

        Also populates:
        - self.scene_names: List of scene names (from metadata Point Names or XY positions)
        - self.scene_positions: List of (X, Y) tuples in µm

        Returns:
            Tuple of (num_z, num_scenes), with None for values that couldn't be determined.
        """
        from nd2._parse._chunk_decode import read_nd2_chunk

        # First, get the point names from metadata (e.g., 'D9_1', 'E10_1')
        point_names = self._parse_multipoint_names()

        # Check if position arrays exist
        x_key = b"CustomData|X!"
        y_key = b"CustomData|Y!"

        if x_key not in self._chunkmap or y_key not in self._chunkmap:
            return None, None

        try:
            # Read X and Y position arrays
            offset, _ = self._chunkmap[x_key]
            x_data = read_nd2_chunk(self._file, offset)
            x_arr = np.frombuffer(x_data, dtype=np.float64)

            offset, _ = self._chunkmap[y_key]
            y_data = read_nd2_chunk(self._file, offset)
            y_arr = np.frombuffer(y_data, dtype=np.float64)

            if len(x_arr) == 0 or len(y_arr) == 0:
                return None, None

            # Cluster scene positions by distance (handles stage drift without
            # rounding that can merge distinct nearby positions).
            # IMPORTANT: Preserve order of first occurrence (acquisition order).
            # Assumes < 1 µm stage drift during Z-stack acquisition (typical for
            # well-equilibrated motorized stages; < 0.25 µm/hour observed drift).
            DRIFT_TOLERANCE_UM = 1.0
            unique_xy_ordered: list[tuple[float, float]] = []
            scene_assignments: list[int] = []

            for i in range(len(x_arr)):
                x, y = float(x_arr[i]), float(y_arr[i])
                # Skip NaN positions (e.g. unchecked/disabled points in NIS-Elements).
                # Assign to the previous scene to avoid inflating scene count.
                if x != x or y != y:  # NaN != NaN is always True
                    scene_assignments.append(scene_assignments[-1] if scene_assignments else 0)
                    continue
                # Find closest existing cluster
                matched = False
                for idx, (cx, cy) in enumerate(unique_xy_ordered):
                    if ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5 < DRIFT_TOLERANCE_UM:
                        scene_assignments.append(idx)
                        matched = True
                        break
                if not matched:
                    scene_assignments.append(len(unique_xy_ordered))
                    unique_xy_ordered.append((x, y))

            num_scenes = len(unique_xy_ordered)

            scene_assignments = np.array(scene_assignments)

            # Find Z-slices by detecting scene transitions
            transitions = np.where(np.diff(scene_assignments) != 0)[0] + 1
            if len(transitions) > 0:
                num_z = transitions[0]  # Frames before first scene change
            else:
                # No scene transitions - either single scene or all same position
                if num_scenes == 1:
                    # Calculate Z from total frames and timepoints (if known)
                    num_z = None
                else:
                    num_z = len(x_arr)

            # Calculate actual scene positions and get names
            # Group by scene assignment and compute mean XY for more accurate positions
            self.scene_positions = []
            self.scene_names = []

            for scene_idx in range(num_scenes):
                # Find frames belonging to this scene
                scene_mask = scene_assignments == scene_idx
                if np.any(scene_mask):
                    # Use mean position (more accurate than rounded cluster center)
                    mean_x = float(np.mean(x_arr[scene_mask]))
                    mean_y = float(np.mean(y_arr[scene_mask]))
                    self.scene_positions.append((mean_x, mean_y))

                    # Find closest point name by distance (not just rounded key match)
                    # Use <= so that later entries in dict (newer names) win ties
                    # This handles stale metadata where old names exist at same positions
                    best_name = None
                    best_dist = float("inf")
                    for (px, py), pname in point_names.items():
                        dist = ((mean_x - px) ** 2 + (mean_y - py) ** 2) ** 0.5
                        if dist <= best_dist:
                            best_dist = dist
                            best_name = pname

                    if best_name and best_dist < 500:  # Within 500um tolerance
                        logger.info(
                            f"Scene {scene_idx}: pos=({mean_x:.1f}, {mean_y:.1f}) -> closest match: {best_name} (dist={best_dist:.0f}um)"
                        )
                        self.scene_names.append(best_name)
                    else:
                        # Fallback to generic name with position
                        name = f"Scene_{scene_idx} (X={mean_x:.0f}, Y={mean_y:.0f})"
                        logger.info(
                            f"Scene {scene_idx}: pos=({mean_x:.1f}, {mean_y:.1f}) -> no close match, using {name}"
                        )
                        self.scene_names.append(name)
                else:
                    # Fallback to cluster center from ordered list
                    x_pos, y_pos = unique_xy_ordered[scene_idx]
                    self.scene_positions.append((float(x_pos), float(y_pos)))
                    # Find closest point name by distance (use <= so later entries win ties)
                    best_name = None
                    best_dist = float("inf")
                    for (px, py), pname in point_names.items():
                        dist = ((x_pos - px) ** 2 + (y_pos - py) ** 2) ** 0.5
                        if dist <= best_dist:
                            best_dist = dist
                            best_name = pname
                    if best_name and best_dist < 500:
                        self.scene_names.append(best_name)
                    else:
                        self.scene_names.append(f"Scene_{scene_idx} (X={x_pos:.0f}, Y={y_pos:.0f})")

            logger.debug(f"Extracted {num_scenes} scene positions: {self.scene_names}")

            return num_z, num_scenes

        except Exception as e:
            logger.debug(f"Failed to parse position arrays: {e}")
            return None, None

    def _get_frame_index(self, t: int, c: int, z: int, s: int = 0) -> int:
        """Calculate linear frame index from (T, S, C, Z) coordinates.

        ND2 files store frames sequentially. There are two storage modes:

        1. Channels interleaved (component_count > 1):
           - Each frame contains all channels as components
           - Frame ordering: T -> S -> Z
           - Channel extracted from frame data, not frame index

        2. Channels as separate frames (component_count == 1):
           - Each channel has its own frame
           - Frame ordering: T -> S -> C -> Z (typical for multi-wavelength)
           - Channel IS part of frame index

        Args:
            t: Timepoint index (0-based)
            c: Channel index (0-based)
            z: Z-slice index (0-based)
            s: Scene/position index (0-based)

        Returns:
            Linear frame index into ImageDataSeq|N! chunks
        """
        num_z = self.num_z if self.num_z > 0 else 1
        num_s = self.num_scenes if self.num_scenes > 0 else 1
        num_c = self.num_channels if self.num_channels > 0 else 1

        if self._channels_interleaved:
            # Mode 1: Channels interleaved in each frame (component_count > 1)
            # Frame ordering: T -> S -> Z (channel extracted from frame data)
            frames_per_scene = num_z
            frames_per_timepoint = num_s * frames_per_scene
            frame_idx = t * frames_per_timepoint + s * frames_per_scene + z
        else:
            # Mode 2: Channels as separate frames (component_count == 1)
            # Frame ordering: T -> S -> C -> Z
            frames_per_channel = num_z
            frames_per_scene = num_c * frames_per_channel
            frames_per_timepoint = num_s * frames_per_scene
            frame_idx = t * frames_per_timepoint + s * frames_per_scene + c * frames_per_channel + z

        return frame_idx

    def _decode_frame(self, raw_bytes: bytes) -> np.ndarray:
        """Decode raw chunk bytes to numpy array.

        Args:
            raw_bytes: Raw bytes from read_nd2_chunk()

        Returns:
            2D numpy array of shape (height, width) for single-channel,
            or (height, width, components) for multi-component
        """
        # ND2 frame data has an 8-byte header before the actual pixel data
        # The header contains timestamp/metadata that we skip
        # Check for zlib compression (magic bytes 0x78 0x9c or 0x78 0x01)
        # Note: nd2 library does zlib.decompress(chunk[8:]) - skipping 8 bytes
        frame_header_size = 8
        if len(raw_bytes) > frame_header_size:
            # Skip the 8-byte frame header
            raw_bytes = raw_bytes[frame_header_size:]

        original_len = len(raw_bytes)
        compression_type = "none"

        # Check compression type
        if len(raw_bytes) >= 4:
            # LZ4 magic: 0x04 0x22 0x4D 0x18
            if raw_bytes[0:4] == b"\x04\x22\x4d\x18":
                compression_type = "lz4"
                try:
                    import lz4.frame

                    raw_bytes = lz4.frame.decompress(raw_bytes)
                except ImportError:
                    raise ImportError(
                        "LZ4 compression detected but lz4 package not installed. Run: pip install lz4"
                    ) from None
            # zlib magic: 0x78 (0x9c, 0x01, 0x5e, 0xda)
            elif raw_bytes[0] == 0x78:
                compression_type = "zlib"
                try:
                    raw_bytes = zlib.decompress(raw_bytes)
                except zlib.error:
                    compression_type = "none"  # Not actually compressed

        if compression_type != "none":
            logger.debug(f"Decompressed ({compression_type}): {original_len} -> {len(raw_bytes)} bytes")

        # Calculate expected size
        bytes_per_pixel = self.dtype.itemsize
        pixels_per_frame = self.height * self.width * self.component_count
        expected_bytes = pixels_per_frame * bytes_per_pixel

        if len(raw_bytes) < expected_bytes:
            raise ValueError(
                f"Frame data too small: got {len(raw_bytes)} bytes, "
                f"expected {expected_bytes} for {self.height}x{self.width}x{self.component_count}"
            )

        # Convert to numpy array
        frame = np.frombuffer(raw_bytes[:expected_bytes], dtype=self.dtype)

        # Reshape based on component count
        if self.component_count > 1:
            frame = frame.reshape((self.height, self.width, self.component_count))
        else:
            frame = frame.reshape((self.height, self.width))

        return frame

    def _ensure_file_open(self) -> None:
        """Lazily open the file handle if not already open."""
        if self._file is None:
            if self._is_local:
                logger.debug(f"Lazily opening local file for: {self.url}")
                f = open(self.url, "rb")
            else:
                import fsspec

                logger.debug(f"Lazily opening fsspec file for: {self.url}")
                storage_options = _s3_storage_options() if self.url.startswith("s3://") else {}
                f = fsspec.open(
                    self.url,
                    mode="rb",
                    block_size=self._block_size,
                    **storage_options,
                ).open()
            self._file = _NamedFileWrapper(f, self.url)

    def _component_for_channel(self, c: int) -> int:
        """Map a metadata channel index to its component index within a frame.

        Interleaved ND2 frames are ``(Y, X, component)``. The component order is
        the numeric suffix of the ``sPicturePlanes`` keys, while the channel order
        is those keys sorted as *strings*, so the two diverge as soon as a file has
        10+ planes or non-sequential keys (``a0, a1, a10, a2`` -> C2 is component
        10). ``_verify_channel_mapping`` can also rewrite the mapping from pixel
        statistics. Every reader entry point must resolve the component through
        this one place, or the same ``c`` returns different pixels depending on
        which method the caller used. Falls back to ``c`` when no mapping was
        parsed.
        """
        if self._channel_to_component and c < len(self._channel_to_component):
            return self._channel_to_component[c]
        return c

    def read_frame(self, t: int = 0, c: int = 0, z: int = 0, s: int = 0) -> np.ndarray:
        """Read a single 2D frame from the remote file.

        Interleaved frames are demultiplexed through
        :meth:`_component_for_channel`, so ``c`` selects the same pixels here as
        in :meth:`read_zstack` / :meth:`read_zstack_multichannel`.

        Args:
            t: Timepoint index (0-based)
            c: Channel index (0-based)
            z: Z-slice index (0-based)
            s: Scene/position index (0-based)

        Returns:
            2D numpy array of shape (height, width)
        """
        from nd2._parse._chunk_decode import read_nd2_chunk

        # Ensure fsspec file is open (lazy initialization)
        self._ensure_file_open()

        frame_idx = self._get_frame_index(t, c, z, s)
        chunk_key = f"ImageDataSeq|{frame_idx}!".encode()

        if chunk_key not in self._chunkmap:
            raise KeyError(
                f"Frame not found: t={t}, c={c}, z={z}, s={s} (frame_idx={frame_idx}). "
                f"Available frames: 0-{self.sequence_count - 1}"
            )

        offset, _ = self._chunkmap[chunk_key]
        raw_bytes = read_nd2_chunk(self._file, offset)
        frame = self._decode_frame(raw_bytes)

        # Extract single channel if channels are interleaved in the frame.
        # N14: this used the raw channel index, so the Streamlit preview
        # (load_data.py) showed a different component than read_zstack fed the
        # pipeline whenever the mapping is not the identity.
        if self._channels_interleaved and frame.ndim == 3:
            frame = frame[:, :, self._component_for_channel(c)]

        return frame

    def _parse_chunk_from_buffer(self, buffer: bytes, buffer_offset: int) -> tuple[bytes, int]:
        """Parse a single ND2 chunk from a buffer.

        ND2 chunk format:
        - 4 bytes: signature (0x0ABECEDA)
        - 8 bytes: data length (uint64 LE)
        - N bytes: data

        Args:
            buffer: Byte buffer containing chunk data
            buffer_offset: Offset within buffer where chunk starts

        Returns:
            Tuple of (data_bytes, next_chunk_offset)
        """
        if len(buffer) < buffer_offset + 12:
            raise ValueError(f"Buffer too small for chunk header at offset {buffer_offset}")

        signature = struct.unpack("<I", buffer[buffer_offset : buffer_offset + 4])[0]
        if signature != 0x0ABECEDA:
            raise ValueError(f"Invalid ND2 chunk signature at buffer offset {buffer_offset}: {hex(signature)}")

        data_length = struct.unpack("<Q", buffer[buffer_offset + 4 : buffer_offset + 12])[0]

        data_start = buffer_offset + 12
        data_end = data_start + data_length

        if len(buffer) < data_end:
            raise ValueError(f"Buffer too small for chunk data: need {data_end}, have {len(buffer)}")

        return buffer[data_start:data_end], data_end

    def _chunk_total_bytes(self, chunk_key: bytes) -> int | None:
        """Exact on-disk length of a chunk (16-byte header + name + data).

        Derived from the cached chunkmap ``(offset, size)`` entry, where
        ``size`` is the chunk length excluding the 16-byte header. Returns
        ``None`` when the chunkmap stores the unknown sentinel (negative size),
        in which case callers read the header first to compute the exact
        length. Using this exact length replaces the old over-reading
        ``16 + 8192 + frame_bytes + 1024`` estimate (finding I2).
        """
        _, size = self._chunkmap[chunk_key]
        if size is None or size < 0:
            return None
        return 16 + int(size)

    def read_zstack(
        self,
        t: int = 0,
        c: int = 0,
        s: int = 0,
        max_workers: int = 32,
        use_multiprocessing: bool = False,
        use_pipeline: bool = False,
        io_workers: int | None = None,
        decode_workers: int | None = None,
        crop: tuple[int, int, int, int, int, int] | None = None,
    ) -> np.ndarray:
        """Read a complete Z-stack (all Z slices for given T, C, S).

        This is the primary method for raw extraction workflows.

        For remote files: Uses parallel HTTP byte-range requests to saturate network bandwidth.
        For local files: Uses parallel file reads with seek/read operations.
        Each Z-slice is fetched independently, using connection/thread pooling for efficiency.

        Args:
            t: Timepoint index (0-based)
            c: Channel index (0-based)
            s: Scene/position index (0-based)
            max_workers: Maximum parallel connections/threads (default: 32)
            use_multiprocessing: If True, use ProcessPoolExecutor to bypass GIL
                and achieve true parallel decompression. Default False.
            use_pipeline: If True, use decoupled I/O + decode pipeline architecture.
                I/O threads feed raw bytes to decode processes via queue.
                This keeps both network and CPU saturated. Default False.
            io_workers: Number of I/O threads for pipeline mode (default: max_workers)
            decode_workers: Number of decode processes for pipeline mode (default: cpu_count)
            crop: Optional 3D crop region as (z_min, z_max, y_min, y_max, x_min, x_max).
                Only fetches Z slices within [z_min, z_max) and crops Y/X after decode.
                This reduces I/O for remote files by skipping unnecessary Z slices.

        Returns:
            3D numpy array of shape (Z, Y, X) or cropped shape if crop is specified
        """
        if use_pipeline:
            return self._read_zstack_pipeline(t, c, s, max_workers, io_workers, decode_workers)

        if self._is_local:
            if use_multiprocessing:
                return self._read_zstack_local_mp(t, c, s, max_workers, crop=crop)
            return self._read_zstack_local(t, c, s, max_workers, crop=crop)
        else:
            if use_multiprocessing:
                return self._read_zstack_remote_mp(t, c, s, max_workers, crop=crop)
            return self._read_zstack_remote(t, c, s, max_workers, crop=crop)

    def read_zstack_multichannel(
        self,
        t: int,
        channels: list[int],
        s: int = 0,
        max_workers: int = 32,
        crop: tuple[int, int, int, int, int, int] | None = None,
    ) -> dict[int, np.ndarray]:
        """Read a Z-stack and extract multiple channels in a single pass.

        For interleaved ND2 data, each frame contains ALL channels. Reading
        channels separately re-reads identical bytes for each channel. This
        method reads each frame once and extracts all requested channels,
        eliminating N-1 redundant reads (where N = number of channels).

        For separate-frame ND2 data, falls back to individual reads.

        Args:
            t: Timepoint index (0-based)
            channels: List of channel indices to extract (0-based)
            s: Scene/position index (0-based)
            max_workers: Maximum parallel threads
            crop: Optional (z_min, z_max, y_min, y_max, x_min, x_max)

        Returns:
            Dict mapping channel index -> 3D numpy array of shape (Z, Y, X)
        """
        if not self._channels_interleaved or len(channels) <= 1:
            # No benefit from batching: separate frames or single channel
            result = {}
            for c in channels:
                result[c] = self.read_zstack(t, c, s, max_workers, crop=crop)
            return result

        if not self._is_local:
            # Remote interleaved data: fetch each frame once over the network and
            # extract every requested channel, instead of re-fetching the same
            # bytes per channel via read_zstack (code-review finding I3).
            return self._read_zstack_remote_multichannel(t, channels, s, max_workers, crop=crop)

        return self._read_zstack_local_multichannel(t, channels, s, max_workers, crop=crop)

    def _read_zstack_local_multichannel(
        self,
        t: int,
        channels: list[int],
        s: int,
        max_workers: int,
        crop: tuple[int, int, int, int, int, int] | None = None,
    ) -> dict[int, np.ndarray]:
        """Read Z-stack from local file extracting multiple channels per frame read.

        Since interleaved ND2 frames contain all channels, this reads each
        frame ONCE and extracts all requested channels, avoiding redundant I/O.
        """
        import concurrent.futures
        import threading
        import time

        start_time = time.time()

        # Parse crop parameters
        if crop is not None:
            z_min, z_max, y_min, y_max, x_min, x_max = crop
            z_min = max(0, z_min)
            z_max = min(self.num_z, z_max)
            y_min = max(0, y_min)
            y_max = min(self.height, y_max)
            x_min = max(0, x_min)
            x_max = min(self.width, x_max)
            z_range = range(z_min, z_max)
            output_z_size = z_max - z_min
            output_y_size = y_max - y_min
            output_x_size = x_max - x_min
            crop_info = f", CROP Z[{z_min}:{z_max}] Y[{y_min}:{y_max}] X[{x_min}:{x_max}]"
        else:
            z_range = range(self.num_z)
            z_min, z_max = 0, self.num_z
            y_min, y_max = 0, self.height
            x_min, x_max = 0, self.width
            output_z_size = self.num_z
            output_y_size = self.height
            output_x_size = self.width
            crop_info = ""

        # Resolve component indices for each channel
        comp_indices = {}
        ch_names = []
        for c in channels:
            comp_indices[c] = self._component_for_channel(c)
            ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
            ch_names.append(f"C{c}({ch_name})")

        logger.info(
            f"read_zstack_multichannel (local): T={t}, channels=[{','.join(ch_names)}], S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z){crop_info}"
        )

        # Get chunk offsets - frames are the same for all channels (interleaved)
        # Use c=0 for frame index since interleaved mode ignores channel
        chunk_info: list[tuple[int, int, int]] = []  # (output_z_idx, file_z_idx, file_offset)
        for output_z_idx, file_z_idx in enumerate(z_range):
            frame_idx = self._get_frame_index(t, 0, file_z_idx, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, z={file_z_idx}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((output_z_idx, file_z_idx, offset, self._chunk_total_bytes(chunk_key)))

        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (len(chunk_info) * single_channel_frame_bytes * len(channels)) / (1024 * 1024)
        logger.info(
            f"Fetching T={t} [{','.join(ch_names)}] S={s}: {len(chunk_info)} Z-slices, "
            f"{data_size_mb:.1f} MB across {len(channels)} channels with {max_workers} threads "
            f"(single-pass interleaved read, {len(channels)}x I/O savings)"
        )

        # Thread-local file handles (tracked so they can be closed — I11)
        thread_local = threading.local()
        open_handles: list[Any] = []
        open_handles_lock = threading.Lock()
        raw_bytes_total = [0]

        def get_file_handle():
            if not hasattr(thread_local, "file"):
                thread_local.file = open(self.url, "rb")
                with open_handles_lock:
                    open_handles.append(thread_local.file)
            return thread_local.file

        def read_and_decode_chunk_multi(z_info: tuple[int, int, int]) -> tuple[int, dict[int, np.ndarray]]:
            """Read single frame and extract all requested channels."""
            output_z_idx, file_z_idx, offset, known_total = z_info
            f = get_file_handle()

            chunk_bytes = _read_chunk_from_handle(f, offset, known_total)
            raw_bytes_total[0] += len(chunk_bytes)

            if len(chunk_bytes) < 16:
                raise ValueError(f"Chunk too small: {len(chunk_bytes)} bytes")

            signature = struct.unpack("<I", chunk_bytes[0:4])[0]
            if signature != 0x0ABECEDA:
                raise ValueError(f"Invalid ND2 signature at Z={file_z_idx}: {hex(signature)}")

            name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
            data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
            data_start = 16 + name_length
            raw_bytes = chunk_bytes[data_start : data_start + data_length]

            frame = self._decode_frame(raw_bytes)

            # Extract all requested channels from the interleaved frame
            channel_frames = {}
            for c in channels:
                comp_idx = comp_indices[c]
                ch_frame = frame[:, :, comp_idx].copy()
                if crop is not None:
                    ch_frame = ch_frame[y_min:y_max, x_min:x_max].copy()
                channel_frames[c] = ch_frame

            return (output_z_idx, channel_frames)

        # Pre-allocate output arrays for each channel
        outputs: dict[int, np.ndarray] = {}
        for c in channels:
            outputs[c] = np.empty((output_z_size, output_y_size, output_x_size), dtype=self.dtype)

        # Parallel read all Z-slices; close thread-local handles after the
        # executor shuts down so worker FDs don't leak (I11).
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(read_and_decode_chunk_multi, zi): zi[0] for zi in chunk_info}

                for future in concurrent.futures.as_completed(futures):
                    z_idx, channel_frames = future.result()
                    for c, frame in channel_frames.items():
                        outputs[c][z_idx] = frame
        finally:
            for fh in open_handles:
                try:
                    fh.close()
                except Exception:
                    pass

        elapsed = time.time() - start_time
        raw_bytes_mb = raw_bytes_total[0] / (1024 * 1024)
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0
        raw_throughput_mbps = (raw_bytes_mb * 8) / elapsed if elapsed > 0 else 0

        logger.info(
            f"Z-stack multichannel read complete (local): T={t} [{','.join(ch_names)}] S={s}, "
            f"{data_size_mb:.1f} MB extracted ({raw_bytes_mb:.1f} MB raw) in {elapsed:.1f}s "
            f"({throughput_mbps:.0f} Mbit/s effective, {raw_throughput_mbps:.0f} Mbit/s raw), "
            f"{len(channels)}x I/O savings vs separate reads"
        )

        return outputs

    def _read_zstack_remote_multichannel(
        self,
        t: int,
        channels: list[int],
        s: int,
        max_workers: int,
        crop: tuple[int, int, int, int, int, int] | None = None,
    ) -> dict[int, np.ndarray]:
        """Read a remote Z-stack extracting multiple channels per frame fetch.

        Mirrors :meth:`_read_zstack_local_multichannel` for remote URLs: each
        interleaved frame is fetched exactly once via the fsspec-aware byte-range
        helper (so ``s3://`` works — I1) using the cached chunk length (I2), then
        every requested channel is extracted from that single download. This
        replaces the previous per-channel ``read_zstack`` fallback that fetched
        the same bytes N times (code-review finding I3).
        """
        import concurrent.futures
        import time

        start_time = time.time()

        # Parse crop parameters
        if crop is not None:
            z_min, z_max, y_min, y_max, x_min, x_max = crop
            z_min = max(0, z_min)
            z_max = min(self.num_z, z_max)
            y_min = max(0, y_min)
            y_max = min(self.height, y_max)
            x_min = max(0, x_min)
            x_max = min(self.width, x_max)
            z_range = range(z_min, z_max)
            output_z_size = z_max - z_min
            output_y_size = y_max - y_min
            output_x_size = x_max - x_min
            crop_info = f", CROP Z[{z_min}:{z_max}] Y[{y_min}:{y_max}] X[{x_min}:{x_max}]"
        else:
            z_range = range(self.num_z)
            z_min, z_max = 0, self.num_z
            y_min, y_max = 0, self.height
            x_min, x_max = 0, self.width
            output_z_size = self.num_z
            output_y_size = self.height
            output_x_size = self.width
            crop_info = ""

        # Resolve component indices for each channel
        comp_indices = {}
        ch_names = []
        for c in channels:
            comp_indices[c] = self._component_for_channel(c)
            ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
            ch_names.append(f"C{c}({ch_name})")

        logger.info(
            f"read_zstack_multichannel (remote): T={t}, channels=[{','.join(ch_names)}], S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z){crop_info}"
        )

        # Frames are shared across channels for interleaved data; use c=0.
        chunk_info: list[tuple[int, int, int, int | None]] = []
        for output_z_idx, file_z_idx in enumerate(z_range):
            frame_idx = self._get_frame_index(t, 0, file_z_idx, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, z={file_z_idx}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((output_z_idx, file_z_idx, offset, self._chunk_total_bytes(chunk_key)))

        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (len(chunk_info) * single_channel_frame_bytes * len(channels)) / (1024 * 1024)
        logger.info(
            f"Fetching T={t} [{','.join(ch_names)}] S={s}: {len(chunk_info)} Z-slices, "
            f"{data_size_mb:.1f} MB across {len(channels)} channels with {max_workers} connections "
            f"(single-pass interleaved read, {len(channels)}x I/O savings)"
        )

        raw_bytes_total = [0]

        def fetch_and_decode_chunk_multi(
            z_info: tuple[int, int, int, int | None],
        ) -> tuple[int, dict[int, np.ndarray]]:
            """Fetch one frame over the network and extract all channels."""
            output_z_idx, file_z_idx, offset, known_total = z_info

            # Single fsspec-aware fetch of the exact chunk length (I1/I2).
            chunk_bytes = _fetch_chunk_remote(self.url, offset, known_total)
            raw_bytes_total[0] += len(chunk_bytes)

            if len(chunk_bytes) < 16:
                raise ValueError(f"Chunk too small: {len(chunk_bytes)} bytes")

            signature = struct.unpack("<I", chunk_bytes[0:4])[0]
            if signature != 0x0ABECEDA:
                raise ValueError(f"Invalid ND2 signature at Z={file_z_idx}: {hex(signature)}")

            name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
            data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
            data_start = 16 + name_length
            raw_bytes = chunk_bytes[data_start : data_start + data_length]

            frame = self._decode_frame(raw_bytes)

            channel_frames = {}
            for c in channels:
                comp_idx = comp_indices[c]
                ch_frame = frame[:, :, comp_idx].copy()
                if crop is not None:
                    ch_frame = ch_frame[y_min:y_max, x_min:x_max].copy()
                channel_frames[c] = ch_frame

            return (output_z_idx, channel_frames)

        outputs: dict[int, np.ndarray] = {}
        for c in channels:
            outputs[c] = np.empty((output_z_size, output_y_size, output_x_size), dtype=self.dtype)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_and_decode_chunk_multi, zi): zi[0] for zi in chunk_info}

            for future in concurrent.futures.as_completed(futures):
                z_idx, channel_frames = future.result()
                for c, frame in channel_frames.items():
                    outputs[c][z_idx] = frame

        elapsed = time.time() - start_time
        raw_bytes_mb = raw_bytes_total[0] / (1024 * 1024)
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0
        raw_throughput_mbps = (raw_bytes_mb * 8) / elapsed if elapsed > 0 else 0

        logger.info(
            f"Z-stack multichannel read complete (remote): T={t} [{','.join(ch_names)}] S={s}, "
            f"{data_size_mb:.1f} MB extracted ({raw_bytes_mb:.1f} MB raw) in {elapsed:.1f}s "
            f"({throughput_mbps:.0f} Mbit/s effective, {raw_throughput_mbps:.0f} Mbit/s raw), "
            f"{len(channels)}x I/O savings vs separate reads"
        )

        return outputs

    def _read_zstack_local(
        self, t: int, c: int, s: int, max_workers: int, crop: tuple[int, int, int, int, int, int] | None = None
    ) -> np.ndarray:
        """Read Z-stack from local/network file using parallel file reads.

        Args:
            crop: Optional (z_min, z_max, y_min, y_max, x_min, x_max) to read only a subregion.
                  Only Z slices in [z_min, z_max) are fetched, Y/X are cropped after decode.
        """
        import concurrent.futures
        import threading
        import time

        start_time = time.time()

        # Parse crop parameters
        if crop is not None:
            z_min, z_max, y_min, y_max, x_min, x_max = crop
            # Clamp to valid ranges
            z_min = max(0, z_min)
            z_max = min(self.num_z, z_max)
            y_min = max(0, y_min)
            y_max = min(self.height, y_max)
            x_min = max(0, x_min)
            x_max = min(self.width, x_max)
            z_range = range(z_min, z_max)
            output_z_size = z_max - z_min
            output_y_size = y_max - y_min
            output_x_size = x_max - x_min
            crop_info = f", CROP Z[{z_min}:{z_max}] Y[{y_min}:{y_max}] X[{x_min}:{x_max}]"
        else:
            z_range = range(self.num_z)
            z_min, z_max = 0, self.num_z
            y_min, y_max = 0, self.height
            x_min, x_max = 0, self.width
            output_z_size = self.num_z
            output_y_size = self.height
            output_x_size = self.width
            crop_info = ""

        # Log exactly which T/C/S is being requested
        logger.info(
            f"read_zstack (local): T={t}, C={c}, S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z){crop_info}"
        )

        # Get chunk offsets only for Z slices we need
        chunk_info: list[tuple[int, int, int]] = []  # (output_z_idx, file_z_idx, file_offset)
        for output_z_idx, file_z_idx in enumerate(z_range):
            frame_idx = self._get_frame_index(t, c, file_z_idx, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, c={c}, z={file_z_idx}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((output_z_idx, file_z_idx, offset, self._chunk_total_bytes(chunk_key)))

        # Calculate expected data size (for the slices we're actually fetching)
        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (len(chunk_info) * single_channel_frame_bytes) / (1024 * 1024)
        channel_mode = "interleaved" if self._channels_interleaved else "separate_frames"
        if self._channels_interleaved and self._channel_to_component and c < len(self._channel_to_component):
            comp_idx = self._channel_to_component[c]
            ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
            comp_info = f", extracting {ch_name} from component {comp_idx}"
        else:
            comp_info = ""
        logger.info(
            f"Fetching T={t} C={c} S={s}: {len(chunk_info)} Z-slices (of {self.num_z}), "
            f"{data_size_mb:.1f} MB with {max_workers} parallel threads "
            f"(channel_mode={channel_mode}{comp_info})"
        )

        # Thread-local file handles for parallel reads (tracked for close — I11)
        thread_local = threading.local()
        open_handles: list[Any] = []
        open_handles_lock = threading.Lock()
        raw_bytes_total = [0]  # Use list to allow modification in nested function

        def get_file_handle():
            """Get thread-local file handle."""
            if not hasattr(thread_local, "file"):
                thread_local.file = open(self.url, "rb")
                with open_handles_lock:
                    open_handles.append(thread_local.file)
            return thread_local.file

        def read_and_decode_chunk(z_info: tuple[int, int, int]) -> tuple[int, np.ndarray]:
            """Read single chunk from local file and decode."""
            output_z_idx, file_z_idx, offset, known_total = z_info
            f = get_file_handle()

            # Seek and read chunk
            chunk_bytes = _read_chunk_from_handle(f, offset, known_total)
            raw_bytes_total[0] += len(chunk_bytes)

            # Parse ND2 chunk header
            if len(chunk_bytes) < 16:
                raise ValueError(f"Chunk too small: {len(chunk_bytes)} bytes")

            signature = struct.unpack("<I", chunk_bytes[0:4])[0]
            if signature != 0x0ABECEDA:
                raise ValueError(f"Invalid ND2 signature at Z={file_z_idx}: {hex(signature)}")

            name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
            data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
            data_start = 16 + name_length
            raw_bytes = chunk_bytes[data_start : data_start + data_length]

            # Decode frame
            frame = self._decode_frame(raw_bytes)

            # Extract single channel if interleaved
            if self._channels_interleaved and frame.ndim == 3:
                comp_idx = self._component_for_channel(c)

                # Extract the correct component (channel mapping is verified during initialization)
                frame = frame[:, :, comp_idx].copy()  # Copy to release reference to full frame

            # Apply Y/X cropping after decode
            if crop is not None:
                frame = frame[y_min:y_max, x_min:x_max].copy()

            return (output_z_idx, frame)

        # Pre-allocate output array with cropped dimensions
        output = np.empty((output_z_size, output_y_size, output_x_size), dtype=self.dtype)

        # Parallel read all Z-slices; close thread-local handles after the
        # executor shuts down so worker FDs don't leak (I11).
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(read_and_decode_chunk, zi): zi[0] for zi in chunk_info}

                for future in concurrent.futures.as_completed(futures):
                    z_idx, frame = future.result()
                    output[z_idx] = frame
        finally:
            for fh in open_handles:
                try:
                    fh.close()
                except Exception:
                    pass

        elapsed = time.time() - start_time
        raw_bytes_mb = raw_bytes_total[0] / (1024 * 1024)
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0
        raw_throughput_mbps = (raw_bytes_mb * 8) / elapsed if elapsed > 0 else 0

        # Sample middle Z-slice for median (much faster than full volume)
        mid_z = output_z_size // 2
        output_median = float(np.median(output[mid_z]))
        ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
        logger.info(
            f"Z-stack read complete (local): T={t} C={c}({ch_name}) S={s}, "
            f"{data_size_mb:.1f} MB decompressed ({raw_bytes_mb:.1f} MB raw) in {elapsed:.1f}s "
            f"({throughput_mbps:.0f} Mbit/s effective, {raw_throughput_mbps:.0f} Mbit/s raw), median={output_median:.1f}"
        )

        self._warn_channel_mismatch(ch_name, output_median)
        return output

    def _read_zstack_local_mp(
        self, t: int, c: int, s: int, max_workers: int, crop: tuple[int, int, int, int, int, int] | None = None
    ) -> np.ndarray:
        """Read Z-stack using ProcessPoolExecutor for true parallel decompression.

        This bypasses Python's GIL by using separate processes, allowing
        zlib decompression to run in true parallel. Best for saturating
        high-bandwidth network storage.

        Args:
            crop: Optional (z_min, z_max, y_min, y_max, x_min, x_max) to read only a subregion.
        """
        import time
        from concurrent.futures import ProcessPoolExecutor

        start_time = time.time()

        # Parse crop parameters
        if crop is not None:
            z_min, z_max, y_min, y_max, x_min, x_max = crop
            # Clamp to valid ranges
            z_min = max(0, z_min)
            z_max = min(self.num_z, z_max)
            y_min = max(0, y_min)
            y_max = min(self.height, y_max)
            x_min = max(0, x_min)
            x_max = min(self.width, x_max)
            z_range = range(z_min, z_max)
            output_z_size = z_max - z_min
            output_y_size = y_max - y_min
            output_x_size = x_max - x_min
            crop_yx = (y_min, y_max, x_min, x_max)
            crop_info = f", CROP Z[{z_min}:{z_max}] Y[{y_min}:{y_max}] X[{x_min}:{x_max}]"
        else:
            z_range = range(self.num_z)
            output_z_size = self.num_z
            output_y_size = self.height
            output_x_size = self.width
            crop_yx = None
            crop_info = ""

        logger.info(
            f"read_zstack (local, multiprocessing): T={t}, C={c}, S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z){crop_info}"
        )

        # Get chunk offsets only for Z slices we need
        chunk_info: list[tuple[int, int, int]] = []  # (output_z_idx, file_z_idx, file_offset)
        for output_z_idx, file_z_idx in enumerate(z_range):
            frame_idx = self._get_frame_index(t, c, file_z_idx, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, c={c}, z={file_z_idx}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((output_z_idx, file_z_idx, offset, self._chunk_total_bytes(chunk_key)))

        # Calculate sizes
        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (len(chunk_info) * single_channel_frame_bytes) / (1024 * 1024)
        logger.info(
            f"Fetching T={t} C={c} S={s}: {len(chunk_info)} Z-slices (of {self.num_z}), "
            f"{data_size_mb:.1f} MB with {max_workers} parallel PROCESSES"
        )

        # Prepare args for module-level function (must be picklable)
        mp_args = [
            (
                output_z_idx,
                self.url,
                offset,
                known_total,
                self.height,
                self.width,
                str(self.dtype),
                self.component_count,
                self._channels_interleaved,
                self._channel_to_component,
                c,
                crop_yx,
            )
            for output_z_idx, file_z_idx, offset, known_total in chunk_info
        ]

        # Pre-allocate output with cropped dimensions
        output = np.empty((output_z_size, output_y_size, output_x_size), dtype=self.dtype)

        # Use ProcessPoolExecutor for true parallel decompression
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for z_idx, frame in executor.map(_read_and_decode_chunk_mp, mp_args):
                output[z_idx] = frame

        elapsed = time.time() - start_time
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0

        # Sample middle Z-slice for median (much faster than full volume)
        mid_z = output_z_size // 2
        output_median = float(np.median(output[mid_z]))
        ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
        logger.info(
            f"Z-stack read complete (local, MP): T={t} C={c}({ch_name}) S={s}, "
            f"{data_size_mb:.1f} MB in {elapsed:.1f}s ({throughput_mbps:.0f} Mbit/s effective), "
            f"median={output_median:.1f}"
        )

        self._warn_channel_mismatch(ch_name, output_median)
        return output

    def _read_zstack_remote_mp(
        self, t: int, c: int, s: int, max_workers: int, crop: tuple[int, int, int, int, int, int] | None = None
    ) -> np.ndarray:
        """Read Z-stack from remote URL using ProcessPoolExecutor.

        Uses separate processes for HTTP fetches and zlib decompression,
        bypassing Python's GIL for true parallel I/O and decompression.
        Each process creates its own HTTP connection.

        Args:
            crop: Optional (z_min, z_max, y_min, y_max, x_min, x_max) to read only a subregion.
        """
        import time
        from concurrent.futures import ProcessPoolExecutor

        start_time = time.time()

        # Parse crop parameters
        if crop is not None:
            z_min, z_max, y_min, y_max, x_min, x_max = crop
            # Clamp to valid ranges
            z_min = max(0, z_min)
            z_max = min(self.num_z, z_max)
            y_min = max(0, y_min)
            y_max = min(self.height, y_max)
            x_min = max(0, x_min)
            x_max = min(self.width, x_max)
            z_range = range(z_min, z_max)
            output_z_size = z_max - z_min
            output_y_size = y_max - y_min
            output_x_size = x_max - x_min
            crop_yx = (y_min, y_max, x_min, x_max)
            crop_info = f", CROP Z[{z_min}:{z_max}] Y[{y_min}:{y_max}] X[{x_min}:{x_max}]"
        else:
            z_range = range(self.num_z)
            output_z_size = self.num_z
            output_y_size = self.height
            output_x_size = self.width
            crop_yx = None
            crop_info = ""

        logger.info(
            f"read_zstack (remote, multiprocessing): T={t}, C={c}, S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z){crop_info}"
        )

        # Get chunk offsets only for Z slices we need
        chunk_info: list[tuple[int, int, int]] = []  # (output_z_idx, file_z_idx, file_offset)
        for output_z_idx, file_z_idx in enumerate(z_range):
            frame_idx = self._get_frame_index(t, c, file_z_idx, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, c={c}, z={file_z_idx}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((output_z_idx, file_z_idx, offset, self._chunk_total_bytes(chunk_key)))

        # Calculate sizes
        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (len(chunk_info) * single_channel_frame_bytes) / (1024 * 1024)
        logger.info(
            f"Fetching T={t} C={c} S={s}: {len(chunk_info)} Z-slices (of {self.num_z}), "
            f"{data_size_mb:.1f} MB with {max_workers} parallel PROCESSES (remote)"
        )

        # Prepare args for module-level function (must be picklable)
        # Each process will create its own HTTP connection
        mp_args = [
            (
                output_z_idx,
                self.url,
                offset,
                known_total,
                self.height,
                self.width,
                str(self.dtype),
                self.component_count,
                self._channels_interleaved,
                self._channel_to_component,
                c,
                crop_yx,
            )
            for output_z_idx, file_z_idx, offset, known_total in chunk_info
        ]

        # Pre-allocate output with cropped dimensions
        output = np.empty((output_z_size, output_y_size, output_x_size), dtype=self.dtype)

        # Use ProcessPoolExecutor for true parallel HTTP fetch + decompression
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for z_idx, frame in executor.map(_fetch_and_decode_chunk_mp, mp_args):
                output[z_idx] = frame

        elapsed = time.time() - start_time
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0

        mid_z = output_z_size // 2
        output_median = float(np.median(output[mid_z]))
        ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
        logger.info(
            f"Z-stack read complete (remote, MP): T={t} C={c}({ch_name}) S={s}, "
            f"{data_size_mb:.1f} MB in {elapsed:.1f}s ({throughput_mbps:.0f} Mbit/s effective), "
            f"median={output_median:.1f}"
        )

        self._warn_channel_mismatch(ch_name, output_median)
        return output

    def _read_zstack_pipeline(
        self, t: int, c: int, s: int, max_workers: int, io_workers: int | None = None, decode_workers: int | None = None
    ) -> np.ndarray:
        """Read Z-stack using decoupled I/O + decode pipeline.

        Architecture:
        - ThreadPoolExecutor for I/O (GIL released during I/O, threads are efficient)
        - ProcessPoolExecutor for decode (CPU-bound, bypasses GIL)
        - Queue buffers raw bytes between I/O and decode stages

        This keeps both network and CPU continuously saturated rather than
        alternating between I/O and decode phases.
        """
        import os
        import queue
        import threading
        import time
        from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

        start_time = time.time()

        # Default worker counts
        if io_workers is None:
            io_workers = max_workers
        if decode_workers is None:
            decode_workers = os.cpu_count() or 8

        logger.info(
            f"read_zstack (pipeline): T={t}, C={c}, S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z)"
        )
        logger.info(f"Pipeline config: {io_workers} I/O threads -> queue -> {decode_workers} decode processes")

        # Get all chunk offsets (with exact chunk lengths from the chunkmap)
        chunk_info: list[tuple[int, int, int | None]] = []
        for z in range(self.num_z):
            frame_idx = self._get_frame_index(t, c, z, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, c={c}, z={z}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((z, offset, self._chunk_total_bytes(chunk_key)))

        # Calculate sizes
        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (self.num_z * single_channel_frame_bytes) / (1024 * 1024)

        # Queue to buffer raw bytes between I/O and decode stages
        # Size limit prevents memory bloat if I/O is faster than decode
        raw_queue: queue.Queue = queue.Queue(maxsize=decode_workers * 2)
        io_done = threading.Event()
        io_errors: list[Exception] = []
        raw_bytes_total = [0]

        # I/O function (runs in threads)
        open_handles: list[Any] = []
        open_handles_lock = threading.Lock()
        if self._is_local:
            # Local file I/O (handles tracked for close — I11)
            thread_local = threading.local()

            def get_file():
                if not hasattr(thread_local, "f"):
                    thread_local.f = open(self.url, "rb")
                    with open_handles_lock:
                        open_handles.append(thread_local.f)
                return thread_local.f

            def fetch_chunk(z_offset: tuple[int, int, int | None]) -> None:
                z_idx, offset, known_total = z_offset
                try:
                    f = get_file()
                    chunk_bytes = _read_chunk_from_handle(f, offset, known_total)
                    raw_bytes_total[0] += len(chunk_bytes)
                    raw_queue.put((z_idx, chunk_bytes))
                except Exception as e:
                    io_errors.append(e)
        else:

            def fetch_chunk(z_offset: tuple[int, int, int | None]) -> None:
                z_idx, offset, known_total = z_offset
                try:
                    # fsspec-aware exact-length read: s3:// works (I1), no over-read (I2).
                    chunk_bytes = _fetch_chunk_remote(self.url, offset, known_total)
                    raw_bytes_total[0] += len(chunk_bytes)
                    raw_queue.put((z_idx, chunk_bytes))
                except Exception as e:
                    io_errors.append(e)

        # Pre-allocate output
        output = np.empty((self.num_z, self.height, self.width), dtype=self.dtype)

        # Decode params (must be picklable for ProcessPoolExecutor)
        decode_params = (
            self.height,
            self.width,
            str(self.dtype),
            self.component_count,
            self._channels_interleaved,
            self._channel_to_component,
            c,
        )

        def io_producer():
            """Thread that runs I/O pool and signals when done."""
            try:
                with ThreadPoolExecutor(max_workers=io_workers) as io_executor:
                    list(io_executor.map(fetch_chunk, chunk_info))
            finally:
                # Close thread-local handles after executor shutdown (I11).
                for fh in open_handles:
                    try:
                        fh.close()
                    except Exception:
                        pass
                io_done.set()

        def decode_consumer():
            """Process that decodes raw bytes from queue."""
            decoded_count = 0
            with ProcessPoolExecutor(max_workers=decode_workers) as decode_executor:
                pending_decodes = []

                while True:
                    # Check if we're done
                    if io_done.is_set() and raw_queue.empty() and not pending_decodes:
                        break

                    # Collect batch of raw chunks for decode
                    batch = []
                    try:
                        while len(batch) < decode_workers:
                            z_idx, chunk_bytes = raw_queue.get(timeout=0.01)
                            batch.append((z_idx, chunk_bytes, *decode_params))
                    except queue.Empty:
                        pass

                    # Submit batch to decode pool
                    if batch:
                        futures = [decode_executor.submit(_decode_chunk_only, args) for args in batch]
                        pending_decodes.extend(futures)

                    # Collect completed decodes
                    still_pending = []
                    for future in pending_decodes:
                        if future.done():
                            z_idx, frame = future.result()
                            output[z_idx] = frame
                            decoded_count += 1
                        else:
                            still_pending.append(future)
                    pending_decodes = still_pending

                # Wait for any remaining decodes
                for future in pending_decodes:
                    z_idx, frame = future.result()
                    output[z_idx] = frame
                    decoded_count += 1

        # Start I/O producer thread
        io_thread = threading.Thread(target=io_producer)
        io_thread.start()

        # Run decode consumer in main thread
        decode_consumer()

        # Wait for I/O to finish
        io_thread.join()

        # Check for I/O errors
        if io_errors:
            raise io_errors[0]

        # Don't close session - it's shared via _get_http_session() for remote files

        elapsed = time.time() - start_time
        raw_bytes_mb = raw_bytes_total[0] / (1024 * 1024)
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0
        raw_throughput_mbps = (raw_bytes_mb * 8) / elapsed if elapsed > 0 else 0

        # Sample middle Z-slice for median (much faster than full volume)
        mid_z = self.num_z // 2
        output_median = float(np.median(output[mid_z]))
        ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
        logger.info(
            f"Z-stack read complete (pipeline): T={t} C={c}({ch_name}) S={s}, "
            f"{data_size_mb:.1f} MB decompressed ({raw_bytes_mb:.1f} MB raw) in {elapsed:.1f}s "
            f"({throughput_mbps:.0f} Mbit/s effective, {raw_throughput_mbps:.0f} Mbit/s raw), "
            f"median={output_median:.1f}"
        )

        self._warn_channel_mismatch(ch_name, output_median)
        return output

    def _read_zstack_remote(
        self, t: int, c: int, s: int, max_workers: int, crop: tuple[int, int, int, int, int, int] | None = None
    ) -> np.ndarray:
        """Read Z-stack from remote URL using parallel HTTP byte-range requests.

        Args:
            crop: Optional (z_min, z_max, y_min, y_max, x_min, x_max) to read only a subregion.
                  Only Z slices in [z_min, z_max) are fetched, Y/X are cropped after decode.
        """
        import concurrent.futures
        import time

        start_time = time.time()

        # Parse crop parameters
        if crop is not None:
            z_min, z_max, y_min, y_max, x_min, x_max = crop
            # Clamp to valid ranges
            z_min = max(0, z_min)
            z_max = min(self.num_z, z_max)
            y_min = max(0, y_min)
            y_max = min(self.height, y_max)
            x_min = max(0, x_min)
            x_max = min(self.width, x_max)
            z_range = range(z_min, z_max)
            output_z_size = z_max - z_min
            output_y_size = y_max - y_min
            output_x_size = x_max - x_min
            crop_info = f", CROP Z[{z_min}:{z_max}] Y[{y_min}:{y_max}] X[{x_min}:{x_max}]"
        else:
            z_range = range(self.num_z)
            z_min, z_max = 0, self.num_z
            y_min, y_max = 0, self.height
            x_min, x_max = 0, self.width
            output_z_size = self.num_z
            output_y_size = self.height
            output_x_size = self.width
            crop_info = ""

        # Log exactly which T/C/S is being requested
        logger.info(
            f"read_zstack (remote): T={t}, C={c}, S={s} "
            f"(file has {self.num_timepoints}T x {self.num_channels}C x {self.num_z}Z){crop_info}"
        )

        # Get chunk offsets only for Z slices we need (from cached chunkmap - no HTTP!)
        chunk_info: list[tuple[int, int, int]] = []  # (output_z_idx, file_z_idx, file_offset)
        for output_z_idx, file_z_idx in enumerate(z_range):
            frame_idx = self._get_frame_index(t, c, file_z_idx, s)
            chunk_key = f"ImageDataSeq|{frame_idx}!".encode()
            if chunk_key not in self._chunkmap:
                raise KeyError(f"Frame not found: t={t}, c={c}, z={file_z_idx}, s={s}")
            offset, _ = self._chunkmap[chunk_key]
            chunk_info.append((output_z_idx, file_z_idx, offset, self._chunk_total_bytes(chunk_key)))

        # Calculate expected data size based on channel storage mode
        single_channel_frame_bytes = self.height * self.width * self.dtype.itemsize
        data_size_mb = (len(chunk_info) * single_channel_frame_bytes) / (1024 * 1024)

        channel_mode = "interleaved" if self._channels_interleaved else "separate_frames"
        # Log component mapping for debugging
        if self._channels_interleaved and self._channel_to_component and c < len(self._channel_to_component):
            comp_idx = self._channel_to_component[c]
            ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
            comp_info = f", extracting {ch_name} from component {comp_idx}"
        else:
            comp_info = ""
        logger.info(
            f"Fetching T={t} C={c} S={s}: {len(chunk_info)} Z-slices (of {self.num_z}), "
            f"{data_size_mb:.1f} MB with {max_workers} parallel connections "
            f"(channel_mode={channel_mode}, component_count={self.component_count}{comp_info})"
        )

        raw_bytes_total = [0]  # Track compressed bytes downloaded

        def fetch_and_decode_chunk(z_info: tuple[int, int, int, int | None]) -> tuple[int, np.ndarray]:
            """Fetch single chunk via fsspec-aware byte-range read and decode."""
            output_z_idx, file_z_idx, offset, known_total = z_info

            # Fetch exactly one chunk. _fetch_chunk_remote routes s3:// through
            # s3fs (I1) and reads the exact chunk length from the cached
            # chunkmap (or a header probe) rather than an over-read estimate (I2).
            chunk_bytes = _fetch_chunk_remote(self.url, offset, known_total)
            raw_bytes_total[0] += len(chunk_bytes)

            # Parse ND2 chunk header:
            # - 4 bytes: signature (0x0ABECEDA)
            # - 4 bytes: name_length
            # - 8 bytes: data_length
            # - name_length bytes: name
            # - data_length bytes: data
            if len(chunk_bytes) < 16:
                raise ValueError(f"Chunk too small: {len(chunk_bytes)} bytes")

            signature = struct.unpack("<I", chunk_bytes[0:4])[0]
            if signature != 0x0ABECEDA:
                raise ValueError(f"Invalid ND2 signature at Z={file_z_idx}: {hex(signature)}")

            name_length = struct.unpack("<I", chunk_bytes[4:8])[0]
            data_length = struct.unpack("<Q", chunk_bytes[8:16])[0]
            data_start = 16 + name_length
            raw_bytes = chunk_bytes[data_start : data_start + data_length]

            # Decode frame
            frame = self._decode_frame(raw_bytes)

            # Extract single channel if channels are interleaved in frame
            if self._channels_interleaved and frame.ndim == 3:
                # Map channel index to component index (they may differ)
                comp_idx = self._component_for_channel(c)

                # Extract the correct component (channel mapping is verified during initialization)
                frame = frame[:, :, comp_idx].copy()  # Copy to release reference to full frame

            # Apply Y/X cropping after decode
            if crop is not None:
                frame = frame[y_min:y_max, x_min:x_max].copy()

            return (output_z_idx, frame)

        # Pre-allocate output array with cropped dimensions
        output = np.empty((output_z_size, output_y_size, output_x_size), dtype=self.dtype)

        # Parallel fetch all Z-slices
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_and_decode_chunk, zi): zi[0] for zi in chunk_info}

            for future in concurrent.futures.as_completed(futures):
                z_idx, frame = future.result()
                output[z_idx] = frame

        # Don't close session - it's shared across all readers via _get_http_session()

        elapsed = time.time() - start_time
        raw_bytes_mb = raw_bytes_total[0] / (1024 * 1024)
        throughput_mbps = (data_size_mb * 8) / elapsed if elapsed > 0 else 0
        raw_throughput_mbps = (raw_bytes_mb * 8) / elapsed if elapsed > 0 else 0

        # Verify correct channel data by checking median value (sampled for performance)
        # TL/brightfield has median ~25000, fluorescence has median ~100
        # Sample middle Z-slice instead of full volume median (much faster)
        mid_z = output_z_size // 2
        output_median = float(np.median(output[mid_z]))
        ch_name = self.channels[c] if c < len(self.channels) else f"Ch{c}"
        logger.info(
            f"Z-stack read complete (remote): T={t} C={c}({ch_name}) S={s}, "
            f"{data_size_mb:.1f} MB decompressed ({raw_bytes_mb:.1f} MB raw) in {elapsed:.1f}s "
            f"({throughput_mbps:.0f} Mbit/s effective, {raw_throughput_mbps:.0f} Mbit/s raw), median={output_median:.1f}"
        )

        self._warn_channel_mismatch(ch_name, output_median)
        return output

    def _warn_channel_mismatch(self, ch_name: str, output_median: float) -> None:
        """Warn if median doesn't match expected pattern for channel type."""
        if "TL" in ch_name.upper() and output_median < 1000:
            logger.warning(
                f"VERIFY: {ch_name} has low median ({output_median:.0f}), expected ~25000 for TL. "
                f"Channel mapping may be incorrect!"
            )
        elif "TL" not in ch_name.upper() and output_median > 10000:
            logger.warning(
                f"VERIFY: {ch_name} has high median ({output_median:.0f}), expected ~100 for fluorescence. "
                f"Channel mapping may be incorrect!"
            )

    def to_dask(self) -> da.Array:
        """Return lazy dask array for the full dataset.

        Returns:
            5D dask array of shape (T, C, Z, Y, X)
        """
        import dask.array as da
        from dask import delayed

        # Create delayed reads for each (T, C) Z-stack
        blocks = []
        for t in range(self.num_timepoints):
            t_blocks = []
            for c in range(self.num_channels):
                # Create a new reader for thread safety
                delayed_stack = delayed(self._read_zstack_new_connection)(t, c)
                dask_stack = da.from_delayed(
                    delayed_stack,
                    shape=(self.num_z, self.height, self.width),
                    dtype=self.dtype,
                )
                # Add T and C dimensions
                dask_stack = dask_stack[np.newaxis, np.newaxis, :, :, :]
                t_blocks.append(dask_stack)
            # Stack channels
            blocks.append(da.concatenate(t_blocks, axis=1))

        # Stack timepoints
        return da.concatenate(blocks, axis=0)

    def _read_zstack_new_connection(self, t: int, c: int) -> np.ndarray:
        """Read Z-stack with a fresh connection (for thread safety in dask).

        Uses cached metadata so only data transfer occurs, not metadata parsing.
        """
        # Create new reader instance - metadata is cached at module level
        # so this only opens a new HTTP connection, no metadata parsing
        reader = RemoteND2Reader(self.url, block_size=self._block_size)
        try:
            return reader.read_zstack(t=t, c=c)
        finally:
            reader.close()

    def close(self) -> None:
        """Close the underlying file connection."""
        if self._file is not None:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None

    def __enter__(self) -> RemoteND2Reader:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"RemoteND2Reader({self.url!r}, "
            f"shape=({self.num_timepoints}, {self.num_scenes}, {self.num_channels}, {self.num_z}, "
            f"{self.height}, {self.width}), dtype={self.dtype})"
        )
