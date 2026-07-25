"""Protocol-agnostic filesystem utilities shared across SqlRec Python modules.

Supports any storage protocol that fsspec can handle (hdfs, s3, gs, jfs, etc.),
plus local paths. All protocols rely on fsspec's native configuration (env vars,
config files, etc.).

Provides three layers of API:

**Layer A — Path resolution** (``url_to_fs``, ``resolve_path``):
    Split a path into ``(fsspec filesystem, full_path)``. Filesystem instances
    are cached by protocol for reuse.

**Layer B — Explicit helpers** (``read_text``, ``write_binary``, …):
    High-level functions for common I/O operations. Use these when you need
    precise control or when third-party libraries perform C-level I/O that
    monkeypatching cannot intercept (e.g. CatBoost ``load_model``).

**Layer C — Monkeypatch** (``apply_monkeypatch`` / ``remove_monkeypatch``):
    Patch ``builtins.open``, ``os.makedirs``, ``os.path.exists`` etc. so that
    standard library calls transparently work with any fsspec-supported
    protocol. Opt-in — call ``apply_monkeypatch()`` at process startup if needed.
"""
from __future__ import annotations

import builtins
import glob as glob_module
import json
import os
import shutil
from typing import Any, Iterable

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Original function references (saved at import time for monkeypatch restore).
# ---------------------------------------------------------------------------
_original_open = builtins.open
_original_makedirs = os.makedirs
_original_listdir = os.listdir
_original_remove = os.remove
_original_isdir = os.path.isdir
_original_exists = os.path.exists
_original_copy = shutil.copy
_original_glob = glob_module.glob
_original_getsize = os.path.getsize

_CACHED_FSSPEC_FILESYSTEMS: dict[str, fsspec.AbstractFileSystem] = {}


# ===========================================================================
# Layer A — Path resolution
# ===========================================================================

def clear_filesystem_cache() -> None:
    """Clear cached fsspec filesystem instances (for testing)."""
    _CACHED_FSSPEC_FILESYSTEMS.clear()


def url_to_fs(path: str) -> tuple[fsspec.AbstractFileSystem | None, str]:
    """Convert a path to ``(filesystem, full_path)``.

    For local paths (no protocol prefix), returns ``(None, path)`` so callers
    fall back to standard ``open()`` / ``os`` calls. For remote paths (any
    fsspec-supported protocol), the filesystem is cached by protocol for reuse.
    """
    protocol, _ = fsspec.core.split_protocol(path)
    if protocol is None:
        return None, path
    if protocol in _CACHED_FSSPEC_FILESYSTEMS:
        return _CACHED_FSSPEC_FILESYSTEMS[protocol], path
    fs, _ = fsspec.core.url_to_fs(path)
    _CACHED_FSSPEC_FILESYSTEMS[protocol] = fs
    return fs, path


def resolve_path(path: str) -> tuple[fsspec.AbstractFileSystem | None, str]:
    """Alias for :func:`url_to_fs`."""
    return url_to_fs(path)


# ===========================================================================
# Layer B — Explicit helper functions
# ===========================================================================

def read_text(path: str) -> str:
    """Read a text file (UTF-8) from local or remote storage."""
    with _patched_open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_text(path: str, content: str) -> None:
    """Write text, creating parent dirs as needed."""
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        _patched_makedirs(parent, exist_ok=True)
    with _patched_open(path, "w", encoding="utf-8") as f:
        f.write(content)


def read_binary(path: str) -> bytes:
    """Read a binary file from local or remote storage."""
    with _patched_open(path, "rb") as f:
        return f.read()


def write_binary(path: str, data: bytes) -> None:
    """Write binary data, creating parent dirs as needed."""
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        _patched_makedirs(parent, exist_ok=True)
    with _patched_open(path, "wb") as f:
        f.write(data)


def load_pipeline_config(path: str) -> dict[str, Any]:
    """Load the JSON pipeline.config produced by the Java side."""
    return json.loads(read_text(path))


def read_parquet_table(paths: str | Iterable[str]) -> pa.Table:
    """Read one or more comma-separated parquet paths into a single Table."""
    if isinstance(paths, str):
        path_list = [p.strip() for p in paths.split(",") if p.strip()]
    else:
        path_list = [p.strip() for p in paths if p.strip()]
    if not path_list:
        raise ValueError("No training data paths provided")

    tables: list[pa.Table] = []
    for p in path_list:
        if any(c in p for c in "*?["):
            # Glob pattern (e.g. "dir/*.parquet"): expand and filter.
            file_paths = [fp for fp in _patched_glob(p, recursive=True)
                          if fp.endswith(".parquet")]
        elif _patched_isdir(p):
            # Directory: glob all .parquet files recursively.
            file_paths = _patched_glob(p.rstrip("/") + "/**/*.parquet", recursive=True)
        else:
            file_paths = [p]
        if not file_paths:
            raise FileNotFoundError(f"No parquet files found at {p}")
        for fp in file_paths:
            fs, _ = url_to_fs(fp)
            pf = pq.ParquetFile(fp, filesystem=fs)
            tables.append(pf.read())
            pf.close()
    return pa.concat_tables(tables)


def to_pandas(table: pa.Table):
    """Convert an Arrow Table to a pandas DataFrame.

    Uses ``self_destruct=True`` to release Arrow buffers as each column is
    converted, reducing peak memory from ~2x to ~1x the data size.

    ``use_threads=False`` avoids a temporary double-buffering spike that
    occurs when multi-threaded conversion allocates all output columns
    before releasing any Arrow input buffers.
    """
    return table.to_pandas(self_destruct=True, use_threads=False)


def download_dir(remote_dir: str, local_dir: str) -> None:
    """Download all files from ``remote_dir`` into ``local_dir`` recursively.

    Used by the container entrypoint to cache model artifacts locally before
    launching the C++ inference server.
    """
    fs, full = url_to_fs(remote_dir)
    _original_makedirs(local_dir, exist_ok=True)
    if fs is not None:
        # fs.find() returns paths in the filesystem's native format: path only,
        # WITHOUT protocol or host prefix (e.g. "/user/.../schema.json", not
        # "jfs://myjfs/user/.../schema.json" or "myjfs/user/.../schema.json").
        # Use infer_storage_options to extract just the path component from
        # `full` so it matches the format returned by fs.find().
        base_path = fsspec.utils.infer_storage_options(full)["path"]
        for entry in fs.find(full):
            if not fs.isfile(entry):
                continue
            # Normalize entry to path-only format in case a filesystem returns
            # URLs (with protocol/host) from find().
            entry_path = (fsspec.utils.infer_storage_options(entry)["path"]
                          if "://" in entry else entry)
            rel = os.path.relpath(entry_path, base_path)
            # Guard against format mismatch producing upward traversal.
            if rel.startswith(".."):
                rel = os.path.basename(entry_path)
            local_path = os.path.join(local_dir, rel)
            local_parent = os.path.dirname(local_path)
            if local_parent:
                _original_makedirs(local_parent, exist_ok=True)
            with fs.open(entry, "rb") as src, _original_open(local_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
    else:
        shutil.copytree(full, local_dir, dirs_exist_ok=True)


def copy_file(src_path: str, dst_path: str) -> None:
    """Copy a file from src to dst, streaming in chunks.
    Both paths can be local or remote (hdfs://, jfs://, s3://, etc.).
    """
    src_fs, src_full = url_to_fs(src_path)
    dst_fs, dst_full = url_to_fs(dst_path)

    # Ensure parent directory exists for destination.
    if dst_fs is not None:
        parent = dst_full.rsplit("/", 1)[0] if "/" in dst_full else ""
        if parent:
            dst_fs.makedirs(parent, exist_ok=True)
    else:
        parent = os.path.dirname(dst_full)
        if parent:
            _original_makedirs(parent, exist_ok=True)

    # Stream copy.
    src = src_fs.open(src_full, "rb") if src_fs is not None else _original_open(src_full, "rb")
    dst = dst_fs.open(dst_full, "wb") if dst_fs is not None else _original_open(dst_full, "wb")
    try:
        shutil.copyfileobj(src, dst)
    finally:
        dst.close()
        src.close()


# ===========================================================================
# Layer C — Monkeypatch (opt-in)
# ===========================================================================

def _patched_open(path, mode="r", *args, **kwargs):
    fs, full = url_to_fs(path)
    if fs is not None:
        return fs.open(full, mode, *args, **kwargs)
    return _original_open(full, mode, *args, **kwargs)


def _patched_makedirs(path, mode=0o777, exist_ok=False):
    fs, full = url_to_fs(path)
    if fs is not None:
        return fs.makedirs(full, exist_ok=exist_ok)
    return _original_makedirs(full, mode=mode, exist_ok=exist_ok)


def _patched_isdir(path):
    fs, full = url_to_fs(path)
    if fs is not None:
        return fs.isdir(full)
    return _original_isdir(full)


def _patched_listdir(path):
    fs, full = url_to_fs(path)
    if fs is not None:
        return fs.ls(full, detail=False)
    return _original_listdir(full)


def _patched_remove(path):
    fs, full = url_to_fs(path)
    if fs is not None:
        return fs.rm(full)
    return _original_remove(full)


def _patched_exists(path):
    fs, full = url_to_fs(path)
    if fs is not None:
        return fs.exists(full)
    return _original_exists(full)


def _patched_copy(src, dst, *args, **kwargs):
    src_fs, src_full = url_to_fs(src)
    dst_fs, dst_full = url_to_fs(dst)
    if src_fs is not None or dst_fs is not None:
        src_fs = src_fs or _get_local_fs()
        dst_fs = dst_fs or _get_local_fs()
        with src_fs.open(src_full, "rb") as fsrc:
            with dst_fs.open(dst_full, "wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)
        return dst
    return _original_copy(src, dst, *args, **kwargs)


def _patched_glob(pattern, *args, **kwargs):
    fs, full = url_to_fs(pattern)
    if fs is not None:
        protocol_prefix = fsspec.utils.infer_storage_options(full).get("protocol", "")
        if protocol_prefix:
            host = fsspec.utils.infer_storage_options(full).get("host", "")
            prefix = f"{protocol_prefix}://{host}" if host else f"{protocol_prefix}://"
        else:
            prefix = ""
        ret = []
        for path in fs.glob(full, *args, **kwargs):
            if isinstance(path, str) and not path.startswith(prefix):
                path = prefix + path
            ret.append(path)
        return ret
    return _original_glob(pattern, *args, **kwargs)


def _patch_get_size(filename):
    fs, full = url_to_fs(filename)
    if fs is not None:
        return fs.size(full)
    return _original_getsize(full)


def _get_local_fs() -> fsspec.AbstractFileSystem:
    if "file" not in _CACHED_FSSPEC_FILESYSTEMS:
        _CACHED_FSSPEC_FILESYSTEMS["file"] = fsspec.filesystem("file")
    return _CACHED_FSSPEC_FILESYSTEMS["file"]


def apply_monkeypatch():
    """Patch ``builtins.open``, ``os.makedirs``, etc. to support remote paths.

    After calling this, standard library calls transparently work with any
    fsspec-supported protocol (hdfs, s3, gs, jfs, etc.). Call
    :func:`remove_monkeypatch` to restore originals.
    """
    builtins.open = _patched_open
    os.makedirs = _patched_makedirs
    os.path.isdir = _patched_isdir
    os.listdir = _patched_listdir
    os.remove = _patched_remove
    os.path.exists = _patched_exists
    shutil.copy = _patched_copy
    glob_module.glob = _patched_glob
    os.path.getsize = _patch_get_size


def remove_monkeypatch():
    """Restore original builtins/os/shutil functions."""
    builtins.open = _original_open
    os.makedirs = _original_makedirs
    os.path.isdir = _original_isdir
    os.listdir = _original_listdir
    os.remove = _original_remove
    os.path.exists = _original_exists
    shutil.copy = _original_copy
    glob_module.glob = _original_glob
    os.path.getsize = _original_getsize
