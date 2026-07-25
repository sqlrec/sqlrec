"""Shared utilities for SqlRec Python modules (gbdt, tzrec, etc.).

Re-exports the public filesystem API so callers can write::

    from common import read_text, write_binary, read_parquet_table

or import the module directly::

    from common import filesystem as fs
    fs.read_text(path)
"""
from common.filesystem import (
    url_to_fs,
    resolve_path,
    read_text,
    write_text,
    read_binary,
    write_binary,
    load_pipeline_config,
    read_parquet_table,
    to_pandas,
    download_dir,
    copy_file,
    apply_monkeypatch,
    remove_monkeypatch,
    clear_filesystem_cache,
)

__all__ = [
    "url_to_fs",
    "resolve_path",
    "read_text",
    "write_text",
    "read_binary",
    "write_binary",
    "load_pipeline_config",
    "read_parquet_table",
    "to_pandas",
    "download_dir",
    "copy_file",
    "apply_monkeypatch",
    "remove_monkeypatch",
    "clear_filesystem_cache",
]
