"""Regression tests for local and fsspec-backed model artifacts."""

import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from common import filesystem


class FilesystemTest(unittest.TestCase):
    def setUp(self):
        self.base = f"memory://{uuid.uuid4().hex}/model"
        self.fs, _ = filesystem.url_to_fs(self.base)
        self.addCleanup(self._remove_remote_files)

    def _remove_remote_files(self):
        if self.fs.exists(self.base):
            self.fs.rm(self.base, recursive=True)

    def test_remote_parquet_glob_and_nested_download(self):
        files = {}
        for name, value in (("first.parquet", 1.0), ("nested/second.parquet", 2.0)):
            path = f"{self.base}/{name}"
            with self.fs.open(path, "wb") as stream:
                pq.write_table(pa.table({"feature": [value]}), stream)
            files[name] = filesystem.read_binary(path)
        filesystem.write_text(f"{self.base}/notes.txt", "ignore me")

        expected = [{"feature": 1.0}, {"feature": 2.0}]
        inputs = (
            self.base,
            f"{self.base}/**/*.parquet",
            [f"{self.base}/{name}" for name in files],
            ",".join(f"{self.base}/{name}" for name in files),
        )
        for paths in inputs:
            with self.subTest(paths=paths):
                rows = filesystem.read_parquet_table(paths).to_pylist()
                self.assertEqual(sorted(rows, key=lambda row: row["feature"]), expected)

        with tempfile.TemporaryDirectory() as local_dir:
            filesystem.download_dir(self.base, local_dir)
            for name, contents in files.items():
                self.assertEqual((Path(local_dir) / name).read_bytes(), contents)
            self.assertEqual((Path(local_dir) / "notes.txt").read_text(), "ignore me")

    def test_missing_parquet_data_reports_the_path(self):
        with self.assertRaisesRegex(ValueError, "No training data paths"):
            filesystem.read_parquet_table([])
        with self.assertRaisesRegex(FileNotFoundError, "missing"):
            filesystem.read_parquet_table(f"{self.base}/missing/*.parquet")

    def test_copy_file_between_local_and_remote_storage(self):
        contents = b"\x00model\xff" * 1024
        with tempfile.TemporaryDirectory() as local_dir:
            source = Path(local_dir) / "source.bin"
            destination = Path(local_dir) / "nested/destination.bin"
            source.write_bytes(contents)
            remote = f"{self.base}/nested/model.bin"

            filesystem.copy_file(str(source), remote)
            self.assertEqual(filesystem.read_binary(remote), contents)
            filesystem.copy_file(remote, str(destination))
            self.assertEqual(destination.read_bytes(), contents)

    def test_download_rejects_files_outside_remote_directory(self):
        class RemoteFS:
            @staticmethod
            def _strip_protocol(path):
                return path.removeprefix("memory://bucket")

            @staticmethod
            def find(path):
                return ["/other/file"]

            @staticmethod
            def isfile(path):
                return True

        with (
            tempfile.TemporaryDirectory() as local_dir,
            patch.object(filesystem, "url_to_fs",
                         return_value=(RemoteFS(), "memory://bucket/model")),
        ):
            with self.assertRaisesRegex(ValueError, "outside model directory"):
                filesystem.download_dir("memory://bucket/model", local_dir)

    def test_url_resolution_does_not_reuse_other_remote(self):
        first, second = object(), object()
        with patch.object(filesystem.fsspec.core, "url_to_fs",
                          side_effect=[(first, ""), (second, "")]) as resolve:
            self.assertIs(filesystem.url_to_fs("custom://first/file")[0], first)
            self.assertIs(filesystem.url_to_fs("custom://second/file")[0], second)
            self.assertEqual(resolve.call_count, 2)

    def test_glob_keeps_remote_host_and_port(self):
        class RemoteFS:
            @staticmethod
            def _strip_protocol(path):
                return path.removeprefix("hdfs://host:8020")

            @staticmethod
            def glob(pattern, **kwargs):
                return ["/data/model.parquet"]

        with patch.object(filesystem, "url_to_fs",
                          return_value=(RemoteFS(), "hdfs://host:8020/data/*.parquet")):
            self.assertEqual(
                filesystem._patched_glob("hdfs://host:8020/data/*.parquet"),
                ["hdfs://host:8020/data/model.parquet"],
            )

    def test_source_is_closed_if_destination_cannot_be_opened(self):
        with tempfile.TemporaryDirectory() as local_dir:
            source = Path(local_dir) / "source"
            source.write_bytes(b"model")
            opened = []
            original_open = filesystem._original_open

            def track_open(path, mode):
                if path == local_dir:
                    raise PermissionError("destination cannot be opened")
                stream = original_open(path, mode)
                opened.append(stream)
                return stream

            with patch.object(filesystem, "_original_open", side_effect=track_open):
                with self.assertRaises(PermissionError):
                    filesystem.copy_file(str(source), local_dir)
            self.assertEqual(len(opened), 1)
            self.assertTrue(opened[0].closed)


if __name__ == "__main__":
    unittest.main()
