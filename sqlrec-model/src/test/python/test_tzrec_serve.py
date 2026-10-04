"""Exercise the shell launcher with fake Hadoop and serving executables."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2] / "main/python/tzrec"


class TzrecServeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.app = self.root / "app with spaces"
        self.app.mkdir()
        shutil.copyfile(SOURCE / "server.sh", self.app / "server.sh")
        self.model = self.root / "export with spaces"
        self.model.mkdir()
        for name in ("scripted_model.pt", "fg.json", "pipeline.config"):
            (self.model / name).write_text(name)
        (self.model / "assets").mkdir()
        (self.model / "assets/vocab.txt").write_text("vocab")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.cache = self.root / "cache with spaces"
        self.log = self.root / "serving.json"
        self.hadoop_log = self.root / "hadoop.json"
        self.env = dict(os.environ)
        self.env.pop("TZREC_SERVING_BACKEND", None)
        self.env.update({"PATH": str(self.bin) + os.pathsep + self.env["PATH"],
                         "HADOOP_HOME": str(self.root / "hadoop home"),
                         "LOCAL_CACHE_DIR": str(self.cache),
                         "REMOTE_SOURCE": str(self.model),
                         "SERVING_LOG": str(self.log), "HADOOP_LOG": str(self.hadoop_log)})
        recorder = """import json,os,sys
from pathlib import Path
Path(os.environ['SERVING_LOG']).write_text(json.dumps(sys.argv[1:]))
"""
        self.executable(self.app / "tzrec_server", recorder)
        self.executable(self.bin / "python", recorder)
        hadoop = Path(self.env["HADOOP_HOME"]) / "bin/hadoop"
        hadoop.parent.mkdir(parents=True)
        self.executable(hadoop, """import json,os,shutil,sys
from pathlib import Path
Path(os.environ['HADOOP_LOG']).write_text(json.dumps(sys.argv[1:]))
assert sys.argv[1:3] == ['fs','-get']
destination=Path(sys.argv[4])
if os.environ.get('FAIL_DOWNLOAD'):
    destination.mkdir()
    (destination/'scripted_model.pt').write_text('partial')
    sys.exit(7)
shutil.copytree(os.environ['REMOTE_SOURCE'],destination)
""")

    @staticmethod
    def executable(path, code):
        path.write_text(f"#!{sys.executable}\n{code}")
        path.chmod(0o755)

    def launch(self, directory, *extra):
        return subprocess.run(["bash", str(self.app / "server.sh"),
                               "--scripted_model_dir", str(directory), *extra],
                              env=self.env, capture_output=True, text=True, timeout=10)

    def test_local_backend_selection_and_arguments_without_hadoop(self):
        for backend in (None, "cpp", "python"):
            with self.subTest(backend=backend):
                self.log.unlink(missing_ok=True)
                if backend is not None:
                    self.env["TZREC_SERVING_BACKEND"] = backend
                result = self.launch(self.model, "--host=127.0.0.1", "--port", "08080")
                self.assertEqual(result.returncode, 0, result.stderr)
                arguments = json.loads(self.log.read_text())
                local = str(self.model.resolve())
                expected = ([str(self.app / "server.py"), "--scripted_model_dir", local,
                             "--host", "127.0.0.1", "--port", "8080"]
                            if backend == "python" else [local, "127.0.0.1", "8080"])
                self.assertEqual(arguments, expected)
                self.assertFalse(self.hadoop_log.exists())
                self.assertFalse(self.cache.exists())

    def test_remote_exports_preserve_layout_use_private_caches_and_quote_paths(self):
        remote = 'hdfs://namenode:8020/models/export with spaces/$(touch injected)'
        directories = []
        for _ in range(2):
            result = self.launch(remote)
            self.assertEqual(result.returncode, 0, result.stderr)
            local = Path(json.loads(self.log.read_text())[0])
            self.assertEqual(local.parent.parent, self.cache)
            self.assertEqual((local / "assets/vocab.txt").read_text(), "vocab")
            self.assertEqual((local / "scripted_model.pt").read_text(), "scripted_model.pt")
            self.assertEqual(json.loads(self.hadoop_log.read_text()), ["fs", "-get", remote, str(local)])
            directories.append(local)
        self.assertNotEqual(*directories)

    def test_hadoop_on_path_and_python_backend_use_the_same_download(self):
        shutil.copyfile(Path(self.env.pop("HADOOP_HOME")) / "bin/hadoop", self.bin / "hadoop")
        (self.bin / "hadoop").chmod(0o755)
        self.env["TZREC_SERVING_BACKEND"] = "python"
        result = self.launch("jfs://myjfs/models/export")
        self.assertEqual(result.returncode, 0, result.stderr)
        arguments = json.loads(self.log.read_text())
        self.assertEqual(arguments[0], str(self.app / "server.py"))
        self.assertTrue((Path(arguments[2]) / "fg.json").is_file())
        self.assertEqual(arguments[2], json.loads(self.hadoop_log.read_text())[-1])

    def test_failed_download_is_removed_and_does_not_start_serving(self):
        self.env["FAIL_DOWNLOAD"] = "1"
        result = self.launch("hdfs://namenode/models/export")
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(list(self.cache.iterdir()), [])
        self.assertFalse(self.log.exists())

    def test_incomplete_remote_export_is_removed_and_local_export_is_preserved(self):
        (self.model / "pipeline.config").unlink()
        for directory in ("hdfs://namenode/models/export", self.model):
            with self.subTest(directory=directory):
                result = self.launch(directory)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Export is missing pipeline.config", result.stderr)
                self.assertFalse(self.log.exists())
                self.assertEqual(list(self.cache.iterdir()), [])
                self.assertTrue((self.model / "scripted_model.pt").is_file())

    def test_invalid_backend_and_port_fail_before_download(self):
        for backend, port in (("invalid", "80"), ("", "80"), ("cpp", "0"),
                              ("cpp", "65536"), ("cpp", "invalid")):
            with self.subTest(backend=backend, port=port):
                self.env["TZREC_SERVING_BACKEND"] = backend
                result = self.launch("hdfs://namenode/models/export", "--port", port)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.hadoop_log.exists())
                self.assertFalse(self.log.exists())
                self.assertFalse(self.cache.exists())

    def test_missing_hadoop_fails_with_a_clear_error(self):
        self.env["HADOOP_HOME"] = str(self.root / "missing")
        result = self.launch("hdfs://namenode/models/export")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Hadoop executable not found", result.stderr)
        self.assertFalse(self.log.exists())
        self.assertFalse(self.cache.exists())
