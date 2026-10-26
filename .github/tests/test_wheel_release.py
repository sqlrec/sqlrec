"""Regression tests for shared wheels, release publication, and local builds."""

import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
import zipfile


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / ".github" / "scripts"


spec = importlib.util.spec_from_file_location("artifacts", SCRIPTS_DIR / "model_wheel_artifacts.py")


artifacts = importlib.util.module_from_spec(spec)


spec.loader.exec_module(artifacts)


COMMIT = "a" * 40


JUICEFS = "b" * 40


TZREC = "c" * 40


COMPAT = "d" * 40


def make_wheel(directory, name, version="1.0", arch=None, machine=None, native_in_pure=False):
    directory.mkdir(parents=True, exist_ok=True)
    tag = f"cp311-cp311-linux_{arch}" if arch else "py3-none-any"
    path = directory / f"{name}-{version}-{tag}.whl"
    prefix = f"{name}-{version}.dist-info/"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(prefix + "METADATA", f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        wheel.writestr(prefix + "WHEEL", f"Wheel-Version: 1.0\nRoot-Is-Purelib: {'false' if arch else 'true'}\nTag: {tag}\n")
        wheel.writestr(f"{name}/__init__.py", "")
        if arch:
            header = bytearray(20)
            header[:6] = b"\x7fELF\x02\x01"
            struct.pack_into("<H", header, 18, machine or (62 if arch == "x86_64" else 183))
            wheel.writestr("juicefs/libjfs.so", header)
        if native_in_pure:
            wheel.writestr(f"{name}/native.so", b"native")
    return path


class ArtifactTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def group(self, group, source=JUICEFS):
        directory = self.root / f"model-wheels-{group}"
        if group.startswith("juicefs-"):
            make_wheel(directory, "juicefs", arch="x86_64" if group.endswith("amd64") else "aarch64")
            sources = [f"juicefs={source}"]
        else:
            for name in ("tzrec", "pyfg", "graphlearn"):
                make_wheel(directory, name)
            sources = [f"tzrec={TZREC}", f"compat={COMPAT}"]
        artifacts.record(directory, group, COMMIT, sources)
        return directory

    def release_inputs(self):
        for group in artifacts.GROUPS:
            self.group(group)
        directory = self.root / "release-jars"
        directory.mkdir()
        for name in ("sqlrec-flink", "sqlrec-frontend"):
            with zipfile.ZipFile(directory / f"{name}-0.1.jar", "w") as jar:
                jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")

    def test_consumers_and_release_preserve_wheel_bytes(self):
        self.release_inputs()
        source = self.root / "model-wheels-juicefs-arm64"
        manifest = artifacts.verify(source, "juicefs-arm64", COMMIT, [f"juicefs={JUICEFS}"])
        wheel = source / manifest["wheels"][0]["filename"]
        for consumer in ("gbdt", "tzrec"):
            artifacts.copy_wheel(wheel, self.root / consumer)
        output = self.root / "release"
        release = artifacts.assemble(self.root, output, COMMIT)
        hashes = [artifacts.sha256(self.root / consumer / wheel.name) for consumer in ("gbdt", "tzrec", "release")]
        self.assertEqual(len(set(hashes)), 1)
        self.assertEqual(len(release["wheels"]), 5)
        for line in (output / "SHA256SUMS").read_text().splitlines():
            digest, filename = line.split("  ", 1)
            self.assertEqual(digest, hashlib.sha256((output / filename).read_bytes()).hexdigest())
        self.assertIn("sources.json", (output / "SHA256SUMS").read_text())

    def test_rejects_corrupted_wheel_and_wrong_source(self):
        directory = self.group("common")
        with self.assertRaisesRegex(ValueError, "revision mismatch"):
            artifacts.verify(directory, "common", COMMIT, [f"tzrec={TZREC}", f"compat={JUICEFS}"])
        with self.assertRaisesRegex(ValueError, "SQLRec commit"):
            artifacts.verify(directory, "common", "e" * 40)
        wheel = next(directory.glob("pyfg-*.whl"))
        with zipfile.ZipFile(wheel, "a") as archive:
            archive.writestr("tampered.py", "pass")
        with self.assertRaisesRegex(ValueError, "checksum"):
            artifacts.verify(directory, "common", COMMIT)

    def test_rejects_missing_or_duplicate_package(self):
        directory = self.group("common")
        next(directory.glob("graphlearn-*.whl")).unlink()
        with self.assertRaisesRegex(ValueError, "Expected"):
            artifacts.verify(directory, "common", COMMIT)
        make_wheel(directory, "graphlearn")
        make_wheel(directory, "pyfg", version="2.0")
        with self.assertRaisesRegex(ValueError, "Expected"):
            artifacts.verify(directory, "common", COMMIT)

    def test_rejects_mislabelled_elf_and_native_pure_wheel(self):
        directory = self.root / "native"
        make_wheel(directory, "juicefs", arch="x86_64", machine=183)
        with self.assertRaisesRegex(ValueError, "ELF architecture"):
            artifacts.inspect_group(directory, "juicefs-amd64")
        with self.assertRaisesRegex(ValueError, "Wrong Python/architecture"):
            artifacts.inspect_group(directory, "juicefs-arm64")
        directory = self.root / "pure"
        make_wheel(directory, "pyfg", native_in_pure=True)
        with self.assertRaisesRegex(ValueError, "pure Python"):
            artifacts.inspect_group(directory, "common")

    def test_rejects_inconsistent_sources_or_missing_artifact(self):
        self.release_inputs()
        directory = self.root / "model-wheels-juicefs-arm64"
        artifacts.record(directory, "juicefs-arm64", COMMIT, [f"juicefs={TZREC}"])
        with self.assertRaisesRegex(ValueError, "different source"):
            artifacts.assemble(self.root, self.root / "release", COMMIT)
        shutil.rmtree(directory)
        with self.assertRaises(FileNotFoundError):
            artifacts.assemble(self.root, self.root / "release", COMMIT)

    def test_rejects_missing_jar_before_creating_release(self):
        self.release_inputs()
        next((self.root / "release-jars").glob("sqlrec-frontend-*.jar")).unlink()
        with self.assertRaisesRegex(ValueError, "sqlrec-frontend"):
            artifacts.assemble(self.root, self.root / "release", COMMIT)
        self.assertFalse((self.root / "release").exists())

    def test_refuses_overwriting_different_consumer_wheel(self):
        directory = self.group("common")
        wheel = next(directory.glob("pyfg-*.whl"))
        destination = self.root / "consumer"
        artifacts.copy_wheel(wheel, destination)
        (destination / wheel.name).write_bytes(b"different wheel")
        with self.assertRaisesRegex(ValueError, "overwrite"):
            artifacts.copy_wheel(wheel, destination)


FAKE_GH = r'''
import json, os
from pathlib import Path
import shutil, sys
root = Path(os.environ["FAKE_GH_STORE"])
args = sys.argv[1:]
with (root/"calls.jsonl").open("a") as stream:
    stream.write(json.dumps(args)+"\n")
path = root/"state.json"
state = json.loads(path.read_text()) if path.exists() else None
command = args[1]
if command == "view":
    if state is None:
        sys.exit(1)
    if "--jq" in args:
        print("\n".join(asset["name"] for asset in state["assets"]))
    else:
        print(json.dumps(state))
    sys.exit(0)
if command == "create":
    state = {"isDraft": True, "assets": []}
elif command == "upload":
    for source in args[3:-1]:
        shutil.copy2(source, root/"assets"/Path(source).name)
        state["assets"] = [{"name": p.name} for p in (root/"assets").iterdir()]
        path.write_text(json.dumps(state))
        if os.environ.get("FAKE_FAIL_UPLOAD"):
            sys.exit(1)
elif command == "delete-asset":
    (root/"assets"/args[3]).unlink()
    state["assets"] = [{"name": p.name} for p in (root/"assets").iterdir()]
elif command == "download":
    shutil.copytree(root/"assets", args[args.index("--dir")+1])
elif command == "edit":
    state["isDraft"] = False
else:
    raise AssertionError(args)
path.write_text(json.dumps(state))
'''


class PublishReleaseTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = self.root / "store"
        (self.store / "assets").mkdir(parents=True)
        self.assets = self.root / "release"
        self.assets.mkdir()
        for name in ("package.whl", "app.jar", "sources.json"):
            (self.assets / name).write_bytes(name.encode())
        (self.assets / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
            for path in sorted(self.assets.iterdir())
        ))
        (self.assets / "RELEASE_NOTES.md").write_text("Release notes")
        tools = self.root / "tools"
        tools.mkdir()
        gh = tools / "gh"
        gh.write_text(f"#!{sys.executable}\n" + FAKE_GH)
        gh.chmod(0o755)
        self.environment = {
            **os.environ, "PATH": f"{tools}:{os.environ['PATH']}",
            "FAKE_GH_STORE": str(self.store), "RUNNER_TEMP": str(self.root),
        }

    def run_publisher(self, fail_upload=False):
        environment = {**self.environment}
        if fail_upload:
            environment["FAKE_FAIL_UPLOAD"] = "1"
        return subprocess.run(
            ["bash", str(SCRIPTS_DIR / "publish_release.sh"), str(self.assets), "v0.1"],
            env=environment, text=True, capture_output=True,
        )

    def state(self):
        return json.loads((self.store / "state.json").read_text())

    def test_publishes_complete_draft_and_retry_does_not_mutate(self):
        result = self.run_publisher()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.state()["isDraft"])
        calls = (self.store / "calls.jsonl").read_text().splitlines()
        result = self.run_publisher()
        self.assertEqual(result.returncode, 0, result.stderr)
        subsequent = (self.store / "calls.jsonl").read_text().splitlines()[len(calls):]
        self.assertEqual([json.loads(call)[1] for call in subsequent], ["view", "download"])

    def test_upload_failure_leaves_draft_and_retry_removes_stale_assets(self):
        result = self.run_publisher(fail_upload=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(self.state()["isDraft"])
        (self.store / "assets" / "obsolete.whl").write_text("stale")
        state = self.state()
        state["assets"].append({"name": "obsolete.whl"})
        (self.store / "state.json").write_text(json.dumps(state))
        result = self.run_publisher()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.state()["isDraft"])
        self.assertFalse((self.store / "assets" / "obsolete.whl").exists())

    def test_never_overwrites_a_published_release_with_different_content(self):
        self.assertEqual(self.run_publisher().returncode, 0)
        (self.store / "assets" / "package.whl").write_text("tampered")
        result = self.run_publisher()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checksum mismatch", result.stderr)
        self.assertEqual((self.store / "assets" / "package.whl").read_text(), "tampered")


class ModelBuildScriptTest(unittest.TestCase):
    def test_each_required_wheel_is_built_once(self):
        script = REPO_ROOT / "bin" / "build_model_docker.sh"
        shell = r'''
set -euo pipefail
source "$1"
target="$2"
arch="$3"
calls="$4"
work_dir="unused"
SQLREC_VERSION="test"
build_juicefs_wheel() { echo juicefs >> "$calls"; }
build_tzrec_wheel() { echo tzrec >> "$calls"; }
build_compat_wheels() { echo compat >> "$calls"; }
prepare_gbdt_context() { :; }
prepare_tzrec_context() { :; }
prepare_transformers_context() { :; }
build_and_verify_image() { :; }
prepare_wheels
case "$target" in
  all) build_tzrec; build_gbdt; build_transformers ;;
  tzrec) build_tzrec ;;
  gbdt) build_gbdt ;;
  transformers) build_transformers ;;
esac
'''
        with tempfile.TemporaryDirectory() as directory:
            for arch in ("amd64", "arm64"):
                for target in ("all", "tzrec", "gbdt", "transformers"):
                    with self.subTest(arch=arch, target=target):
                        calls = Path(directory) / f"{arch}-{target}"
                        calls.touch()
                        subprocess.run(["bash", "-c", shell, "test", str(script), target, arch, str(calls)], check=True)
                        expected = [] if target == "transformers" else ["juicefs"]
                        if target in ("all", "tzrec"):
                            expected.append("tzrec")
                            if arch == "arm64":
                                expected.append("compat")
                        self.assertEqual(collections.Counter(calls.read_text().splitlines()), collections.Counter(expected))


if __name__ == "__main__":
    unittest.main()
