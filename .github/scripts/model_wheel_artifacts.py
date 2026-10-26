#!/usr/bin/env python3
"""Record, verify, and collect the model wheels shared by images and releases."""

import argparse
from email.parser import BytesParser
import hashlib
from itertools import product
import json
from pathlib import Path
import re
import shutil
import struct
import zipfile


GROUPS = {
    "juicefs-amd64": {"juicefs"},
    "juicefs-arm64": {"juicefs"},
    "common": {"tzrec", "pyfg", "graphlearn"},
}
SOURCE_REPOS = {
    "juicefs": "sqlrec/juicefs",
    "tzrec": "sqlrec/TorchEasyRec",
    "compat": "sqlrec/sqlrec-arm-compat",
}


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def check_commit(value):
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value):
        raise ValueError(f"Invalid source commit: {value!r}")
    return value


def inspect_wheel(path, group):
    parts = path.stem.split("-")
    if len(parts) not in (5, 6):
        raise ValueError(f"Invalid wheel filename: {path.name}")
    with zipfile.ZipFile(path) as archive:
        metadata_paths = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise ValueError(f"Expected one METADATA in {path.name}")
        prefix = metadata_paths[0].removesuffix("METADATA")
        metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
        wheel = BytesParser().parsebytes(archive.read(prefix + "WHEEL"))
        name, version = metadata["Name"], metadata["Version"]
        if name not in GROUPS[group] or name.replace("-", "_") != parts[0] or version != parts[1]:
            raise ValueError(f"Unexpected package or version in {path.name}: {name} {version}")
        tags = sorted(wheel.get_all("Tag", []))
        if set(tags) != {"-".join(tag) for tag in product(*(part.split(".") for part in parts[-3:]))}:
            raise ValueError(f"Filename and WHEEL tags disagree in {path.name}")
        if group.startswith("juicefs-"):
            arch = "x86_64" if group == "juicefs-amd64" else "aarch64"
            tag = f"cp311-cp311-linux_{arch}"
            if "-".join(parts[-3:]) != tag or tags != [tag]:
                raise ValueError(f"Wrong Python/architecture tag in {path.name}; expected {tag}")
            with archive.open("juicefs/libjfs.so") as library:
                header = library.read(20)
            machine = 62 if arch == "x86_64" else 183
            if len(header) < 20 or header[:6] != b"\x7fELF\x02\x01" or struct.unpack("<H", header[18:20])[0] != machine:
                raise ValueError(f"Wrong ELF architecture in {path.name}")
        elif (parts[-2:] != ["none", "any"] or not tags
              or any(not tag.endswith("-none-any") for tag in tags)
              or wheel["Root-Is-Purelib"] != "true"
              or any(name.endswith((".so", ".pyd", ".dll", ".dylib")) for name in archive.namelist())):
            raise ValueError(f"Expected a pure Python wheel: {path.name}")
    return {
        "filename": path.name, "name": name, "version": version,
        "requires_python": metadata.get("Requires-Python", ""),
        "tags": tags, "sha256": sha256(path),
    }


def inspect_group(directory, group):
    wheels = [inspect_wheel(path, group) for path in sorted(directory.glob("*.whl"))]
    names = [wheel["name"] for wheel in wheels]
    if len(names) != len(GROUPS[group]) or set(names) != GROUPS[group]:
        raise ValueError(f"Expected {sorted(GROUPS[group])} in {directory}; found {names}")
    return wheels


def source_commits(values, group):
    sources = {}
    for value in values:
        name, commit = value.split("=", 1)
        if name in sources:
            raise ValueError(f"Duplicate source: {name}")
        sources[name] = check_commit(commit)
    expected = {"juicefs"} if group.startswith("juicefs-") else {"tzrec", "compat"}
    if sources.keys() != expected:
        raise ValueError(f"Expected source commits for {sorted(expected)}")
    return sources


def record(directory, group, sqlrec_commit, sources):
    manifest = {
        "schema": 1, "group": group, "sqlrec_commit": check_commit(sqlrec_commit),
        "sources": source_commits(sources, group), "wheels": inspect_group(directory, group),
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def verify(directory, group, sqlrec_commit, sources=None):
    manifest = json.loads((directory / "manifest.json").read_text())
    if (manifest.get("schema") != 1 or manifest.get("group") != group
            or manifest.get("sqlrec_commit") != check_commit(sqlrec_commit)):
        raise ValueError(f"Wrong artifact group or SQLRec commit in {directory}")
    recorded_sources = source_commits([f"{key}={value}" for key, value in manifest["sources"].items()], group)
    if sources is not None and recorded_sources != source_commits(sources, group):
        raise ValueError(f"Source revision mismatch in {directory}")
    if manifest["wheels"] != inspect_group(directory, group):
        raise ValueError(f"Wheel checksum or metadata mismatch in {directory}")
    return manifest


def copy_wheel(source, destination):
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / source.name
    if target.exists() and sha256(source) != sha256(target):
        raise ValueError(f"Refusing to overwrite a different wheel: {target}")
    shutil.copy2(source, target)


def assemble(artifacts, output, sqlrec_commit):
    manifests = {
        group: verify(artifacts / f"model-wheels-{group}", group, sqlrec_commit)
        for group in GROUPS
    }
    if manifests["juicefs-amd64"]["sources"] != manifests["juicefs-arm64"]["sources"]:
        raise ValueError("JuiceFS architectures were built from different source revisions")
    wheels = [wheel for manifest in manifests.values() for wheel in manifest["wheels"]]
    if len({wheel["filename"] for wheel in wheels}) != len(wheels):
        raise ValueError("Duplicate release wheel filenames")
    jars = sorted((artifacts / "release-jars").rglob("*.jar"))
    for package in ("sqlrec-flink", "sqlrec-frontend"):
        matches = [path for path in jars if path.name.startswith(package + "-")]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one {package} release JAR; found {len(matches)}")
    if len(jars) != 2 or any(not zipfile.is_zipfile(path) for path in jars):
        raise ValueError("Expected two valid release JARs")
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"Release output directory must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    for group, manifest in manifests.items():
        for wheel in manifest["wheels"]:
            copy_wheel(artifacts / f"model-wheels-{group}" / wheel["filename"], output)
    for path in jars:
        shutil.copy2(path, output / path.name)
    sources = {}
    for manifest in manifests.values():
        for name, commit in manifest["sources"].items():
            sources[name] = {"repository": SOURCE_REPOS[name], "commit": commit}
    release = {
        "schema": 1, "sqlrec_commit": sqlrec_commit, "sources": sources,
        "wheels": sorted(wheels, key=lambda wheel: wheel["filename"]),
        "jars": [{"filename": path.name, "sha256": sha256(path)} for path in jars],
    }
    (output / "sources.json").write_text(json.dumps(release, indent=2, sort_keys=True) + "\n")
    assets = sorted(output.iterdir())
    (output / "SHA256SUMS").write_text("".join(f"{sha256(path)}  {path.name}\n" for path in assets))
    notes = [
        "JARs and Python wheels built and tested together for this SQLRec release.", "",
        "| Package | Version | Wheel tags |", "| --- | --- | --- |",
        *[f"| {wheel['name']} | {wheel['version']} | {', '.join(wheel['tags'])} |" for wheel in release["wheels"]],
        "", "JuiceFS wheels target CPython 3.11 on Debian Bookworm Linux (glibc 2.36).",
        "The pyfg and graphlearn wheels provide the SQLRec ARM compatibility subset for Python 3.11.",
        "Their cityhash dependency is installed from PyPI.",
        "See sources.json for source revisions and SHA256SUMS for asset checksums.", "",
    ]
    (output / "RELEASE_NOTES.md").write_text("\n".join(notes))
    return release


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("record", "verify"):
        sub = commands.add_parser(command)
        sub.add_argument("--directory", type=Path, required=True)
        sub.add_argument("--group", choices=GROUPS, required=True)
        sub.add_argument("--sqlrec-commit", required=True)
        sub.add_argument("--source", action="append", default=None, required=command == "record")
        if command == "verify":
            sub.add_argument("--copy-to", type=Path)
    sub = commands.add_parser("assemble")
    sub.add_argument("--artifacts-dir", type=Path, required=True)
    sub.add_argument("--output-dir", type=Path, required=True)
    sub.add_argument("--sqlrec-commit", required=True)
    args = parser.parse_args()
    try:
        if args.command == "assemble":
            manifest = assemble(args.artifacts_dir, args.output_dir, args.sqlrec_commit)
        elif args.command == "record":
            manifest = record(args.directory, args.group, args.sqlrec_commit, args.source)
        else:
            manifest = verify(args.directory, args.group, args.sqlrec_commit, args.source)
            if args.copy_to:
                for wheel in manifest["wheels"]:
                    copy_wheel(args.directory / wheel["filename"], args.copy_to)
    except (ValueError, KeyError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f"Artifact validation failed: {error}\n")
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
