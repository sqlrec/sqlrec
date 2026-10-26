#!/usr/bin/env bash
set -euo pipefail

release_dir="${1:?Specify the release assets directory}"
RELEASE_TAG="${2:?Specify the release tag}"
scratch="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/sqlrec-release.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
state="$scratch/state.json"
if ! gh release view "$RELEASE_TAG" --json isDraft,assets > "$state"; then
  gh release create "$RELEASE_TAG" --verify-tag --draft \
    --title "$RELEASE_TAG" --notes-file "$release_dir/RELEASE_NOTES.md"
  gh release view "$RELEASE_TAG" --json isDraft,assets > "$state"
fi
files=("$release_dir"/*.jar "$release_dir"/*.whl "$release_dir/SHA256SUMS" "$release_dir/sources.json")
printf '%s\n' "${files[@]##*/}" | sort > "$scratch/expected-assets"
jq -r '.assets[].name' "$state" | sort > "$scratch/actual-assets"
if [[ "$(jq -r '.isDraft' "$state")" == false ]]; then
  gh release download "$RELEASE_TAG" --dir "$scratch/published"
  if ! cmp -s "$release_dir/SHA256SUMS" "$scratch/published/SHA256SUMS" || \
     ! cmp -s "$scratch/expected-assets" "$scratch/actual-assets"; then
    echo "::error::Published release differs from these artifacts; use a new tag."
    exit 1
  fi
  python3 - "$scratch/published" <<'PY'
import hashlib
from pathlib import Path
import sys
root = Path(sys.argv[1])
for line in (root / "SHA256SUMS").read_text().splitlines():
    expected, name = line.split("  ", 1)
    if Path(name).name != name:
        raise ValueError(f"Invalid checksum filename: {name}")
    digest = hashlib.sha256()
    with (root / name).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != expected:
        raise ValueError(f"Published asset checksum mismatch: {name}")
PY
  echo "Release $RELEASE_TAG already contains these artifacts."
  exit 0
fi
# A retried draft may contain filenames from an earlier build attempt.
while IFS= read -r asset; do
  gh release delete-asset "$RELEASE_TAG" "$asset" --yes
done < <(comm -23 "$scratch/actual-assets" "$scratch/expected-assets")
gh release upload "$RELEASE_TAG" "${files[@]}" --clobber
gh release view "$RELEASE_TAG" --json assets --jq '.assets[].name' | sort > "$scratch/actual-assets"
cmp "$scratch/expected-assets" "$scratch/actual-assets"
gh release edit "$RELEASE_TAG" --draft=false --notes-file "$release_dir/RELEASE_NOTES.md"
