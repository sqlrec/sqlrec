#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
image="${1:?Usage: verify_tzrec_image.sh IMAGE [amd64|arm64]}"
expected_arch="${2:-}"
actual_arch="$(docker run --rm --entrypoint uname "$image" -m)"
if [[ -n "$expected_arch" ]]; then
  case "$expected_arch:$actual_arch" in
    amd64:x86_64|arm64:aarch64) ;;
    *) echo "Unexpected image architecture: $actual_arch" >&2; exit 1 ;;
  esac
fi

container=""
cleanup() {
  if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null; fi
}
trap cleanup EXIT
container="$(docker create --env USE_FARM_HASH_TO_BUCKETIZE=true --env PYTHONPATH=/app \
  --entrypoint bash "$image" -euo pipefail -c '
  test -x /app/tzrec_server
  test -x /app/features_test
  bash -n /app/server.sh
  links="$(ldd /app/tzrec_server 2>&1)"
  if [[ "$links" == *"not found"* ]]; then printf "%s\n" "$links"; exit 1; fi
  python -c "import ctypes, pathlib, flask, graphlearn, juicefs, pyarrow, pyfg, server, torch, torchrec, tzrec.main; ctypes.CDLL(str(pathlib.Path(juicefs.__file__).with_name(\"libjfs.so\")))"
  python /tests/tzrec_pipeline_config_tests.py
  python /tests/tzrec_dssm_tests.py
  python /tests/tzrec_native_smoke.py
  python /tests/tzrec_pipeline_smoke.py
')"
docker cp "$repo_root/sqlrec-model/src/test/python/." "$container:/tests"
docker cp "$repo_root/sqlrec-model/src/test/resources/tzrec" "$container:/tests/configs"
docker start --attach "$container"
exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container")"
[[ "$exit_code" == 0 ]] || exit "$exit_code"
