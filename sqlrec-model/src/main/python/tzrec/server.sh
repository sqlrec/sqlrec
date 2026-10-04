#!/bin/bash
set -euo pipefail

fail() {
  echo "TZRec startup error: $*" >&2
  exit 1
}

model_dir=""
host="0.0.0.0"
port="80"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scripted_model_dir|--host|--port)
      [[ $# -ge 2 ]] || fail "Missing value for $1"
      case "$1" in
        --scripted_model_dir) model_dir="$2" ;;
        --host) host="$2" ;;
        --port) port="$2" ;;
      esac
      shift 2
      ;;
    --scripted_model_dir=*) model_dir="${1#*=}"; shift ;;
    --host=*) host="${1#*=}"; shift ;;
    --port=*) port="${1#*=}"; shift ;;
    -h|--help)
      echo "Usage: $0 --scripted_model_dir DIR [--host HOST] [--port PORT]"
      echo "TZREC_SERVING_BACKEND=cpp|python (default: cpp)"
      exit 0
      ;;
    *) fail "Unknown argument: $1" ;;
  esac
done
[[ -n "$model_dir" ]] || fail "--scripted_model_dir is required"
[[ -n "$host" ]] || fail "--host must not be empty"
[[ "$port" =~ ^[0-9]{1,5}$ ]] || fail "port must be in [1, 65535]"
port=$((10#$port))
(( port >= 1 && port <= 65535 )) || fail "port must be in [1, 65535]"

backend="${TZREC_SERVING_BACKEND-cpp}"
case "$backend" in
  cpp|python) ;;
  *) fail "TZREC_SERVING_BACKEND must be cpp or python" ;;
esac

# Only remove the private directory allocated by this startup if it fails.
# Successful exec keeps the files available for the serving process.
download_root=""
cleanup() {
  if [[ -n "$download_root" ]]; then
    rm -rf -- "$download_root"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$model_dir" == *://* ]]; then
  if [[ -n "${HADOOP_HOME:-}" ]]; then
    hadoop="${HADOOP_HOME}/bin/hadoop"
  else
    hadoop="$(command -v hadoop)" || fail "Set HADOOP_HOME or put hadoop on PATH"
  fi
  [[ -x "$hadoop" ]] || fail "Hadoop executable not found: $hadoop"
  cache="${LOCAL_CACHE_DIR:-/tmp/tzrec_model_cache}"
  mkdir -p -- "$cache"
  cache="$(cd -- "$cache" && pwd -P)"
  download_root="$(mktemp -d "$cache/export-XXXXXX")"
  # An absent destination avoids an extra source-directory layer in the cache.
  local_model_dir="$download_root/model"
  echo "Downloading $model_dir to $local_model_dir using Hadoop"
  "$hadoop" fs -get "$model_dir" "$local_model_dir"
else
  [[ -d "$model_dir" ]] || fail "Model directory not found: $model_dir"
  local_model_dir="$(cd -- "$model_dir" && pwd -P)"
fi

for file in scripted_model.pt fg.json pipeline.config; do
  [[ -f "$local_model_dir/$file" ]] || fail "Export is missing $file: $local_model_dir"
done

app_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
echo "Starting TZRec $backend serving with model $local_model_dir"
if [[ "$backend" == "python" ]]; then
  exec python "$app_dir/server.py" --scripted_model_dir "$local_model_dir" --host "$host" --port "$port"
else
  exec "$app_dir/tzrec_server" "$local_model_dir" "$host" "$port"
fi
