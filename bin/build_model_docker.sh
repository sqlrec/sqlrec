#!/usr/bin/env bash
set -eo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
# main sets target, arch, artifact_dir, and work_dir for the build helpers.

select_docker_engine() {
  # On macOS without Docker Desktop, use the native Docker daemon in Minikube.
  if ! docker info >/dev/null 2>&1; then
    if command -v minikube >/dev/null 2>&1 && minikube -p minikube status >/dev/null 2>&1; then
      eval "$(minikube -p minikube docker-env)"
    fi
  fi
  docker info >/dev/null
}

normalize_arch() {
  case "$1" in
    arm64|aarch64) printf 'arm64\n' ;;
    amd64|x86_64) printf 'amd64\n' ;;
    *) echo "Unsupported architecture: $1" >&2; return 1 ;;
  esac
}

detect_arch_and_check_builder() {
  # The CLI may run on a different architecture from a remote native Docker
  # server. The builder must still use that server, without QEMU.
  arch="$(normalize_arch "$(docker info --format '{{.Architecture}}')")"
  local builder_info builder_driver builder_endpoint docker_context docker_endpoint
  builder_info="$(docker buildx inspect)"
  builder_driver="$(printf '%s\n' "$builder_info" | sed -n 's/^Driver:[[:space:]]*//p')"
  builder_endpoint="$(printf '%s\n' "$builder_info" | sed -n 's/^Endpoint:[[:space:]]*//p')"
  docker_context="$(docker context show)"
  docker_endpoint="${DOCKER_HOST:-$(docker context inspect "$docker_context" --format '{{ (index .Endpoints "docker").Host }}')}"
  if [[ "$builder_driver" != docker && "$builder_driver" != docker-container ]] ||
     [[ -z "$builder_endpoint" || "$builder_endpoint" == *$'\n'* ]] ||
     [[ "$builder_endpoint" != "$docker_context" && "$builder_endpoint" != "$docker_endpoint" ]]; then
    echo "Buildx builder must use the selected Docker engine; current builder: $builder_driver at $builder_endpoint." >&2
    return 1
  fi
}

require_one_wheel() {
  local directory="$1" pattern="$2" matches=()
  shopt -s nullglob
  matches=("$directory"/$pattern)
  shopt -u nullglob
  if [[ "${#matches[@]}" -ne 1 ]]; then
    echo "Expected exactly one $pattern in $directory; found ${#matches[@]}" >&2
    return 1
  fi
  printf '%s\n' "${matches[0]}"
}

update_sources() {
  local path
  for path in "$@"; do
    if [[ -e "$path/.git" ]] && [[ -n "$(git -C "$path" status --porcelain)" ]]; then
      echo "Submodule $path has local changes; commit or stash them before building." >&2
      return 1
    fi
  done
  git submodule update --init --depth 1 -- "$@"
  if [[ "${MODEL_SOURCE_UPDATE:-1}" == 1 ]]; then
    local branch
    for path in "$@"; do
      branch="$(git config -f .gitmodules --get "submodule.${path##*/}.branch")"
      git -C "$path" fetch --depth 1 origin "refs/heads/$branch"
      git -C "$path" checkout --detach FETCH_HEAD
    done
  fi
  for path in "$@"; do
    echo "$path: $(git -C "$path" rev-parse HEAD)"
  done
}

snapshot_source() {
  local name="$1" destination="$2"
  mkdir -p "$destination"
  git -C "$name" archive HEAD | tar -x -C "$destination"
}

build_juicefs_wheel() {
  local python_version="$1" destination="$artifact_dir/juicefs-py$1"
  rm -rf "$destination"
  mkdir -p "$destination"
  docker buildx build --platform "linux/$arch" \
    --file "$repo_root/docker/model-wheel-juicefs.Dockerfile" \
    --build-arg "PYTHON_VERSION=$python_version" \
    --target wheels --output "type=local,dest=$destination" \
    "$work_dir/juicefs-src"
  require_one_wheel "$destination" "juicefs-*-cp${python_version//./}-cp${python_version//./}-linux_*.whl" >/dev/null
}

build_tzrec_wheel() {
  local destination="$artifact_dir/tzrec"
  rm -rf "$destination"
  mkdir -p "$destination"
  docker buildx build --platform "linux/$arch" \
    --file "$repo_root/docker/model-wheel-tzrec.Dockerfile" \
    --target wheels --output "type=local,dest=$destination" \
    "$work_dir/tzrec-src"
  require_one_wheel "$destination" 'tzrec-*-none-any.whl' >/dev/null
}

build_compat_wheels() {
  local destination="$artifact_dir/compat"
  rm -rf "$destination"
  mkdir -p "$destination"
  docker buildx build --platform "linux/$arch" \
    --file "$repo_root/submodules/compat-src/Dockerfile" \
    --target wheels --output "type=local,dest=$destination" \
    "$repo_root/submodules/compat-src"
  require_one_wheel "$destination" 'pyfg-*-none-any.whl' >/dev/null
  require_one_wheel "$destination" 'graphlearn-*-none-any.whl' >/dev/null
  require_one_wheel "$destination" 'pyfarmhash-*-cp311-cp311-linux_*.whl' >/dev/null
}

prepare_tzrec_context() {
  local context="$1"
  mkdir -p "$context"
  cp "$(require_one_wheel "$artifact_dir/juicefs-py3.11" 'juicefs-*-cp311-cp311-linux_*.whl')" "$context/"
  cp "$(require_one_wheel "$artifact_dir/tzrec" 'tzrec-*-none-any.whl')" "$context/"
  if [[ "$arch" == arm64 ]]; then
    cp "$(require_one_wheel "$artifact_dir/compat" 'pyfg-*-none-any.whl')" "$context/"
    cp "$(require_one_wheel "$artifact_dir/compat" 'graphlearn-*-none-any.whl')" "$context/"
    cp "$(require_one_wheel "$artifact_dir/compat" 'pyfarmhash-*-cp311-cp311-linux_*.whl')" "$context/"
  fi
  mkdir -p "$context/sqlrec-model/src/main/python/tzrec"
  cp "$repo_root"/sqlrec-model/src/main/python/tzrec/*.py \
    "$repo_root"/sqlrec-model/src/main/python/tzrec/*.sh \
    "$context/sqlrec-model/src/main/python/tzrec/"
}

prepare_gbdt_context() {
  local context="$1"
  mkdir -p "$context"
  cp "$(require_one_wheel "$artifact_dir/juicefs-py3.10" 'juicefs-*-cp310-cp310-linux_*.whl')" "$context/"
  mkdir -p "$context/sqlrec-model/src/main/cpp" "$context/sqlrec-model/src/main/python"
  cp -R "$repo_root/sqlrec-model/src/main/cpp/gbdt" "$repo_root/sqlrec-model/src/main/cpp/common" \
    "$context/sqlrec-model/src/main/cpp/"
  cp -R "$repo_root/sqlrec-model/src/main/python/gbdt" "$repo_root/sqlrec-model/src/main/python/common" \
    "$context/sqlrec-model/src/main/python/"
}

prepare_transformers_context() {
  local context="$1"
  mkdir -p "$context/sqlrec-model/src/main/python"
  cp -R "$repo_root/sqlrec-model/src/main/python/huggingface" \
    "$context/sqlrec-model/src/main/python/"
}

build_and_verify_image() {
  local image="$1" dockerfile="$2" context="$3" actual_arch
  docker buildx build --platform "linux/$arch" --load \
    --tag "$image" --file "$dockerfile" "$context"
  actual_arch="$(normalize_arch "$(docker image inspect --format '{{.Architecture}}' "$image")")"
  [[ "$actual_arch" == "$arch" ]] || { echo "Unexpected image architecture: $actual_arch" >&2; return 1; }
  echo "Built $image ($actual_arch)"
}

prepare_sources() {
  local sources=()
  if [[ "$target" == all || "$target" == tzrec || "$target" == gbdt ]]; then
    sources+=(submodules/juicefs-src)
  fi
  if [[ "$target" == all || "$target" == tzrec ]]; then
    sources+=(submodules/tzrec-src)
    if [[ "$arch" == arm64 ]]; then
      sources+=(submodules/compat-src)
    fi
  fi
  if [[ "${#sources[@]}" -gt 0 ]]; then
    update_sources "${sources[@]}"
  fi

  # The wheel Dockerfiles see committed source only, without ignored build
  # artifacts or local files from a submodule checkout.
  if [[ "$target" == all || "$target" == tzrec || "$target" == gbdt ]]; then
    snapshot_source submodules/juicefs-src "$work_dir/juicefs-src"
  fi
  if [[ "$target" == all || "$target" == tzrec ]]; then
    snapshot_source submodules/tzrec-src "$work_dir/tzrec-src"
  fi
}

build_tzrec() {
  local context="$work_dir/context-tzrec"
  build_juicefs_wheel 3.11
  build_tzrec_wheel
  if [[ "$arch" == arm64 ]]; then
    build_compat_wheels
  fi
  prepare_tzrec_context "$context"
  build_and_verify_image "sqlrec/tzrec:${SQLREC_VERSION}-cpu" \
    "$repo_root/docker/sqlrec-model-tzrec.Dockerfile" "$context"
}

build_gbdt() {
  local context="$work_dir/context-gbdt"
  build_juicefs_wheel 3.10
  prepare_gbdt_context "$context"
  build_and_verify_image "sqlrec/gbdt:${SQLREC_VERSION}-cpu" \
    "$repo_root/docker/sqlrec-model-gbdt.Dockerfile" "$context"
}

build_transformers() {
  local context="$work_dir/context-transformers"
  prepare_transformers_context "$context"
  build_and_verify_image "sqlrec/transformers:${SQLREC_VERSION}" \
    "$repo_root/docker/sqlrec-model-transformers.Dockerfile" "$context"
}

main() {
  target="${1:-all}"
  case "$target" in
    all|tzrec|gbdt|transformers) ;;
    *) echo "Usage: $0 [all|tzrec|gbdt|transformers]" >&2; return 2 ;;
  esac

  source "$repo_root/deploy/env.sh"
  cd "$repo_root"
  select_docker_engine
  detect_arch_and_check_builder
  echo "Building $target for native linux/$arch on the selected Docker engine"

  docker image prune -f

  artifact_dir="$repo_root/build/model-wheels/$arch"
  mkdir -p "$artifact_dir" "$repo_root/build"
  work_dir="$(mktemp -d "$repo_root/build/model-image.XXXXXX")"
  trap 'rm -rf "$work_dir"' EXIT
  prepare_sources

  case "$target" in
    all) build_tzrec; build_gbdt; build_transformers ;;
    tzrec) build_tzrec ;;
    gbdt) build_gbdt ;;
    transformers) build_transformers ;;
  esac
}

main "$@"
