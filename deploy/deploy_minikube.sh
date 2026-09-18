#!/bin/bash
set -exo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

MINIKUBE_PROFILE=minikube

die() {
  echo "ERROR: $*" >&2
  exit 1
}

prepare_directories() {
  mkdir -p "${CONF_DIR}" "${LIB_DIR}" "${CLIENT_DIR}" "${PV_DIR}" "${IMAGE_CACHE_DIR}"
}

minikube_for_profile() {
  minikube -p "${MINIKUBE_PROFILE}" "$@"
}

find_vmnet_helper() {
  local helper_path helper_prefix
  helper_path="$(command -v vmnet-helper 2>/dev/null || true)"
  if [ -n "${helper_path}" ] && [ -x "${helper_path}" ]; then
    printf '%s\n' "${helper_path}"
    return 0
  fi

  if command -v brew >/dev/null 2>&1; then
    helper_prefix="$(brew --prefix vmnet-helper 2>/dev/null || true)"
    helper_path="${helper_prefix}/libexec/vmnet-helper"
    if [ -n "${helper_prefix}" ] && [ -x "${helper_path}" ]; then
      printf '%s\n' "${helper_path}"
      return 0
    fi
  fi

  if [ -x /opt/vmnet-helper/bin/vmnet-helper ]; then
    printf '%s\n' /opt/vmnet-helper/bin/vmnet-helper
    return 0
  fi

  return 1
}

install_linux_dependencies() {
  local command_name docker_user
  local missing_cli=false
  for command_name in curl tar gzip envsubst psql; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
      missing_cli=true
      break
    fi
  done

  if [ "${missing_cli}" = true ]; then
    if command -v apt-get >/dev/null 2>&1; then
      run_privileged apt-get update
      run_privileged apt-get install -y ca-certificates curl tar gzip gettext-base postgresql-client
    elif command -v dnf >/dev/null 2>&1; then
      run_privileged dnf install -y ca-certificates curl tar gzip gettext postgresql
    elif command -v yum >/dev/null 2>&1; then
      run_privileged yum install -y ca-certificates curl tar gzip gettext postgresql
    else
      echo "Install curl, tar, gzip, envsubst, and psql, then rerun this script." >&2
      die "cannot install required Linux commands automatically."
    fi
  fi

  require_commands curl tar gzip envsubst psql ||
    die "required Linux commands are still missing after installation."

  # refer to https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository
  if command -v docker >/dev/null 2>&1; then
    echo 'skip install docker'
  else
    download_file https://get.docker.com "${CLIENT_DIR}/get-docker.sh"
    run_privileged sh "${CLIENT_DIR}/get-docker.sh"
    docker_user="${SUDO_USER:-${USER:-}}"
    if [ -n "${docker_user}" ] && [ "${docker_user}" != root ]; then
      run_privileged usermod -aG docker "${docker_user}"
    fi
    die "Docker was installed. Log out and back in, then rerun this script."
  fi

  docker info >/dev/null 2>&1 ||
    die "Docker is installed but the daemon is unavailable or the current user cannot access it."

  if command -v minikube >/dev/null 2>&1; then
    echo 'skip install minikube'
  else
    if [ ! -f "${CLIENT_DIR}/${MINIKUBE_ARCH_NAME}" ]; then
      download_file "${MINIKUBE_URL}" "${CLIENT_DIR}/${MINIKUBE_ARCH_NAME}"
    fi
    run_privileged install "${CLIENT_DIR}/${MINIKUBE_ARCH_NAME}" /usr/local/bin/minikube
  fi

  if command -v helm >/dev/null 2>&1; then
    echo 'skip install helm'
  else
    download_file https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 "${CLIENT_DIR}/get-helm-3.sh"
    bash "${CLIENT_DIR}/get-helm-3.sh"
  fi

  # env.sh ran before a first-time Minikube installation. Configure the
  # bundled kubectl fallback now that the minikube command is available.
  configure_host_tools
  require_commands docker minikube kubectl helm ||
    die "required Linux deployment commands are still missing after installation."
}

install_macos_dependencies() {
  [ "${DEPLOY_ARCH}" = arm64 ] ||
    die "the macOS deployment currently supports Apple Silicon only."

  local macos_major
  macos_major="$(sw_vers -productVersion | cut -d. -f1)"
  [ "${macos_major}" -ge 14 ] || die "vfkit requires macOS 14 or later."

  command -v brew >/dev/null 2>&1 ||
    die "Homebrew is required. Install it from https://brew.sh/ and rerun."

  local formula
  local missing_formulae=()
  for formula in minikube vfkit docker docker-buildx helm gettext libpq; do
    if ! brew list --formula "${formula}" >/dev/null 2>&1; then
      missing_formulae+=("${formula}")
    fi
  done
  if [ "${#missing_formulae[@]}" -gt 0 ]; then
    echo "Installing missing Homebrew packages: ${missing_formulae[*]}"
    brew install "${missing_formulae[@]}"
  fi

  configure_host_tools

  require_commands minikube vfkit docker docker-buildx kubectl helm envsubst psql ||
    die "required commands are still missing after Homebrew installation."

  # Homebrew installs Buildx outside Docker's default macOS plugin directory.
  # Add a user-scoped symlink without overwriting an existing plugin.
  if ! docker buildx version >/dev/null 2>&1; then
    local buildx_source buildx_target
    buildx_source="$(brew --prefix docker-buildx)/bin/docker-buildx"
    buildx_target="${HOME}/.docker/cli-plugins/docker-buildx"
    mkdir -p "${HOME}/.docker/cli-plugins"
    if [ -e "${buildx_target}" ] || [ -L "${buildx_target}" ]; then
      echo "ERROR: Docker Buildx exists at ${buildx_target} but cannot be loaded." >&2
      die "Resolve the existing plugin and rerun this script."
    fi
    ln -s "${buildx_source}" "${buildx_target}"
    docker buildx version >/dev/null
  fi

  if ! find_vmnet_helper >/dev/null; then
    echo "Installing vmnet-helper for vmnet-shared networking..."
    if [ "${macos_major}" -ge 26 ]; then
      brew tap nirs/vmnet-helper
      brew trust nirs/vmnet-helper
      brew install vmnet-helper
    else
      download_file \
        https://github.com/minikube-machine/vmnet-helper/releases/latest/download/install.sh \
        "${CLIENT_DIR}/install-vmnet-helper.sh"
      bash "${CLIENT_DIR}/install-vmnet-helper.sh"
    fi
  fi
  local vmnet_helper
  vmnet_helper="$(find_vmnet_helper || true)"
  [ -n "${vmnet_helper}" ] ||
    die "vmnet-helper installation did not provide an executable."
  prepend_path "$(dirname "${vmnet_helper}")"
  echo "Using vmnet-helper at ${vmnet_helper}"

  local minikube_version
  minikube_version="$(minikube version --short)"
  version_at_least "${minikube_version}" 1.37.0 ||
    die "Minikube 1.37.0 or later is required for vfkit VirtioFS mounts."
}

install_host_dependencies() {
  case "${DEPLOY_OS}" in
    linux) install_linux_dependencies ;;
    darwin) install_macos_dependencies ;;
  esac
}

start_minikube() {
  local driver
  local args=(
    --container-runtime=docker
    --cpus="${MINIKUBE_CPUS}"
    --memory="${MINIKUBE_MEMORY}"
    --disk-size="${MINIKUBE_DISK_SIZE}"
    --mount
    --mount-string="${DATA_DIR}:${DATA_DIR}"
  )

  if [ "${DEPLOY_OS}" = linux ]; then
    driver=docker
    args+=(
      --driver="${driver}"
      --ports="${PORT_RANGE_START}-${PORT_RANGE_END}:${PORT_RANGE_START}-${PORT_RANGE_END}"
      --ports="${REDIS_CLUSTER_BUS_PORT_RANGE_START}-${REDIS_CLUSTER_BUS_PORT_RANGE_END}:${REDIS_CLUSTER_BUS_PORT_RANGE_START}-${REDIS_CLUSTER_BUS_PORT_RANGE_END}"
    )
  else
    driver=vfkit
    args+=(--driver="${driver}" --network=vmnet-shared)
  fi

  if ! minikube_for_profile start "${args[@]}"; then
    if [ "${DEPLOY_OS}" = darwin ]; then
      echo "An existing profile created with another driver must be deleted manually with 'minikube delete'." >&2
    fi
    die "failed to start Minikube with the ${driver} driver."
  fi
}

configure_cluster() {
  NODE_IP="$(minikube_for_profile ip)"
  [ -n "${NODE_IP}" ] || die "Minikube did not report a node IP."

  export NODE_IP
  export K8S_APISERVER_ADDR="k8s://https://${NODE_IP}:8443"
}

verify_cluster_network() {
  if command -v nc >/dev/null 2>&1 && ! nc -z -w 5 "${NODE_IP}" 8443; then
    if [ "${DEPLOY_OS}" = darwin ]; then
      echo "Check vmnet-helper, VPN routes, and the macOS firewall." >&2
    fi
    die "Minikube node IP ${NODE_IP} is not directly reachable from the host."
  fi
}

verify_data_mount() {
  local mount_probe="${DATA_DIR}/.sqlrec-mount-probe-$$"
  touch "${mount_probe}"

  if ! minikube_for_profile ssh -- test -f "${mount_probe}"; then
    rm -f "${mount_probe}"
    die "${DATA_DIR} is not mounted into Minikube at the same path."
  fi

  rm -f "${mount_probe}"
}

verify_minikube_docker() {
  # Both host platforms use the Docker daemon inside Minikube. Docker Desktop
  # is not required on macOS.
  if ! (eval "$(minikube_for_profile docker-env)" && docker info >/dev/null); then
    die "Docker CLI cannot connect to the Minikube Docker daemon."
  fi
}

verify_minikube() {
  verify_cluster_network
  verify_data_mount
  verify_minikube_docker
}

install_local_path_provisioner() {
  case "${LOCAL_PATH_PROVISIONER_DATA_DIR}" in
    /*) ;;
    *) die "LOCAL_PATH_PROVISIONER_DATA_DIR must be an absolute path." ;;
  esac

  # Do not patch Minikube's storage-provisioner-rancher addon: enabling that
  # addon again restores its /opt/local-path-provisioner ConfigMap. Install a
  # project-managed provisioner whose configuration survives restarts.
  if minikube_for_profile addons list -o json |
    grep -q '"storage-provisioner-rancher":{[^}]*"Status":"enabled"'; then
    minikube_for_profile addons disable storage-provisioner-rancher
    kubectl wait --for=delete namespace/local-path-storage \
      --timeout="${DEPLOY_TIMEOUT}s"
  fi

  render_config "${dir}/local-path-provisioner.values.yaml" \
    '${LOCAL_PATH_PROVISIONER_DATA_DIR}'
  helm upgrade --install local-path-provisioner \
    "${LOCAL_PATH_PROVISIONER_CHART}" \
    --version "${LOCAL_PATH_PROVISIONER_VERSION}" \
    --namespace local-path-storage \
    --create-namespace \
    --values "${dir}/local-path-provisioner.values.yaml.tmp" \
    --wait \
    --timeout "${DEPLOY_TIMEOUT}s"

  # Keep local-path as the only default class when Minikube's standard
  # provisioner is also enabled.
  if kubectl get storageclass standard >/dev/null 2>&1; then
    kubectl annotate storageclass standard \
      storageclass.kubernetes.io/is-default-class=false \
      --overwrite
  fi
}

main() {
  prepare_directories
  install_host_dependencies
  configure_minikube_resources || die "failed to configure Minikube resources."
  start_minikube
  configure_cluster
  verify_minikube
  bash "${dir}/cache_images.sh" load
  install_local_path_provisioner

  echo "Minikube is ready at ${NODE_IP}"
  echo 'deploy minikube done'
}

main "$@"
