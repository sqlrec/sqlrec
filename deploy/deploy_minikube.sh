#!/bin/bash
set -eo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

mkdir -p "${CONF_DIR}" "${LIB_DIR}" "${CLIENT_DIR}" "${PV_DIR}" "${IMAGE_CACHE_DIR}"

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
  if [ "${DEPLOY_ARCH}" != amd64 ]; then
    echo "ERROR: the Linux deployment currently supports AMD64 only." >&2
    exit 1
  fi

  # refer to https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository
  if command -v docker >/dev/null 2>&1; then
    echo 'skip install docker'
  else
    download_file https://get.docker.com "${CLIENT_DIR}/get-docker.sh"
    sudo sh "${CLIENT_DIR}/get-docker.sh"
    sudo usermod -aG docker "${USER}"
    echo "Docker was installed. Log out and back in, then rerun this script." >&2
    exit 1
  fi

  if command -v minikube >/dev/null 2>&1; then
    echo 'skip install minikube'
  else
    if [ ! -f "${CLIENT_DIR}/${MINIKUBE_ARCH_NAME}" ]; then
      download_file "${MINIKUBE_URL}" "${CLIENT_DIR}/${MINIKUBE_ARCH_NAME}"
    fi
    sudo install "${CLIENT_DIR}/${MINIKUBE_ARCH_NAME}" /usr/local/bin/minikube
  fi

  if command -v helm >/dev/null 2>&1; then
    echo 'skip install helm'
  else
    download_file https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 "${CLIENT_DIR}/get-helm-3.sh"
    bash "${CLIENT_DIR}/get-helm-3.sh"
  fi
}

check_macos_dependencies() {
  if [ "${DEPLOY_ARCH}" != arm64 ]; then
    echo "ERROR: the macOS deployment currently supports Apple Silicon only." >&2
    exit 1
  fi

  local macos_major
  macos_major="$(sw_vers -productVersion | cut -d. -f1)"
  if [ "${macos_major}" -lt 14 ]; then
    echo "ERROR: vfkit requires macOS 14 or later." >&2
    exit 1
  fi

  if ! command -v brew >/dev/null 2>&1; then
    echo "ERROR: Homebrew is required. Install it from https://brew.sh/ and rerun." >&2
    exit 1
  fi

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

  prepend_path "$(brew --prefix gettext)/bin"
  prepend_path "$(brew --prefix libpq)/bin"

  if ! require_commands minikube vfkit docker docker-buildx kubectl helm envsubst psql; then
    echo "ERROR: required commands are still missing after Homebrew installation." >&2
    exit 1
  fi

  # Homebrew installs Buildx outside Docker's default macOS plugin directory.
  # Add a user-scoped symlink without overwriting an existing plugin.
  if ! docker buildx version >/dev/null 2>&1; then
    local buildx_source buildx_target
    buildx_source="$(brew --prefix docker-buildx)/bin/docker-buildx"
    buildx_target="${HOME}/.docker/cli-plugins/docker-buildx"
    mkdir -p "${HOME}/.docker/cli-plugins"
    if [ -e "${buildx_target}" ] || [ -L "${buildx_target}" ]; then
      echo "ERROR: Docker Buildx exists at ${buildx_target} but cannot be loaded." >&2
      echo "Resolve the existing plugin and rerun this script." >&2
      exit 1
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
  if [ -z "${vmnet_helper}" ]; then
    echo "ERROR: vmnet-helper installation did not provide an executable." >&2
    exit 1
  fi
  prepend_path "$(dirname "${vmnet_helper}")"
  echo "Using vmnet-helper at ${vmnet_helper}"

  local minikube_version
  minikube_version="$(minikube version --short)"
  if ! version_at_least "${minikube_version}" 1.37.0; then
    echo "ERROR: Minikube 1.37.0 or later is required for vfkit VirtioFS mounts." >&2
    exit 1
  fi
}

start_linux_minikube() {
  minikube start \
    --driver=docker \
    --container-runtime=docker \
    --cpus="${MINIKUBE_CPUS}" \
    --memory="${MINIKUBE_MEMORY}" \
    --disk-size="${MINIKUBE_DISK_SIZE}" \
    --mount-string="${DATA_DIR}:${DATA_DIR}" \
    --ports="${PORT_RANGE_START}-${PORT_RANGE_END}:${PORT_RANGE_START}-${PORT_RANGE_END}" \
    --ports="${REDIS_CLUSTER_BUS_PORT_RANGE_START}-${REDIS_CLUSTER_BUS_PORT_RANGE_END}:${REDIS_CLUSTER_BUS_PORT_RANGE_START}-${REDIS_CLUSTER_BUS_PORT_RANGE_END}"
}

start_macos_minikube() {
  minikube start \
    --driver=vfkit \
    --network=vmnet-shared \
    --container-runtime=docker \
    --cpus="${MINIKUBE_CPUS}" \
    --memory="${MINIKUBE_MEMORY}" \
    --disk-size="${MINIKUBE_DISK_SIZE}" \
    --mount-string="${DATA_DIR}:${DATA_DIR}"
}

if [ "${DEPLOY_OS}" = linux ]; then
  install_linux_dependencies
  if ! start_linux_minikube; then
    echo "ERROR: failed to start Minikube with the Docker driver." >&2
    exit 1
  fi
else
  check_macos_dependencies
  if ! start_macos_minikube; then
    echo "ERROR: failed to start Minikube with the vfkit driver." >&2
    echo "An existing profile created with another driver must be deleted manually with 'minikube delete'." >&2
    exit 1
  fi
fi

NODE_IP="$(minikube -p minikube ip)"
export NODE_IP
if [ -z "${NODE_IP}" ]; then
  echo "ERROR: Minikube did not report a node IP." >&2
  exit 1
fi
if command -v nc >/dev/null 2>&1 && ! nc -z -w 5 "${NODE_IP}" 8443; then
  echo "ERROR: Minikube node IP ${NODE_IP} is not directly reachable from the host." >&2
  if [ "${DEPLOY_OS}" = darwin ]; then
    echo "Check vmnet-helper, VPN routes, and the macOS firewall." >&2
  fi
  exit 1
fi

# Verify that hostPath volumes will resolve to the same directory in the node.
mount_probe="${DATA_DIR}/.sqlrec-mount-probe-$$"
touch "${mount_probe}"
if ! minikube -p minikube ssh -- test -f "${mount_probe}"; then
  rm -f "${mount_probe}"
  echo "ERROR: ${DATA_DIR} is not mounted into Minikube at the same path." >&2
  exit 1
fi
rm -f "${mount_probe}"

# Both host platforms use the Docker daemon inside Minikube. Docker Desktop is
# not required on macOS.
if ! (eval "$(minikube -p minikube docker-env)" && docker info >/dev/null); then
  echo "ERROR: Docker CLI cannot connect to the Minikube Docker daemon." >&2
  exit 1
fi

bash "${dir}/cache_images.sh" load
minikube addons enable storage-provisioner-rancher

echo "Minikube is ready at ${NODE_IP}"
echo 'deploy minikube done'