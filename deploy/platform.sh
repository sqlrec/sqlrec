#!/bin/bash
# Host-specific setup used by env.sh. This file defines functions only.

run_privileged() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        echo "ERROR: root privileges are required to run: $*" >&2
        return 1
    fi
}

detect_deploy_platform() {
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"

    case "${os}" in
        Linux) export DEPLOY_OS=linux ;;
        Darwin) export DEPLOY_OS=darwin ;;
        *)
            echo "ERROR: unsupported operating system: ${os}" >&2
            return 1
            ;;
    esac

    case "${arch}" in
        x86_64|amd64) export DEPLOY_ARCH=amd64 ;;
        arm64|aarch64) export DEPLOY_ARCH=arm64 ;;
        *)
            echo "ERROR: unsupported architecture: ${arch}" >&2
            return 1
            ;;
    esac
}

configure_cluster_address() {
    export NODE_IP="${NODE_IP:-}"
    if [ -z "${NODE_IP}" ] && command -v minikube >/dev/null 2>&1; then
        export NODE_IP="$(minikube -p minikube ip 2>/dev/null || true)"
    fi
    if [ -n "${NODE_IP}" ]; then
        export K8S_APISERVER_ADDR="k8s://https://${NODE_IP}:8443"
    fi
}

configure_minikube_resources() {
    if [ "${DEPLOY_OS}" = linux ]; then
        export MINIKUBE_CPUS="${MINIKUBE_CPUS:-no-limit}"
        export MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-no-limit}"
        return
    fi

    if [ -z "${MINIKUBE_CPUS:-}" ]; then
        local host_cpus
        if ! host_cpus="$(sysctl -n hw.physicalcpu)" || [ -z "${host_cpus}" ]; then
            echo "ERROR: unable to determine host CPU count with sysctl." >&2
            return 1
        fi
        export MINIKUBE_CPUS="${host_cpus}"
    else
        export MINIKUBE_CPUS
    fi
    export MINIKUBE_MEMORY_PERCENT="${MINIKUBE_MEMORY_PERCENT:-80}"
    if [ -n "${MINIKUBE_MEMORY:-}" ]; then
        export MINIKUBE_MEMORY
        return
    fi

    case "${MINIKUBE_MEMORY_PERCENT}" in
        ''|*[!0-9]*)
            echo "ERROR: MINIKUBE_MEMORY_PERCENT must be an integer from 1 to 100." >&2
            return 1
            ;;
    esac
    if [ "${MINIKUBE_MEMORY_PERCENT}" -lt 1 ] || [ "${MINIKUBE_MEMORY_PERCENT}" -gt 100 ]; then
        echo "ERROR: MINIKUBE_MEMORY_PERCENT must be an integer from 1 to 100." >&2
        return 1
    fi

    local host_memory_bytes host_memory_mb
    if ! host_memory_bytes="$(sysctl -n hw.memsize)" || [ -z "${host_memory_bytes}" ]; then
        echo "ERROR: unable to determine host memory with sysctl." >&2
        return 1
    fi
    host_memory_mb=$((host_memory_bytes / 1024 / 1024))
    export MINIKUBE_MEMORY="$((host_memory_mb * MINIKUBE_MEMORY_PERCENT / 100))mb"
}

configure_java_distribution() {
    local java_platform
    if [ "${DEPLOY_OS}" = darwin ]; then
        export JAVA_CLIENT_URL="https://corretto.aws/downloads/resources/${JAVA_VERSION}/amazon-corretto-${JAVA_VERSION}-macosx-aarch64.tar.gz"
        export JAVA_CLIENT_ARCH_NAME="amazon-corretto-${JAVA_VERSION}-macosx-aarch64.tar.gz"
        export JAVA_CLIENT_DIR_NAME="amazon-corretto-8.jdk/Contents/Home"
        java_platform=linux-aarch64
    else
        case "${DEPLOY_ARCH}" in
            amd64) java_platform=linux-x64 ;;
            arm64) java_platform=linux-aarch64 ;;
        esac
        export JAVA_CLIENT_URL="https://corretto.aws/downloads/resources/${JAVA_VERSION}/amazon-corretto-${JAVA_VERSION}-${java_platform}.tar.gz"
        export JAVA_CLIENT_ARCH_NAME="amazon-corretto-${JAVA_VERSION}-${java_platform}.tar.gz"
        export JAVA_CLIENT_DIR_NAME="amazon-corretto-${JAVA_VERSION}-${java_platform}"
    fi

    export CONTAINER_JAVA_URL="https://corretto.aws/downloads/resources/${JAVA_VERSION}/amazon-corretto-${JAVA_VERSION}-${java_platform}.tar.gz"
    export CONTAINER_JAVA_ARCH_NAME="amazon-corretto-${JAVA_VERSION}-${java_platform}.tar.gz"
    export CONTAINER_JAVA_DIR_NAME="amazon-corretto-${JAVA_VERSION}-${java_platform}"
}

configure_host_tools() {
    local prefix
    if [ "${DEPLOY_OS}" = darwin ] && command -v brew >/dev/null 2>&1; then
        prefix="$(brew --prefix gettext 2>/dev/null || true)"
        [ -n "${prefix}" ] && prepend_path "${prefix}/bin"
        prefix="$(brew --prefix libpq 2>/dev/null || true)"
        [ -n "${prefix}" ] && prepend_path "${prefix}/bin"
    fi

    # Fall back to Minikube's bundled kubectl when no standalone CLI exists.
    if ! command -v kubectl >/dev/null 2>&1 && command -v minikube >/dev/null 2>&1; then
        kubectl() {
            minikube kubectl -- "$@"
        }
        if [ -n "${BASH_VERSION:-}" ]; then
            export -f kubectl
        fi
    fi
}
