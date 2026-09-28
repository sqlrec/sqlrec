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

discover_node_ip() {
    local nodes node_name unschedulable ready address taints
    local node_template='{range .items[*]}{.metadata.name}{"|"}{.spec.unschedulable}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"|"}{.status.addresses[?(@.type=="InternalIP")].address}{"|"}{.spec.taints[*].key}{"\n"}{end}'

    # Prefer a ready, uncordoned worker. Exclude both current and legacy
    # control-plane roles, including nodes identified only by a role taint.
    nodes="$(kubectl get nodes --request-timeout=5s \
        --selector='!node-role.kubernetes.io/control-plane,!node-role.kubernetes.io/master' \
        --sort-by=.metadata.name -o jsonpath="${node_template}" 2>/dev/null)" || return 1
    while IFS='|' read -r node_name unschedulable ready address taints; do
        [ "${ready}" = True ] && [ "${unschedulable}" != true ] && [ -n "${address}" ] || continue
        case " ${taints} " in
            *node-role.kubernetes.io/control-plane*|*node-role.kubernetes.io/master*) continue ;;
        esac
        printf '%s\n' "${address%% *}"
        return 0
    done <<< "${nodes}"

    # A single-node Minikube cluster also runs its workloads on the control plane.
    [ "$(kubectl config current-context 2>/dev/null)" = minikube ] || return 1
    nodes="$(kubectl get nodes --request-timeout=5s \
        -o jsonpath="${node_template}" 2>/dev/null)" || return 1
    [ "$(printf '%s\n' "${nodes}" | sed '/^$/d' | wc -l | tr -d '[:space:]')" = 1 ] || return 1
    IFS='|' read -r node_name unschedulable ready address taints <<< "${nodes}"
    [ "${ready}" = True ] && [ "${unschedulable}" != true ] && [ -n "${address}" ] || return 1
    printf '%s\n' "${address%% *}"
}

configure_cluster_address() {
    export NODE_IP="${NODE_IP:-}"
    export K8S_APISERVER_ADDR="${K8S_APISERVER_ADDR:-}"
    if command -v kubectl >/dev/null 2>&1; then
        if [ -z "${NODE_IP}" ]; then
            NODE_IP="$(discover_node_ip || true)"
            export NODE_IP
        fi
        if [ -z "${K8S_APISERVER_ADDR}" ]; then
            local api_server
            api_server="$(kubectl config view --minify \
                -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
            if [ -n "${api_server}" ]; then
                export K8S_APISERVER_ADDR="k8s://${api_server}"
            fi
        fi
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
    local java_platform container_platform
    if [ "${DEPLOY_OS}" = darwin ]; then
        export JAVA_CLIENT_URL="https://corretto.aws/downloads/resources/${JAVA_VERSION}/amazon-corretto-${JAVA_VERSION}-macosx-aarch64.tar.gz"
        export JAVA_CLIENT_ARCH_NAME="amazon-corretto-${JAVA_VERSION}-macosx-aarch64.tar.gz"
        export JAVA_CLIENT_DIR_NAME="amazon-corretto-8.jdk/Contents/Home"
    else
        case "${DEPLOY_ARCH}" in
            amd64) java_platform=linux-x64 ;;
            arm64) java_platform=linux-aarch64 ;;
        esac
        export JAVA_CLIENT_URL="https://corretto.aws/downloads/resources/${JAVA_VERSION}/amazon-corretto-${JAVA_VERSION}-${java_platform}.tar.gz"
        export JAVA_CLIENT_ARCH_NAME="amazon-corretto-${JAVA_VERSION}-${java_platform}.tar.gz"
        export JAVA_CLIENT_DIR_NAME="amazon-corretto-${JAVA_VERSION}-${java_platform}"
    fi

    # The deployment machine and the remote Kubernetes nodes may have different
    # architectures. Keep the host Java distribution independent of the target.
    export CONTAINER_ARCH="${CONTAINER_ARCH:-${DEPLOY_ARCH}}"
    case "${CONTAINER_ARCH}" in
        amd64) container_platform=linux-x64 ;;
        arm64) container_platform=linux-aarch64 ;;
        *) echo 'ERROR: CONTAINER_ARCH must be amd64 or arm64.' >&2; return 1 ;;
    esac
    export CONTAINER_JAVA_URL="https://corretto.aws/downloads/resources/${JAVA_VERSION}/amazon-corretto-${JAVA_VERSION}-${container_platform}.tar.gz"
    export CONTAINER_JAVA_ARCH_NAME="amazon-corretto-${JAVA_VERSION}-${container_platform}.tar.gz"
    export CONTAINER_JAVA_DIR_NAME="amazon-corretto-${JAVA_VERSION}-${container_platform}"
}

configure_host_tools() {
    local prefix
    if [ "${DEPLOY_OS}" = darwin ] && command -v brew >/dev/null 2>&1; then
        prefix="$(brew --prefix gettext 2>/dev/null || true)"
        [ -n "${prefix}" ] && prepend_path "${prefix}/bin"
        prefix="$(brew --prefix libpq 2>/dev/null || true)"
        [ -n "${prefix}" ] && prepend_path "${prefix}/bin"
    fi
}
