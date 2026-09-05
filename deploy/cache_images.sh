#!/bin/bash
set -eo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

action=${1:-save}
cache_arch_dir="${IMAGE_CACHE_DIR}/${DEPLOY_ARCH}"
manifest="${cache_arch_dir}/images.tsv"

mkdir -p "${cache_arch_dir}"

load_images() {
  if [ ! -s "${manifest}" ]; then
    echo "No cached ${DEPLOY_ARCH} workload images found."
    return 0
  fi

  echo "Loading cached ${DEPLOY_ARCH} workload images into Minikube..."
  while IFS="$(printf '\t')" read -r image archive_name; do
    [ -n "${image}" ] || continue
    archive="${cache_arch_dir}/${archive_name}"
    if [ ! -f "${archive}" ]; then
      echo "WARNING: cached archive is missing for ${image}: ${archive}" >&2
      continue
    fi
    echo "Loading ${image}"
    minikube -p minikube image load "${archive}" --overwrite=true
  done < "${manifest}"
}

save_images() {
  require_commands kubectl minikube

  images_file="$(mktemp "${TMPDIR:-/tmp}/sqlrec-images.XXXXXX")"
  manifest_tmp="$(mktemp "${cache_arch_dir}/images.tsv.XXXXXX")"
  trap 'rm -f "${images_file}" "${manifest_tmp}"' EXIT INT TERM

  # Cache workload images that are actually present after a successful deploy.
  # Minikube already manages the Kubernetes system/preload images separately.
  kubectl get pods --all-namespaces \
    -o go-template='{{range .items}}{{if ne .metadata.namespace "kube-system"}}{{range .spec.initContainers}}{{printf "%s\n" .image}}{{end}}{{range .spec.containers}}{{printf "%s\n" .image}}{{end}}{{end}}{{end}}' |
    sed '/^[[:space:]]*$/d' |
    sort -u > "${images_file}"

  if [ ! -s "${images_file}" ]; then
    echo "No workload images found to cache."
    rm -f "${manifest_tmp}"
    trap - EXIT INT TERM
    return 0
  fi

  echo "Saving workload images to ${cache_arch_dir}..."
  while IFS= read -r image; do
    checksum="$(printf '%s' "${image}" | cksum | awk '{print $1}')"
    safe_name="$(printf '%s' "${image}" | sed 's/[^[:alnum:]._-]/_/g' | cut -c1-180)"
    archive_name="${safe_name}-${checksum}.tar"
    archive="${cache_arch_dir}/${archive_name}"
    archive_tmp="${archive}.tmp"

    if [ -s "${archive}" ]; then
      echo "Using existing cache for ${image}"
      printf '%s\t%s\n' "${image}" "${archive_name}" >> "${manifest_tmp}"
      continue
    fi

    echo "Saving ${image}"
    rm -f "${archive_tmp}"
    # `minikube image save` can exit successfully without creating an archive
    # when a Pod declares an image that has not actually been pulled yet.
    if minikube -p minikube image save "${image}" "${archive_tmp}" && [ -s "${archive_tmp}" ]; then
      mv "${archive_tmp}" "${archive}"
      printf '%s\t%s\n' "${image}" "${archive_name}" >> "${manifest_tmp}"
    else
      rm -f "${archive_tmp}"
      echo "WARNING: skipping unavailable image ${image}; no archive was created." >&2
    fi
  done < "${images_file}"

  mv "${manifest_tmp}" "${manifest}"
  rm -f "${images_file}"
  trap - EXIT INT TERM
  echo "Workload image cache updated."
}

case "${action}" in
  load) load_images ;;
  save) save_images ;;
  *)
    echo "Usage: $0 [load|save]" >&2
    exit 2
    ;;
esac
