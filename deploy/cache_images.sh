#!/bin/bash
set -eo pipefail

dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
source "${dir}/env.sh"

action=${1:-save}
cache_arch_dir="${IMAGE_CACHE_DIR}/${DEPLOY_ARCH}"
manifest="${cache_arch_dir}/images.tsv"

mkdir -p "${cache_arch_dir}"

cache_created_time() {
  local created
  # GNU stat uses %W for birth time; BSD/macOS stat uses %B.
  created="$(stat -c %W "$1" 2>/dev/null || stat -f %B "$1" 2>/dev/null)" || created=""
  if ! [[ "${created}" =~ ^[0-9]+$ ]] || [ "${created}" -le 0 ]; then
    # Some Linux filesystems do not expose birth time. Use the archive's last
    # write time rather than ctime, which also changes when permissions change.
    created="$(stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null)" || return 1
  fi
  [[ "${created}" =~ ^[0-9]+$ ]] && [ "${created}" -gt 0 ] || return 1
  printf '%s\n' "${created}"
}

image_created_time() {
  local quoted_image
  printf -v quoted_image '%q' "$1"
  # Inspect the Docker daemon inside Minikube, where image save reads from.
  # Convert RFC3339 timestamps on the Linux node to avoid host date differences.
  # SSH must not consume the image list supplied to the surrounding read loop.
  minikube -p minikube ssh -- \
    "created=\$(sudo docker image inspect --format '{{.Created}}' ${quoted_image}) && date -d \"\${created}\" +%s" < /dev/null |
    tr -d '\r'
}

load_images() {
  local quoted_image loaded_count=0
  if [ ! -s "${manifest}" ]; then
    echo "No cached ${DEPLOY_ARCH} images found."
    return 0
  fi

  echo "Loading cached ${DEPLOY_ARCH} images into Minikube..."
  while IFS="$(printf '\t')" read -r image archive_name; do
    [ -n "${image}" ] || continue
    archive="${cache_arch_dir}/${archive_name}"
    if [ ! -s "${archive}" ]; then
      echo "WARNING: cached archive is missing or empty for ${image}: ${archive}" >&2
      continue
    fi
    echo "Loading ${image}"
    minikube -p minikube image load "${archive}" --overwrite=true < /dev/null
    # A successful load must make the exact image reference available.
    # This also catches archives containing an outdated or different image tag.
    printf -v quoted_image '%q' "${image}"
    if ! minikube -p minikube ssh -- \
      "sudo docker image inspect -- ${quoted_image} >/dev/null" < /dev/null; then
      echo "ERROR: cached archive did not provide ${image}: ${archive}" >&2
      return 1
    fi
    loaded_count=$((loaded_count + 1))
  done < "${manifest}"
  echo "Loaded ${loaded_count} cached images."
}

save_images() {
  require_commands minikube stat

  images_file="$(mktemp "${TMPDIR:-/tmp}/sqlrec-images.XXXXXX")"
  manifest_tmp="$(mktemp "${cache_arch_dir}/images.tsv.XXXXXX")"
  trap 'rm -f "${images_file}" "${manifest_tmp}"' EXIT INT TERM

  # Cache every local image in the same Minikube Docker daemon used by save,
  # including system images, unused tags, and untagged images (by full ID).
  minikube -p minikube ssh -- \
    'sudo docker image ls --all --no-trunc --format '\''{{if or (eq .Repository "<none>") (eq .Tag "<none>")}}{{.ID}}{{else}}{{.Repository}}:{{.Tag}}{{end}}'\''' < /dev/null |
    tr -d '\r' |
    sed '/^[[:space:]]*$/d' |
    sort -u > "${images_file}"

  if [ ! -s "${images_file}" ]; then
    echo "No local Minikube images found to cache."
    rm -f "${images_file}" "${manifest_tmp}"
    trap - EXIT INT TERM
    return 0
  fi

  echo "Saving all local Minikube images to ${cache_arch_dir}..."
  while IFS= read -r image; do
    checksum="$(printf '%s' "${image}" | cksum | awk '{print $1}')"
    safe_name="$(printf '%s' "${image}" | sed 's/[^[:alnum:]._-]/_/g' | cut -c1-180)"
    archive_name="${safe_name}-${checksum}.tar"
    archive="${cache_arch_dir}/${archive_name}"
    archive_tmp="${archive}.tmp"

    if [ -s "${archive}" ]; then
      if cache_created="$(cache_created_time "${archive}")" &&
        image_created="$(image_created_time "${image}")" &&
        [[ "${image_created}" =~ ^[0-9]+$ ]]; then
        if [ "${image_created}" -le "${cache_created}" ]; then
          echo "Using existing cache for ${image}"
          printf '%s\t%s\n' "${image}" "${archive_name}" >> "${manifest_tmp}"
          continue
        fi
        echo "Image ${image} is newer than its cache; refreshing cache."
      else
        echo "WARNING: unable to compare creation times for ${image}; refreshing cache." >&2
      fi
      rm -f "${archive}"
    fi

    echo "Saving ${image}"
    rm -f "${archive_tmp}"
    # `minikube image save` can exit successfully without creating an archive
    # if an image becomes unavailable between listing and saving.
    if minikube -p minikube image save "${image}" "${archive_tmp}" < /dev/null && [ -s "${archive_tmp}" ]; then
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
  echo "Image cache updated."
}

case "${action}" in
  load) load_images ;;
  save) save_images ;;
  *)
    echo "Usage: $0 [load|save]" >&2
    exit 2
    ;;
esac
