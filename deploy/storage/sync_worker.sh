#!/bin/sh
# Runs inside the temporary sync Pod. The manifest is TSV:
# type (directory/file/executable/link), SHA256, relative path. No cached completion
# marker is trusted: checks read the actual PVC contents on every deployment.
set -eu

action=$1
root=$2
manifest=$3
tab=$(printf '\t')

matches() {
  kind=$1
  expected=$2
  path=$3
  case "$kind" in
    D) [ -d "$path" ] && [ ! -L "$path" ]; return $? ;;
    L)
      [ -L "$path" ] || return 1
      actual=$(printf '%s' "$(readlink "$path")" | sha256sum)
      ;;
    F|X)
      [ -f "$path" ] && [ ! -L "$path" ] || return 1
      if [ "$kind" = X ]; then
        [ -x "$path" ] || return 1
      else
        [ ! -x "$path" ] || return 1
      fi
      actual=$(sha256sum < "$path")
      ;;
    *) echo "ERROR: invalid manifest type '$kind'." >&2; return 1 ;;
  esac
  [ "${actual%% *}" = "$expected" ]
}

validate_path() {
  case "$1" in
    ./*) ;;
    *) echo "ERROR: invalid resource path '$1'." >&2; exit 1 ;;
  esac
  case "/$1/" in
    */../*) echo "ERROR: invalid resource path '$1'." >&2; exit 1 ;;
  esac
}

case "$action" in
  check|verify)
    if [ "$action" = check ]; then
      changed=$4
      : > "$changed"
    fi
    while IFS="$tab" read -r kind expected relative; do
      validate_path "$relative"
      if ! matches "$kind" "$expected" "$root/$relative"; then
        if [ "$action" = verify ]; then
          echo "ERROR: resource verification failed: $relative" >&2
          exit 1
        fi
        printf '%s\t%s\t%s\n' "$kind" "$expected" "$relative" >> "$changed"
        printf '%s\n' "$relative"
      fi
    done < "$manifest"
    ;;
  install)
    # Extract on the same filesystem. Failed/truncated streams never replace
    # live files, and each verified replacement is an atomic rename.
    staging=$(mktemp -d "$root/.sqlrec-sync.XXXXXX")
    trap 'rm -rf "$staging"' EXIT
    trap 'exit 1' HUP INT TERM
    # Keep the Pod's ownership, not the deployment machine's UID/GID. This
    # also avoids chown failures on shared storage with NFS root squashing.
    tar --no-same-owner -xpf - -C "$staging"
    while IFS="$tab" read -r kind expected relative; do
      validate_path "$relative"
      # This manifest contains only the requested changes. Every entry must
      # have arrived, even when tar accepts a stream containing only a prefix.
      matches "$kind" "$expected" "$staging/$relative" || {
        echo "ERROR: uploaded resource missing or invalid: $relative" >&2
        exit 1
      }
      if [ "$kind" = D ] && { [ -L "$root/$relative" ] || { [ -e "$root/$relative" ] && [ ! -d "$root/$relative" ]; }; }; then
        echo "ERROR: expected directory but found another file type: $relative" >&2
        exit 1
      fi
    done < "$manifest"
    while IFS="$tab" read -r kind expected relative; do
      if [ "$kind" = D ]; then
        mkdir -p "$root/$relative"
      else
        destination=$(dirname "$root/$relative")
        mkdir -p "$destination"
        # Target the parent directory so an existing symlink to a directory
        # is replaced rather than followed by mv.
        mv -f "$staging/$relative" "$destination/"
      fi
    done < "$manifest"
    ;;
  *) echo "ERROR: unsupported sync action '$action'." >&2; exit 1 ;;
esac
