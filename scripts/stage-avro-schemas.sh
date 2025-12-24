#!/usr/bin/env bash
set -euo pipefail

service="${1:-}"
event_name="${2:-}"
output_dir="${3:-}"
root_dir="${4:-}"

if [ -z "$service" ] || [ -z "$event_name" ] || [ -z "$output_dir" ] || [ -z "$root_dir" ]; then
  echo "Usage: $0 <service> <event_name> <output_dir> <root_dir>"
  exit 1
fi

mkdir -p "$output_dir"

copy_with_prefix() {
  local src="$1"
  local prefix="$2"
  local base
  base=$(basename "$src")
  cp "$src" "${output_dir}/${prefix}-${base}"
}

# Always include common metadata first.
copy_with_prefix "${root_dir}/apis/common/event/Metadata.avsc" "00"

event_dir="${root_dir}/apis/${service}/event/${event_name}"
if [ ! -d "$event_dir" ]; then
  echo "ERROR: event directory not found at ${event_dir}"
  exit 1
fi

while IFS= read -r file; do
  case "$file" in
    */imports/*.avsc)
      copy_with_prefix "$file" "10"
      ;;
    */envelope.avsc)
      copy_with_prefix "$file" "90"
      ;;
    *.avsc)
      copy_with_prefix "$file" "50"
      ;;
  esac
done < <(find "$event_dir" -type f -name "*.avsc" | sort)
