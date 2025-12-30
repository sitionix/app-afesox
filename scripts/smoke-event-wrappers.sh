#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
base_package="com.app_afesox.athssox.events"
event_one="emailverify"
event_two="smoke2"
shared_package="${base_package}.kafka"

if ! command -v yq >/dev/null 2>&1; then
  echo "ERROR: yq is required for the smoke test."
  exit 1
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

asyncapi_file="${tmp_dir}/asyncapi.yml"
cat > "$asyncapi_file" <<'YAML'
asyncapi: 2.0.0
info:
  title: Smoke Events
  version: 0.0.1
channels:
  athssox.${environment}.email-verify.public.v1:
    publish:
      tags:
        - name: EmailVerify
      x-itx-metadata-version: v1
      x-itx-envelop-namespace: com.app_afesox.athssox.events.emailverify
      x-itx-envelop-name: EmailVerifyEventEnvelope
    subscribe:
      tags:
        - name: EmailVerify
      x-itx-metadata-version: v1
      x-itx-envelop-namespace: com.app_afesox.athssox.events.emailverify
      x-itx-envelop-name: EmailVerifyEventEnvelope
  athssox.${environment}.smoke2.public.v1:
    publish:
      tags:
        - name: Smoke2
      x-itx-metadata-version: v1
      x-itx-envelop-namespace: com.app_afesox.athssox.events.smoke2
      x-itx-envelop-name: Smoke2EventEnvelope
    subscribe:
      tags:
        - name: Smoke2
      x-itx-metadata-version: v1
      x-itx-envelop-namespace: com.app_afesox.athssox.events.smoke2
      x-itx-envelop-name: Smoke2EventEnvelope
YAML

out_one="${tmp_dir}/out-emailverify"
out_two="${tmp_dir}/out-smoke2"
resources_one="${tmp_dir}/resources-emailverify"
resources_two="${tmp_dir}/resources-smoke2"

mkdir -p "$out_one" "$out_two" "$resources_one" "$resources_two"

bash "${repo_root}/scripts/generate-event-wrappers.sh" producer athssox EmailVerify "$asyncapi_file" "$out_one" \
  "$base_package" "$event_one" "${base_package}.${event_one}.kafka" "$shared_package" "$resources_one"

bash "${repo_root}/scripts/generate-event-wrappers.sh" producer athssox Smoke2 "$asyncapi_file" "$out_two" \
  "$base_package" "$event_two" "${base_package}.${event_two}.kafka" "$shared_package" "$resources_two"

if command -v rg >/dev/null 2>&1; then
  search=(rg -n)
else
  search=(grep -R -n)
fi

if "${search[@]}" "dummy" "$out_one" "$out_two" >/dev/null 2>&1; then
  echo "ERROR: dummy packages detected in generated output."
  exit 1
fi

if ! "${search[@]}" "package ${shared_package};" "$out_one" >/dev/null 2>&1; then
  echo "ERROR: shared package not found for ${event_one}."
  exit 1
fi

if ! "${search[@]}" "package ${shared_package};" "$out_two" >/dev/null 2>&1; then
  echo "ERROR: shared package not found for ${event_two}."
  exit 1
fi

if ! "${search[@]}" "package ${base_package}.${event_one}.kafka;" "$out_one" >/dev/null 2>&1; then
  echo "ERROR: event package not found for ${event_one}."
  exit 1
fi

if ! "${search[@]}" "package ${base_package}.${event_two}.kafka;" "$out_two" >/dev/null 2>&1; then
  echo "ERROR: event package not found for ${event_two}."
  exit 1
fi

if ! "${search[@]}" "com.app_afesox.athssox.events.emailverify.kafka.EmailverifyV1ProducerAutoConfiguration" \
  "$resources_one/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports" >/dev/null 2>&1; then
  echo "ERROR: AutoConfiguration import missing for ${event_one}."
  exit 1
fi

if ! "${search[@]}" "com.app_afesox.athssox.events.smoke2.kafka.Smoke2V1ProducerAutoConfiguration" \
  "$resources_two/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports" >/dev/null 2>&1; then
  echo "ERROR: AutoConfiguration import missing for ${event_two}."
  exit 1
fi

echo "Smoke test passed."
