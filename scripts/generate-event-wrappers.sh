#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
service="${2:-}"
tag="${3:-}"
asyncapi="${4:-}"
out_dir="${5:-}"
base_package="${6:-}"

if [ -z "$mode" ] || [ -z "$service" ] || [ -z "$tag" ] || [ -z "$asyncapi" ] || [ -z "$out_dir" ] || [ -z "$base_package" ]; then
  echo "Usage: $0 <producer|consumer> <service> <tag> <asyncapi.yml> <out_dir> <base_package>"
  exit 1
fi

if ! command -v yq >/dev/null 2>&1; then
  echo "ERROR: yq is required to generate event wrappers."
  exit 1
fi

mkdir -p "$out_dir"

tag_lower=$(echo "$tag" | tr '[:upper:]' '[:lower:]')

to_pascal() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+/ /g' | awk '{for (i=1;i<=NF;i++){printf toupper(substr($i,1,1)) tolower(substr($i,2))} printf "\n"}'
}

to_version_suffix() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+//g' | awk '{printf toupper($0)}'
}

tag_pascal=$(to_pascal "$tag")

publish_or_subscribe="publish"
if [ "$mode" = "consumer" ]; then
  publish_or_subscribe="subscribe"
fi

channels_json=$(yq -o=json '.channels' "$asyncapi")
channel_count=$(echo "$channels_json" | yq -r '
  to_entries
  | map(select(.value['"$publish_or_subscribe"'].tags[].name == "'$tag'"))
  | length
')

if [ "$channel_count" -eq 0 ]; then
  echo "ERROR: No $publish_or_subscribe channels found with tag $tag in $asyncapi"
  exit 1
fi

echo "$channels_json" | yq -r '
  to_entries
  | map(select(.value['"$publish_or_subscribe"'].tags[].name == "'$tag'"))
  | .[]
  | [
      .key,
      .value['"$publish_or_subscribe"']["x-itx-metadata-version"],
      .value['"$publish_or_subscribe"']["x-itx-envelop-namespace"],
      .value['"$publish_or_subscribe"']["x-itx-envelop-name"]
    ] | @tsv
' | while IFS=$'\t' read -r channel meta_version envelope_namespace envelope_name; do
  if [ -z "$meta_version" ] || [ "$meta_version" = "null" ]; then
    echo "ERROR: Missing x-itx-metadata-version for channel $channel"
    exit 1
  fi
  if [ -z "$envelope_namespace" ] || [ "$envelope_namespace" = "null" ] || \
     [ -z "$envelope_name" ] || [ "$envelope_name" = "null" ]; then
    echo "ERROR: Missing envelope metadata for channel $channel"
    exit 1
  fi

  version_suffix=$(to_version_suffix "$meta_version")
  class_name="${tag_pascal}${version_suffix}"
  if [ "$mode" = "producer" ]; then
    class_name="${class_name}Producer"
  else
    class_name="${class_name}Consumer"
  fi

  cat > "${out_dir}/${class_name}.java" <<EOF
package ${base_package};

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ${envelope_namespace}.${envelope_name};

public class ${class_name} implements AutoCloseable {
  private final String topic = "${channel}";
EOF

  if [ "$mode" = "producer" ]; then
    cat >> "${out_dir}/${class_name}.java" <<EOF
  private final KafkaProducer<String, ${envelope_name}> producer;

  public ${class_name}(Properties properties, Serializer<${envelope_name}> valueSerializer) {
    this.producer = new KafkaProducer<>(properties, new StringSerializer(), valueSerializer);
  }

  public void send(String key, ${envelope_name} value) {
    producer.send(new ProducerRecord<>(topic, key, value));
  }
EOF
  else
    cat >> "${out_dir}/${class_name}.java" <<EOF
  private final KafkaConsumer<String, ${envelope_name}> consumer;

  public ${class_name}(Properties properties, Deserializer<${envelope_name}> valueDeserializer) {
    this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), valueDeserializer);
    this.consumer.subscribe(Collections.singletonList(topic));
  }

  public void pollOnce(Duration timeout, Consumer<ConsumerRecord<String, ${envelope_name}>> handler) {
    ConsumerRecords<String, ${envelope_name}> records = consumer.poll(timeout);
    for (ConsumerRecord<String, ${envelope_name}> record : records) {
      handler.accept(record);
    }
  }
EOF
  fi

  if [ "$mode" = "producer" ]; then
    cat >> "${out_dir}/${class_name}.java" <<EOF

  @Override
  public void close() {
    producer.close();
  }
}
EOF
  else
    cat >> "${out_dir}/${class_name}.java" <<EOF

  @Override
  public void close() {
    consumer.close();
  }
}
EOF
  fi
done
