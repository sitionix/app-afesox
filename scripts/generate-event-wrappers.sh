#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
service="${2:-}"
tag="${3:-}"
asyncapi="${4:-}"
out_dir="${5:-}"
base_package="${6:-}"
resources_dir="${7:-}"

if [ -z "$mode" ] || [ -z "$service" ] || [ -z "$tag" ] || [ -z "$asyncapi" ] || [ -z "$out_dir" ] || [ -z "$base_package" ]; then
  echo "Usage: $0 <producer|consumer> <service> <tag> <asyncapi.yml> <out_dir> <base_package> [resources_dir]"
  exit 1
fi

if ! command -v yq >/dev/null 2>&1; then
  echo "ERROR: yq is required to generate event wrappers."
  exit 1
fi

if [ -z "$resources_dir" ]; then
  resources_dir="${out_dir/generated-sources/generated-resources}"
fi

mkdir -p "$out_dir"
mkdir -p "$resources_dir"

tag_lower=$(echo "$tag" | tr '[:upper:]' '[:lower:]')

to_pascal() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+/ /g' | awk '{for (i=1;i<=NF;i++){printf toupper(substr($i,1,1)) tolower(substr($i,2))} printf "\n"}'
}

to_version_suffix() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+//g' | awk '{printf toupper($0)}'
}

tag_pascal=$(to_pascal "$tag")

lower_first() {
  local text="$1"
  if [ -z "$text" ]; then
    echo ""
    return
  fi
  printf '%s%s\n' "$(printf '%s' "${text:0:1}" | tr '[:upper:]' '[:lower:]')" "${text:1}"
}

publish_or_subscribe="publish"
if [ "$mode" = "consumer" ]; then
  publish_or_subscribe="subscribe"
fi

channels_json=$(yq -o=json '.channels' "$asyncapi")
escaped_tag=$(printf '%s' "$tag" | sed 's/"/\\"/g')
count_expr="to_entries | map(select(.value.${publish_or_subscribe}.tags[].name == \"${escaped_tag}\")) | length"
channel_count=$(echo "$channels_json" | yq -r "$count_expr")

if [ "$channel_count" -eq 0 ]; then
  echo "ERROR: No $publish_or_subscribe channels found with tag $tag in $asyncapi"
  exit 1
fi

cat > "${out_dir}/AvroRecordSerializer.java" <<EOF
package ${base_package};

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AvroRecordSerializer<T extends SpecificRecord> implements Serializer<T> {

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final DatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(data, encoder);
      encoder.flush();
      return outputStream.toByteArray();
    } catch (IOException ex) {
      throw new SerializationException("Failed to serialize Avro record", ex);
    }
  }
}
EOF

cat > "${out_dir}/AvroRecordDeserializer.java" <<EOF
package ${base_package};

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroRecordDeserializer<T extends SpecificRecord> implements Deserializer<T> {

  private final Schema schema;

  public AvroRecordDeserializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      final DatumReader<T> reader = new SpecificDatumReader<>(schema);
      final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      return reader.read(null, decoder);
    } catch (IOException ex) {
      throw new SerializationException("Failed to deserialize Avro record", ex);
    }
  }
}
EOF

cat > "${out_dir}/KafkaClientProperties.java" <<EOF
package ${base_package};

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

final class KafkaClientProperties {

  private static final String BOOTSTRAP_SERVERS_PROPERTY = "spring.kafka.bootstrap-servers";
  private static final String PRODUCER_PROPERTIES_PREFIX = "spring.kafka.producer.properties.";
  private static final String CONSUMER_PROPERTIES_PREFIX = "spring.kafka.consumer.properties.";

  private KafkaClientProperties() {
  }

  static Properties buildProducerProperties(Environment environment) {
    final Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, required(environment, BOOTSTRAP_SERVERS_PROPERTY));
    putIfPresent(environment, "spring.kafka.producer.client-id", ProducerConfig.CLIENT_ID_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.acks", ProducerConfig.ACKS_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.retries", ProducerConfig.RETRIES_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.linger-ms", ProducerConfig.LINGER_MS_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.batch-size", ProducerConfig.BATCH_SIZE_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.buffer-memory", ProducerConfig.BUFFER_MEMORY_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.compression-type", ProducerConfig.COMPRESSION_TYPE_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.delivery-timeout", ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.request-timeout", ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.producer.max-in-flight-requests-per-connection",
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, properties);
    putPrefixProperties(environment, PRODUCER_PROPERTIES_PREFIX, properties);
    return properties;
  }

  static Properties buildConsumerProperties(Environment environment) {
    final Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, required(environment, BOOTSTRAP_SERVERS_PROPERTY));
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, required(environment, "spring.kafka.consumer.group-id"));
    putIfPresent(environment, "spring.kafka.consumer.client-id", ConsumerConfig.CLIENT_ID_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.consumer.auto-offset-reset", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.enable-auto-commit", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.auto-commit-interval",
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties);
    putIfPresent(environment, "spring.kafka.consumer.max-poll-records", ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.max-poll-interval", ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.fetch-min-size", ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.fetch-max-wait", ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.session-timeout", ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
        properties);
    putIfPresent(environment, "spring.kafka.consumer.heartbeat-interval",
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, properties);
    putPrefixProperties(environment, CONSUMER_PROPERTIES_PREFIX, properties);
    return properties;
  }

  private static void putIfPresent(Environment environment, String propertyKey, String configKey,
      Properties properties) {
    final String value = environment.getProperty(propertyKey);
    if (value != null && !value.isBlank()) {
      properties.put(configKey, value);
    }
  }

  private static String required(Environment environment, String propertyKey) {
    final String value = environment.getProperty(propertyKey);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required Kafka property '" + propertyKey + "'");
    }
    return value;
  }

  private static void putPrefixProperties(Environment environment, String prefix, Properties properties) {
    if (!(environment instanceof ConfigurableEnvironment configurableEnvironment)) {
      return;
    }
    for (PropertySource<?> source : configurableEnvironment.getPropertySources()) {
      if (source instanceof EnumerablePropertySource<?> enumerableSource) {
        for (String name : enumerableSource.getPropertyNames()) {
          if (name.startsWith(prefix)) {
            final String key = name.substring(prefix.length());
            if (!properties.containsKey(key)) {
              final String value = environment.getProperty(name);
              if (value != null) {
                properties.put(key, value);
              }
            }
          }
        }
      }
    }
  }
}
EOF

mapfile -t channels < <(echo "$channels_json" | yq -r "to_entries | map(select(.value.${publish_or_subscribe}.tags[].name == \"${escaped_tag}\")) | .[] | [ .key, .value.${publish_or_subscribe}[\"x-itx-metadata-version\"], .value.${publish_or_subscribe}[\"x-itx-envelop-namespace\"], .value.${publish_or_subscribe}[\"x-itx-envelop-name\"] ] | @tsv")

config_classes=()
for channel_entry in "${channels[@]}"; do
  IFS=$'\t' read -r channel meta_version envelope_namespace envelope_name <<< "$channel_entry"
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
  bean_name=$(lower_first "$class_name")

  placeholder_array=$(CHANNEL_TEMPLATE="$channel" python3 - <<'PY'
import os, re

channel = os.environ["CHANNEL_TEMPLATE"]
names = sorted(dict.fromkeys(re.findall(r'\$\{([^}]+)\}', channel)))
if names:
    print(", ".join(f"\"{name}\"" for name in names))
else:
    print("")
PY
)
  placeholder_array=$(printf '%s' "$placeholder_array" | tr -d '\n')
  if [ -z "$placeholder_array" ]; then
    placeholder_init="{}"
  else
    placeholder_init="{${placeholder_array}}"
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
import org.springframework.core.env.Environment;
import ${envelope_namespace}.${envelope_name};

public class ${class_name} implements AutoCloseable {
EOF
  printf '  private static final String DEFAULT_TOPIC = "%s";\n' "$(printf '%s' "$channel" | sed 's/\\/\\\\/g; s/"/\\"/g')" >> "${out_dir}/${class_name}.java"
  cat >> "${out_dir}/${class_name}.java" <<EOF
  private static final String[] PLACEHOLDER_KEYS = ${placeholder_init};
  private final String topic;

  private static String resolveTopic(Environment environment) {
    if (PLACEHOLDER_KEYS.length == 0 || environment == null) {
      return DEFAULT_TOPIC;
    }
    String topic = DEFAULT_TOPIC;
    for (String key : PLACEHOLDER_KEYS) {
      String value = environment.getProperty(key);
      if (value == null) {
        throw new IllegalStateException(
            "Missing property '" + key + "' required to resolve topic " + DEFAULT_TOPIC);
      }
      topic = topic.replace("\${" + key + "}", value);
    }
    return topic;
  }
EOF

  if [ "$mode" = "producer" ]; then
    cat >> "${out_dir}/${class_name}.java" <<EOF
  private final KafkaProducer<String, ${envelope_name}> producer;

  public ${class_name}(Properties properties, Serializer<${envelope_name}> valueSerializer) {
    this(properties, valueSerializer, DEFAULT_TOPIC);
  }

  public ${class_name}(Properties properties, Serializer<${envelope_name}> valueSerializer, Environment environment) {
    this(properties, valueSerializer, resolveTopic(environment));
  }

  public ${class_name}(Properties properties, Serializer<${envelope_name}> valueSerializer, String topic) {
    this.topic = topic;
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
    this(properties, valueDeserializer, DEFAULT_TOPIC);
  }

  public ${class_name}(Properties properties, Deserializer<${envelope_name}> valueDeserializer, Environment environment) {
    this(properties, valueDeserializer, resolveTopic(environment));
  }

  public ${class_name}(Properties properties, Deserializer<${envelope_name}> valueDeserializer, String topic) {
    this.topic = topic;
    this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), valueDeserializer);
    this.consumer.subscribe(Collections.singletonList(this.topic));
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

  config_class="${class_name}AutoConfiguration"
  config_classes+=("${base_package}.${config_class}")
  if [ "$mode" = "producer" ]; then
    cat > "${out_dir}/${config_class}.java" <<EOF
package ${base_package};

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

@AutoConfiguration
public class ${config_class} {

  @Bean(name = "${bean_name}")
  @ConditionalOnBean(name = "kafkaContainerManager")
  @DependsOn("kafkaContainerManager")
  public ${class_name} forgeIt${class_name}(Environment environment) {
    return new ${class_name}(
        KafkaClientProperties.buildProducerProperties(environment),
        new AvroRecordSerializer<>(),
        environment);
  }

  @Bean(name = "${bean_name}")
  @ConditionalOnMissingBean(name = "kafkaContainerManager")
  public ${class_name} default${class_name}(Environment environment) {
    return new ${class_name}(
        KafkaClientProperties.buildProducerProperties(environment),
        new AvroRecordSerializer<>(),
        environment);
  }
}
EOF
  else
    cat > "${out_dir}/${config_class}.java" <<EOF
package ${base_package};

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import ${envelope_namespace}.${envelope_name};

@AutoConfiguration
public class ${config_class} {

  @Bean(name = "${bean_name}")
  @ConditionalOnBean(name = "kafkaContainerManager")
  @DependsOn("kafkaContainerManager")
  public ${class_name} forgeIt${class_name}(Environment environment) {
    return new ${class_name}(
        KafkaClientProperties.buildConsumerProperties(environment),
        new AvroRecordDeserializer<>(${envelope_name}.getClassSchema()),
        environment);
  }

  @Bean(name = "${bean_name}")
  @ConditionalOnMissingBean(name = "kafkaContainerManager")
  public ${class_name} default${class_name}(Environment environment) {
    return new ${class_name}(
        KafkaClientProperties.buildConsumerProperties(environment),
        new AvroRecordDeserializer<>(${envelope_name}.getClassSchema()),
        environment);
  }
}
EOF
  fi
done

auto_config_path="${resources_dir}/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"
mkdir -p "$(dirname "$auto_config_path")"
printf '%s\n' "${config_classes[@]}" > "$auto_config_path"
