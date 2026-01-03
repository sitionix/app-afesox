#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
service="${2:-}"
tag="${3:-}"
asyncapi="${4:-}"
out_dir="${5:-}"
base_package="${6:-}"
event_name="${7:-}"
event_package="${8:-}"
shared_package="${9:-}"
resources_dir="${10:-}"

if [ -z "$mode" ] || [ -z "$service" ] || [ -z "$tag" ] || [ -z "$asyncapi" ] || [ -z "$out_dir" ] || \
   [ -z "$base_package" ] || [ -z "$event_name" ] || [ -z "$event_package" ] || [ -z "$shared_package" ]; then
  echo "Usage: $0 <producer|consumer> <service> <tag> <asyncapi.yml> <out_dir> <base_wrapper_package> <event_name> <event_wrapper_package> <shared_kafka_package> [resources_dir]"
  exit 1
fi

if ! command -v yq >/dev/null 2>&1; then
  echo "ERROR: yq is required to generate event wrappers."
  exit 1
fi

if [ -z "$resources_dir" ]; then
  resources_dir="${out_dir/generated-sources/generated-resources}"
fi

if [[ "$base_package" == *".dummy."* ]] || [[ "$base_package" == *.dummy ]] || [[ "$base_package" == *.dummy.kafka ]]; then
  echo "ERROR: BASE_WRAPPER_PACKAGE must not contain dummy."
  exit 1
fi

if [[ "$base_package" == *.kafka ]]; then
  echo "ERROR: BASE_WRAPPER_PACKAGE must not end with .kafka."
  exit 1
fi

expected_event_package="${base_package}.${event_name}.kafka"
expected_shared_package="${base_package}.kafka"

if [[ "$event_package" != "$expected_event_package" ]]; then
  echo "ERROR: EVENT_WRAPPER_PACKAGE must be ${expected_event_package}."
  exit 1
fi

if [[ "$shared_package" != "$expected_shared_package" ]]; then
  echo "ERROR: SHARED_KAFKA_PACKAGE must be ${expected_shared_package}."
  exit 1
fi

if [[ "$event_package" == *".dummy."* ]] || [[ "$event_package" == *.dummy ]] || [[ "$event_package" == *.dummy.kafka ]]; then
  echo "ERROR: EVENT_WRAPPER_PACKAGE must not contain dummy."
  exit 1
fi

mkdir -p "$out_dir"
mkdir -p "$resources_dir"

to_pascal() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+/ /g' | awk '{for (i=1;i<=NF;i++){printf toupper(substr($i,1,1)) tolower(substr($i,2))} printf "\n"}'
}

to_pascal_preserve() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+/ /g' | awk '{for (i=1;i<=NF;i++){printf toupper(substr($i,1,1)) substr($i,2)} printf "\n"}'
}

to_version_suffix() {
  echo "$1" | sed -E 's/[^a-zA-Z0-9]+//g' | awk '{printf toupper($0)}'
}

tag_pascal=$(to_pascal "$tag")
tag_method=$(to_pascal_preserve "$tag")

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
package ${shared_package};

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
package ${shared_package};

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

cat > "${out_dir}/KafkaSerdeDefaults.java" <<EOF
package ${shared_package};

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;

import java.util.LinkedHashMap;
import java.util.Map;

public final class KafkaSerdeDefaults {

  private static final String PROPERTY_SOURCE_NAME = "afesoxKafkaSerdeDefaults";

  private KafkaSerdeDefaults() {
  }

  public static void apply(Environment environment) {
    if (!(environment instanceof ConfigurableEnvironment configurableEnvironment)) {
      return;
    }
    final Map<String, Object> properties = new LinkedHashMap<>();
    if (environment.getProperty("spring.kafka.consumer.value-deserializer") == null) {
      properties.put("spring.kafka.consumer.value-deserializer", ByteArrayDeserializer.class.getName());
    }
    if (properties.isEmpty()) {
      return;
    }
    final MapPropertySource propertySource = new MapPropertySource(PROPERTY_SOURCE_NAME, properties);
    if (configurableEnvironment.getPropertySources().contains(PROPERTY_SOURCE_NAME)) {
      configurableEnvironment.getPropertySources().replace(PROPERTY_SOURCE_NAME, propertySource);
    } else {
      configurableEnvironment.getPropertySources().addFirst(propertySource);
    }
  }
}
EOF

cat > "${out_dir}/KafkaClientProperties.java" <<EOF
package ${shared_package};

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

public final class KafkaClientProperties {

  private static final String BOOTSTRAP_SERVERS_PROPERTY = "spring.kafka.bootstrap-servers";
  private static final String PRODUCER_PROPERTIES_PREFIX = "spring.kafka.producer.properties.";
  private static final String CONSUMER_PROPERTIES_PREFIX = "spring.kafka.consumer.properties.";

  private KafkaClientProperties() {
  }

  public static Properties buildProducerProperties(Environment environment) {
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

  public static Properties buildConsumerProperties(Environment environment) {
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

channels=()
while IFS= read -r line; do
  channels+=("$line")
done < <(echo "$channels_json" | yq -r "to_entries | map(select(.value.${publish_or_subscribe}.tags[].name == \"${escaped_tag}\")) | .[] | [ .key, .value.${publish_or_subscribe}[\"x-itx-metadata-version\"], .value.${publish_or_subscribe}[\"x-itx-envelop-namespace\"], .value.${publish_or_subscribe}[\"x-itx-envelop-name\"] ] | @tsv")

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
package ${event_package};

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

  if [ "$mode" = "consumer" ]; then
    handler_name="${class_name}Handler"
    runner_name="${class_name}Runner"
    cat > "${out_dir}/${handler_name}.java" <<EOF
package ${event_package};

import ${envelope_namespace}.${envelope_name};

@FunctionalInterface
public interface ${handler_name} {
  void consume${tag_method}(${envelope_name} event);
}
EOF

    cat > "${out_dir}/${runner_name}.java" <<EOF
package ${event_package};

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.SmartLifecycle;
import ${envelope_namespace}.${envelope_name};

public class ${runner_name} implements SmartLifecycle {
  private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

  private final ${class_name} consumer;
  private final List<${handler_name}> handlers;
  private final Duration pollTimeout;
  private final Duration shutdownTimeout;
  private final ExecutorService executor;
  private final AtomicBoolean running = new AtomicBoolean(false);

  public ${runner_name}(${class_name} consumer, List<${handler_name}> handlers) {
    this(consumer, handlers, DEFAULT_POLL_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT, newSingleThreadExecutor());
  }

  public ${runner_name}(${class_name} consumer, List<${handler_name}> handlers, Duration pollTimeout,
      Duration shutdownTimeout, ExecutorService executor) {
    this.consumer = consumer;
    this.handlers = handlers;
    this.pollTimeout = pollTimeout == null ? DEFAULT_POLL_TIMEOUT : pollTimeout;
    this.shutdownTimeout = shutdownTimeout == null ? DEFAULT_SHUTDOWN_TIMEOUT : shutdownTimeout;
    this.executor = executor;
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    executor.submit(this::runLoop);
  }

  private void runLoop() {
    try {
      while (running.get()) {
        consumer.pollOnce(pollTimeout, this::dispatch);
      }
    } finally {
      consumer.close();
    }
  }

  private void dispatch(ConsumerRecord<String, ${envelope_name}> record) {
    for (${handler_name} handler : handlers) {
      handler.consume${tag_method}(record.value());
    }
  }

  @Override
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    shutdownExecutor();
  }

  @Override
  public void stop(Runnable callback) {
    stop();
    callback.run();
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  private void shutdownExecutor() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
  }

  private static ExecutorService newSingleThreadExecutor() {
    ThreadFactory threadFactory = runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName("${bean_name}-runner");
      thread.setDaemon(true);
      return thread;
    };
    return Executors.newSingleThreadExecutor(threadFactory);
  }
}
EOF
  fi

  config_class="${class_name}AutoConfiguration"
  config_classes+=("${event_package}.${config_class}")
  if [ "$mode" = "producer" ]; then
    cat > "${out_dir}/${config_class}.java" <<EOF
package ${event_package};

import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import ${shared_package}.AvroRecordSerializer;
import ${shared_package}.KafkaClientProperties;
import ${shared_package}.KafkaSerdeDefaults;

@AutoConfiguration
public class ${config_class} {

  @Bean(name = "${bean_name}")
  @ConditionalOnBean(name = "kafkaContainerManager")
  @DependsOn("kafkaContainerManager")
  public ${class_name} forgeIt${class_name}(Environment environment) {
    KafkaSerdeDefaults.apply(environment);
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
package ${event_package};

import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import ${shared_package}.AvroRecordDeserializer;
import ${shared_package}.KafkaClientProperties;
import ${shared_package}.KafkaSerdeDefaults;
import ${envelope_namespace}.${envelope_name};

@AutoConfiguration
public class ${config_class} {

  @Bean(name = "${bean_name}")
  @ConditionalOnBean(name = "kafkaContainerManager")
  @DependsOn("kafkaContainerManager")
  public ${class_name} forgeIt${class_name}(Environment environment) {
    KafkaSerdeDefaults.apply(environment);
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

  @Bean(name = "${bean_name}Runner")
  @ConditionalOnBean(${class_name}Handler.class)
  @ConditionalOnMissingBean(${class_name}Runner.class)
  public ${class_name}Runner ${bean_name}Runner(${class_name} consumer, List<${class_name}Handler> handlers) {
    return new ${class_name}Runner(consumer, handlers);
  }
}
EOF
  fi
done

auto_config_path="${resources_dir}/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"
mkdir -p "$(dirname "$auto_config_path")"
printf '%s\n' "${config_classes[@]}" > "$auto_config_path"
