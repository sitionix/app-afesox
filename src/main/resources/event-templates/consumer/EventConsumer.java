package ${event.wrapper.package};

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ${event.package}.${event.class};

public class EventConsumer implements AutoCloseable {
    private final KafkaConsumer<String, ${event.class}> consumer;

    public EventConsumer(Properties properties, Deserializer<${event.class}> valueDeserializer, String topic) {
        this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), valueDeserializer);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void pollOnce(Duration timeout, Consumer<ConsumerRecord<String, ${event.class}>> handler) {
        ConsumerRecords<String, ${event.class}> records = consumer.poll(timeout);
        for (ConsumerRecord<String, ${event.class}> record : records) {
            handler.accept(record);
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
