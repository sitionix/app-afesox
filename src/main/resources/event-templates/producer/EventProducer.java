package ${event.wrapper.package};

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ${event.package}.${event.class};

public class EventProducer implements AutoCloseable {
    private final KafkaProducer<String, ${event.class}> producer;
    private final String topic;

    public EventProducer(Properties properties, Serializer<${event.class}> valueSerializer, String topic) {
        this.producer = new KafkaProducer<>(properties, new StringSerializer(), valueSerializer);
        this.topic = topic;
    }

    public void send(String key, ${event.class} value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    @Override
    public void close() {
        producer.close();
    }
}
