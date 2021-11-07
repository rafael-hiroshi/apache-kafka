package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse) {
        this(groupId, parse);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse) {
        this(groupId, parse);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
    }

    public void run() {
        while(true) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));

            if (records.isEmpty()) {
                continue;
            }

            System.out.println("Found " + records.count() + " records");
            for (var record : records) {
                parse.consume((ConsumerRecord<String, String>) record);
            }
        }
    }

    private static Properties properties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
