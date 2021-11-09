package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LogService logService = new LogService();
        KafkaService service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
        service.run();
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("-------------------LOG-------------------");
        System.out.println("topic: " + record.topic());
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        System.out.println("--------------------------------------");
    }
}
