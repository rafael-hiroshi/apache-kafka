package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        LogService logService = new LogService();
        KafkaService service = new KafkaService(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), logService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        ConsumerRecord<String, String> rc = record;
        System.out.printf("Log: (%s, %s, %d, %d)\n", rc.key(), rc.value(), rc.partition(), rc.offset());
    }
}
