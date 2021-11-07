package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class EmailService {
    public static void main(String[] args) {
        EmailService emailService = new EmailService();
        KafkaService service = new KafkaService<>(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                new HashMap<>());
        service.run();
    }

    private void parse(ConsumerRecord<String, Email> record) {
        try {
            System.out.println("-----------------------------------------");
            System.out.printf("Consumer Record: (%s, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
            System.out.println("Email sent");
            Thread.sleep(2500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
