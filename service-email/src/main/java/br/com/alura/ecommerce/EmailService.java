package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class EmailService {
    public static void main(String[] args) {
        EmailService emailService = new EmailService();
        KafkaService service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                new HashMap<>());
        service.run();
    }

    private void parse(ConsumerRecord<String, Message<Email>> record) {
        try {
            System.out.println("-----------------------------------------");
            System.out.println("Send email");
            System.out.println("Key: " + record.key() + " Value: " + record.value() + " Partition: " + record.partition()
                    + " Offset: " + record.offset());
            Thread.sleep(2500);
            System.out.println("Email sent");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
