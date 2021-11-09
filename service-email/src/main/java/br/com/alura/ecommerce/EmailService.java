package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<Email> {
    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Email>> record) {
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
