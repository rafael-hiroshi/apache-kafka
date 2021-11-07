package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.*;

public class FraudDetectorService {
    public static void main(String[] args) {
        FraudDetectorService fraudDetectorService = new FraudDetectorService();
        KafkaService service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class);
        service.run();
    }

    private void parse(ConsumerRecord<String, Order> record) {
        try {
            System.out.println("-----------------------------------------");
            System.out.printf("Consumer Record: (%s, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
            System.out.println("New order - Checking for fraud");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
