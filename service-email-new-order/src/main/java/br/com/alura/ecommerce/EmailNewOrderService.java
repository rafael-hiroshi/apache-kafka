package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EmailNewOrderService emailNewOrderService = new EmailNewOrderService();
        KafkaService service = new KafkaService(EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                emailNewOrderService::parse,
                new HashMap<>());
        service.run();
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        try {
            System.out.println("-----------------------------------------");
            System.out.println("Processing new order, preparing email");
            Message<Order> message = record.value();
            System.out.println("Value: " + message);

            Email content = new Email("br.com.alura.ecommerce.Order received", "Thank you for your order! We are processing your request");
            Order order = message.getPayload();
            CorrelationId id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, content);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
