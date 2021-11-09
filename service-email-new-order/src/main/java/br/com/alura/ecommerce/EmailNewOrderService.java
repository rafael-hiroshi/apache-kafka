package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException {
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
