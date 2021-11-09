import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
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

            Email content = new Email("Order received", "Thank you for your order! We are processing your request");
            Order order = message.getPayload();
            CorrelationId id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, content);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Message<Order> message = record.value();
        Order order = record.value().getPayload();
        if (isFraud(order)) {
            System.out.println("Order is a fraud!!!! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
                    message.getId().continueWith(EmailNewOrderService.class.getSimpleName()), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
                    message.getId().continueWith(EmailNewOrderService.class.getSimpleName()), order);
        }
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
