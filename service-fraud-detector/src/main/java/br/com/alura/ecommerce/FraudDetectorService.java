package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dao.OrderDAO;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        new ServiceRunner(FraudDetectorService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, SQLException {
        try(Connection connection = new ConnectionFactory().getConnection()) {
            System.out.println("---------------Checking for fraud---------------");
            System.out.println("Key: " + record.key() + " Value: " + record.value() + " Partition: " + record.partition()
                    + " Offset: " + record.offset());

            Message<Order> message = record.value();
            Order order = record.value().getPayload();
            OrderDAO orderDao = new OrderDAO(connection);
            String uuid = order.getOrderId();

            if(wasProcessed(orderDao, uuid)) {
                System.out.println("Order + " + uuid + " was already processed");
                return;
            }

            Thread.sleep(5000);

            boolean isFraud = isFraud(order);
            dispatchFraudStatus(message, order, isFraud);
            orderDao.insert(order, isFraud);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean wasProcessed(OrderDAO orderDao, String uuid) throws SQLException {
        return orderDao.findById(uuid).next();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

    private void dispatchFraudStatus(Message<Order> message, Order order, boolean isFraud) throws ExecutionException, InterruptedException {
        if (isFraud) {
            System.out.println("Order is a fraud!!!! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }
    }
}
