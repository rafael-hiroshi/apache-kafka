package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;

public class NewOrderMain {
    public static void main(String[] args) {
        try(KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>()) {
            for (int i = 0; i < 10; i++) {
                String orderId = UUID.randomUUID().toString();
                BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                String userEmail = Math.random() + "@email.com";

                Order order = new Order(orderId, amount, userEmail);
                CorrelationId id = new CorrelationId(NewOrderMain.class.getSimpleName());

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, id, order);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
