package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;

public class NewOrderMain {
    public static void main(String[] args) {
        try(KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>()) {
            try(KafkaDispatcher emailDispatcher = new KafkaDispatcher<Email>()) {
                for (int i = 0; i < 10; i++) {
                    String orderId = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                    //String userEmail = Math.random() + "@email.com";
                    String userEmail = "test@email.com";

                    Order order = new Order(orderId, amount, userEmail);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail, order);

                    Email emailContent = new Email("Order received", "Thank you for your order! We are processing your request");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userEmail, emailContent);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
