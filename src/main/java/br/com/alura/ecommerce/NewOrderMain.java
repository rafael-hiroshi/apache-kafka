package br.com.alura.ecommerce;

import java.util.UUID;

public class NewOrderMain {
    public static void main(String[] args) {
        try(KafkaDispatcher dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {
                String key = UUID.randomUUID().toString();
                String value = key + ", 17049, 3048952";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                String email = "Thank you for your order! We are processing your request";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
