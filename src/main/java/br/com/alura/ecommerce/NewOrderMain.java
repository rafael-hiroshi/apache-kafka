package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaDispatcher dispatcher = new KafkaDispatcher();

        for (int i = 0; i < 10; i++) {
            String key = UUID.randomUUID().toString();
            String value = key + ", 17049, 3048952";
            dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

            String email = "Thank you for your order! We are processing your request";
            dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
        }
    }
}
