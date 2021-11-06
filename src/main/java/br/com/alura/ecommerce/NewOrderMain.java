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
        int i = 1;
        while(i < 100) {

        KafkaProducer producer = new KafkaProducer<String, String>(properties());
        String key = UUID.randomUUID().toString();
        String value = key + ", 17049, 3048952";
        ProducerRecord record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

        Callback callback = (data, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }
            System.out.println("Message sent " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        String email = "Thank you for your order! We are processing your request";
        ProducerRecord emailRecord = new ProducerRecord("ECOMMERCE_SEND_EMAIL", key ,email);
        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
        i++;
        }

    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
