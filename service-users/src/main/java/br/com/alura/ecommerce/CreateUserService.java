package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final Connection connection;

    public CreateUserService() throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
    }

    public static void main(String[] args) {
        new ServiceRunner(CreateUserService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.printf("Consumer Record: (%s)\n", record.value());

        Order order = record.value().getPayload();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        PreparedStatement insert = connection.prepareStatement("INSERT INTO Users (uuid, email) VALUES (?, ?)");
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("User " + uuid + " and email " + email + " added");

    }

    private boolean isNewUser(String email) throws SQLException {
        PreparedStatement exists = connection.prepareStatement("SELECT uuid FROM Users WHERE email = ? limit 1");
        exists.setString(1, email);
        ResultSet results = exists.executeQuery();
        return !results.next();
    }
}
