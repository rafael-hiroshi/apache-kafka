package br.com.alura;

import br.com.alura.ecommerce.KafkaDispatcher;
import br.com.alura.ecommerce.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    private final Connection connection;

    public CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        connection.createStatement().execute("Create table if not exists Users (uuid varchar(200) primary key, email varchar(200))");
    }

    public static void main(String[] args) throws SQLException {
        CreateUserService createUserService = new CreateUserService();
        KafkaService service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                new HashMap<>());
        service.run();
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.printf("Consumer Record: (%s, %s, %d, %d)\n", record.value());

        Order order = record.value();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        PreparedStatement insert = connection.prepareStatement("INSERT INTO Users (uuid, email) VALUES (?, ?)");
        insert.setString(1, "uuid");
        insert.setString(2, "email");
        insert.execute();
        System.out.println("User uuid and email " + email + " added");

    }

    private boolean isNewUser(String email) throws SQLException {
        PreparedStatement exists = connection.prepareStatement("SELECT uuid FROM Users WHERE email = ? limit 1");
        exists.setString(1, email);
        ResultSet results = exists.executeQuery();
        return !results.next();
    }
}
