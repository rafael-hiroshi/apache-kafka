package br.com.alura;

import br.com.alura.ecommerce.KafkaDispatcher;
import br.com.alura.ecommerce.KafkaService;
import br.com.alura.ecommerce.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
    private final Connection connection;

    public BatchSendMessageService() throws SQLException {
        this.connection = new ConnectionFactory().getConnection();
    }

    public static void main(String[] args) throws SQLException {
        BatchSendMessageService batchService = new BatchSendMessageService();
        KafkaService<String> service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                String.class,
                new HashMap<>());
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new batch");
        System.out.printf("Topic: (%s)\n", record.topic());

        for(User user : getAllUsers()) {
            userDispatcher.send(record.value(), user.getUuid(), user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        ResultSet results = connection.prepareStatement("SELECT uuid FROM Users").executeQuery();

        List<User> users = new ArrayList<>();
        while(results.next()) {
            users.add(new User(results.getString((1))));
        }
        return users;
    }
}
