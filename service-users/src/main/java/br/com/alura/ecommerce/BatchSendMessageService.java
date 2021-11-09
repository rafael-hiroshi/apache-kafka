package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
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

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        BatchSendMessageService batchService = new BatchSendMessageService();
        KafkaService service = new KafkaService(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                new HashMap<>());
        service.run();
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new batch");

        Message<String> message = record.value();
        System.out.println("Topic: " + message.getPayload());

        for(User user : getAllUsers()) {
            userDispatcher.sendAsync(message.getPayload(), user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
            System.out.println("Sent to user " + user);
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
