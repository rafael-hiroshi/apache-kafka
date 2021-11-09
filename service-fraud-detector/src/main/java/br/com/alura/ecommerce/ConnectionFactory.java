package br.com.alura.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    private Connection connection;

    public ConnectionFactory() throws SQLException {
        String url = "jdbc:sqlite:target/frauds_database.db";
        this.connection = DriverManager.getConnection(url);
        connection.createStatement().execute("Create table if not exists Orders (uuid varchar(200) primary key, is_fraud boolean)");
    }

    public Connection getConnection() {
        return this.connection;
    }
}
