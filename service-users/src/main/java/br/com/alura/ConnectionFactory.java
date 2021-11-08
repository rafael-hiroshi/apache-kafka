package br.com.alura;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    private Connection connection;

    public ConnectionFactory() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        connection.createStatement().execute("Create table if not exists Users (uuid varchar(200) primary key, email varchar(200))");
    }

    public Connection getConnection() {
        return this.connection;
    }
}
