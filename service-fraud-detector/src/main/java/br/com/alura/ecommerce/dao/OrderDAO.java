package br.com.alura.ecommerce.dao;

import br.com.alura.ecommerce.Order;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OrderDAO {

    private final Connection connection;

    public OrderDAO(Connection connection) {
        this.connection = connection;
    }

    public void insert(Order order, boolean isFraud) throws SQLException {
        PreparedStatement stm = connection.prepareStatement("INSERT INTO Orders (uuid, is_fraud) VALUES (?, ?)");
        stm.setString(1, order.getOrderId());
        stm.setBoolean(2, isFraud);
        stm.execute();
    }

    public ResultSet findById(String uuid) throws SQLException {
        PreparedStatement stm = connection.prepareStatement("SELECT uuid, is_fraud FROM Orders WHERE uuid = ? ");
        stm.setString(1, uuid);
        return stm.executeQuery();
    }
}
