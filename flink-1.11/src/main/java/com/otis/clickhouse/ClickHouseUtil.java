package com.otis.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseUtil {
    private static Connection connection;

    public static Connection getConnection(String host, int port, String database) throws SQLException {
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(address);
    }

    public static Connection getConnection(String host, int port) throws SQLException, ClassNotFoundException {
        return getConnection(host, port, "default");
    }

    public void close() throws SQLException {
        connection.close();
    }

}
