package org.example.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnector {
    private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/Makeup_Shop";
    private static final String POSTGRES_USER = "Janine";
    private static final String POSTGRES_PASSWORD = "janine";

    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/Makeup_Shop";
    private static final String MYSQL_USER = "Janine";
    private static final String MYSQL_PASSWORD = "janine";

    public static Connection getPostgresConnection() throws SQLException {
        return DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
    }

    public static Connection getMySQLConnection() throws SQLException {
        return DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
    }
}