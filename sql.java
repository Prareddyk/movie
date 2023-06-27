package com.example.movieratings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class App {
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/movieratings";
    private static final String DB_USERNAME = "your-username";
    private static final String DB_PASSWORD = "your-password";

    public static void main(String[] args) {
        try {
            createTables();
            loadDataFromCSV("data/movies.csv", "movies");
            loadDataFromCSV("data/ratings.csv", "ratings");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USERNAME, DB_PASSWORD)) {
            String createMoviesTable = "CREATE TABLE IF NOT EXISTS movies (" +
                    "tconst VARCHAR(255) PRIMARY KEY," +
                    "primaryTitle VARCHAR(255)," +
                    "runtimeMinutes INT," +
                    "genres VARCHAR(255)" +
                    ")";
            String createRatingsTable = "CREATE TABLE IF NOT EXISTS ratings (" +
                    "tconst VARCHAR(255) PRIMARY KEY," +
                    "averageRating DOUBLE," +
                    "numVotes INT" +
                    ")";

            try (PreparedStatement stmt1 = conn.prepareStatement(createMoviesTable);
                 PreparedStatement stmt2 = conn.prepareStatement(createRatingsTable)) {
                stmt1.executeUpdate();
                stmt2.executeUpdate();
            }
        }
    }

    private static void loadDataFromCSV(String csvFilePath, String tableName) throws SQLException {
        String insertQuery = "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USERNAME, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(insertQuery);
             BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");

                stmt.setString(1, data[0]);
                stmt.setString(2, data[1]);
                stmt.setInt(3, Integer.parseInt(data[2]));
                stmt.setString(4, data[3]);

                stmt.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}