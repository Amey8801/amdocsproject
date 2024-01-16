package com.processor.banktransaction;

import java.sql.*;

public class DisplayBankTrans {

    // Database connection details
    private static final String JDBC_URL = "jdbc:oracle:thin:@localhost:1521:XE";
    private static final String USERNAME = "scott";
    private static final String PASSWORD = "tiger";

    public static void main(String[] args) throws ClassNotFoundException {
        try {
            // Loading the Database Driver
            Class.forName("oracle.jdbc.driver.OracleDriver");

            // Establish database connection
            Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);

            // Display BankTrans table
            System.out.println("BankTrans Table:");
            displayTable(connection, "BankTrans");

            // Display ValidTrans table
            System.out.println("\nValidTrans Table:");
            displayTable(connection, "ValidTrans");

            // Display InvalidTrans table
            System.out.println("\nInvalidTrans Table:");
            displayTable(connection, "InvalidTrans");

            // Close the database connection
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void displayTable(Connection connection, String tableName) throws SQLException {
    	
        // Query to fetch all columns from the specified table
        String selectQuery = "SELECT * FROM " + tableName;
        
       
        // PreparedStatement to execute the query and obtains the resultset.
        try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery);
             ResultSet resultSet = selectStatement.executeQuery()) {
        	
        	//The ResultSetMetaData is used to get column names and count.
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Display header
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-20s", metaData.getColumnName(i));
            }
            System.out.println();

            // Display data
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.printf("%-20s", resultSet.getString(i));
                }
                System.out.println();
            }
        }
    }
}

