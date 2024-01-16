package com.processor.banktransaction;

import java.sql.*;
public class BankTransUpdater
{
	

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

	            // Process transactions and update tables
	            processTransactionsAndUpdateTables(connection);

	            // Close the database connection
	            connection.close();

	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }

	    private static void processTransactionsAndUpdateTables(Connection connection) throws SQLException {
	        // Query to fetch transactions from BankTrans table
	        String selectQuery = "SELECT TransID, OldBal, TransType, TransAmt, Transstat FROM BankTrans";

	        try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery);
	             ResultSet resultSet = selectStatement.executeQuery()) {
	        	
	        	int count=0;
	            while (resultSet.next()) {
	                int transID = resultSet.getInt("TransID");
	                double oldBal = resultSet.getDouble("OldBal");
	                String transType = resultSet.getString("TransType");
	                double transAmt = resultSet.getDouble("TransAmt");
	                
	                 // Calculate new balance based on transaction details
	                double newBal = oldBal + transAmt;
	                
	                // Retrieve the transaction status from the result set
	                String transStat = resultSet.getString("Transstat");
	                
	                // Check if the transaction status is null
	                if(transStat == null) {
	                	
	                	// Determine the transaction status based on the new balance
	                	transStat = (newBal >= 0) ? "Valid" : "Invalid";
	                	
	                	// Log the information into ValidTrans or InvalidTrans table
	                	logTransaction(connection, transID, transType, transAmt, transStat);
	                	System.out.println("Data Updated");
	                	count++;
	                }
	             

	                // Update NewBal and TransStat in BankTrans table
	                updateTransaction(connection, transID, newBal, transStat);
	                
	            }
	            if( count == 0) {
	            	System.out.println("There NO Data To Update");
	            }
	        }
	    }

	    private static void updateTransaction(Connection connection, int transID, double newBal, String transStat)
	            throws SQLException {
	        // Update NewBal and TransStat in BankTrans table
	        String updateQuery = "UPDATE BankTrans SET NewBal=?, TransStat=? WHERE TransID=?";
	        try (PreparedStatement updateStatement = connection.prepareStatement(updateQuery)) {
	            updateStatement.setDouble(1, newBal);
	            updateStatement.setString(2, transStat);
	            updateStatement.setInt(3, transID);
	            updateStatement.executeUpdate();
	        }
	    }

	    private static void logTransaction(Connection connection, int transID, String transType, double transAmt,
	                                       String validity) throws SQLException {
	        // Log the information into ValidTrans or InvalidTrans table
	        String logQuery = "INSERT INTO " + (validity.equals("Valid") ? "ValidTrans" : "InvalidTrans") +
	                "(TransID, TransType, TransAmt, Validity) VALUES (?, ?, ?, ?)";
	        try (PreparedStatement logStatement = connection.prepareStatement(logQuery)) {
	            logStatement.setInt(1, transID);
	            logStatement.setString(2, transType);
	            logStatement.setDouble(3, transAmt);
	            logStatement.setString(4, validity);
	            logStatement.executeUpdate();
	        }
	    }
	}