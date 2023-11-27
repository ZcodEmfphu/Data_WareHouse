package Controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWareHouse {

	private static final Logger logger = LoggerFactory.getLogger(DataWareHouse.class);

	private static final String STAGING_URL = "jdbc:mysql://localhost:3306/GiaVang_Staging";
	private static final String STAGING_USERNAME = "root";
	private static final String STAGING_PASSWORD = "";

	private static final String DATAMART_URL = "jdbc:mysql://localhost:3306/GiaVang_Datamart";
	private static final String DATAMART_USERNAME = "root";
	private static final String DATAMART_PASSWORD = "";

	private static final String DATAWAREHOUSE_URL = "jdbc:mysql://localhost:3306/GiaVang_DataWarehouse";
	private static final String DATAWAREHOUSE_USERNAME = "root";
	private static final String DATAWAREHOUSE_PASSWORD = "";

	private static final String CONTROL_URL = "jdbc:mysql://localhost:3306/GiaVang_Control";
	private static final String CONTROL_USERNAME = "root";
	private static final String CONTROL_PASSWORD = "";

	public static void loadDataFromTempToProductDim() {
		try (Connection stagingConnection = DriverManager.getConnection(STAGING_URL, STAGING_USERNAME,STAGING_PASSWORD);
			 Connection dataWarehouseConnection = DriverManager.getConnection(DATAWAREHOUSE_URL,DATAWAREHOUSE_USERNAME, DATAWAREHOUSE_PASSWORD)) {
			
			String insertQuery = "INSERT INTO Product_dim (id, p_id, Name, time) VALUES (?, ?, ?, NOW())";
			String selectQuery = "SELECT Id, Product FROM TEMP";
			
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
				 PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {
				
				ResultSet resultSet = selectStatement.executeQuery();
				while (resultSet.next()) {
					int id = resultSet.getInt("Id");
					String product = resultSet.getString("Product");
					insertStatement.setInt(1, id);
					insertStatement.setInt(2, id);
					insertStatement.setString(3, product);
					
					insertStatement.executeUpdate();
				}

				System.out.println("Data loaded from TEMP to Product_dim successfully.");
			} catch (SQLException e) {
				System.err.println("Error while loading data from TEMP to Product_dim: " + e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error connecting to databases: " + e.getMessage());
		}
	}

	public static void truncateTable() {
		
		try (Connection dataWarehouseConnection = DriverManager.getConnection(DATAWAREHOUSE_URL, DATAWAREHOUSE_USERNAME,DATAWAREHOUSE_PASSWORD)) {
			
			String truncateQuery = "TRUNCATE TABLE product_dim";
			
			try (PreparedStatement truncateStatement = dataWarehouseConnection.prepareStatement(truncateQuery)) {
				truncateStatement.executeUpdate();
				System.out.println("Table truncated successfully.");
			} catch (SQLException e) {
				System.err.println("Error while truncating table " + ": " + e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error connecting to the database: " + e.getMessage());
		}
	}

	public static void loadDataFromTempToDateDim() {
		try (Connection stagingConnection = DriverManager.getConnection(STAGING_URL, STAGING_USERNAME, STAGING_PASSWORD);
			 Connection dataWarehouseConnection = DriverManager.getConnection(DATAWAREHOUSE_URL, DATAWAREHOUSE_USERNAME, DATAWAREHOUSE_PASSWORD)) {
			
			String selectQuery = "SELECT distinct Date FROM TEMP";
			String insertQuery = "INSERT INTO Date_dim (full_date) VALUES (?)";
			
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
				 ResultSet resultSet = selectStatement.executeQuery();
				 PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {
				
				while (resultSet.next()) {
					String date = resultSet.getString("Date");
					insertStatement.setString(1, date);
					insertStatement.executeUpdate();
				}
				System.out.println("Data loaded from TEMP to Date_dim successfully.");
			} catch (SQLException e) {
				System.err.println("Error while loading data from TEMP to Date_dim: " + e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error connecting to databases: " + e.getMessage());
		}
	}

	public static void loadDataFromTempToProductFact() {
		try (Connection stagingConnection = DriverManager.getConnection(STAGING_URL, STAGING_USERNAME,STAGING_PASSWORD);
			 Connection dataWarehouseConnection = DriverManager.getConnection(DATAWAREHOUSE_URL, DATAWAREHOUSE_USERNAME, DATAWAREHOUSE_PASSWORD)) {
			
			String selectQuery = "SELECT date_dim.d_id, temp.Id, temp.Time, temp.BuyingPrice, temp.SellingPrice "
								+"FROM giavang_staging.temp "
								+"JOIN giavang_datawarehouse.date_dim ON temp.Date = date_dim.full_date";
			
			String insertQuery = "INSERT INTO giavang_datawarehouse.product_fact (Date_Id, Time, Date_ex, Product_Id, BuyingPrice, SellingPrice, Status) "
								+"VALUES (?, ?, STR_TO_DATE('31/12/9999', '%d/%m/%Y'), ?, ?, ?, 8)";
			
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
				 ResultSet resultSet = selectStatement.executeQuery();
				 PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {
				
				while (resultSet.next()) {
					int dateId = resultSet.getInt("d_id");
					int tempId = resultSet.getInt("Id");
					String time = resultSet.getString("Time");
					String buyingPrice = resultSet.getString("BuyingPrice");
					String sellingPrice = resultSet.getString("SellingPrice");
					insertStatement.setInt(1, dateId);
					insertStatement.setString(2, time);
					insertStatement.setInt(3, tempId);
					insertStatement.setString(4, buyingPrice);
					insertStatement.setString(5, sellingPrice);
					
					insertStatement.executeUpdate();
				}
				System.out.println("Data loaded from TEMP to Product_fact successfully.");
			} catch (SQLException e) {
				System.err.println("Error while loading data from TEMP to Product_fact: " + e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error connecting to databases: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		loadDataFromTempToProductFact();
	}
}
