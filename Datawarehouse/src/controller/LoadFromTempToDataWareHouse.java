package controller;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.Config;
import model.Log;

public class LoadFromTempToDataWareHouse {

	private static Log log = new Log();
	private static Config cof = new Config();

//	1. Xóa dữ liệu trong bảng fact
	public static void truncateProductFactTable() {
		try (Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {
			String truncateQuery = "TRUNCATE TABLE product_fact";

			try (PreparedStatement truncateStatement = dataWarehouseConnection.prepareStatement(truncateQuery)) {
				truncateStatement.executeUpdate();
				System.out.println("Table product_fact truncated successfully.");
			} catch (SQLException e) {
				System.err.println("Error while truncating table product_fact: " + e.getMessage());
			}
		} catch (SQLException e) {
			System.err.println("Error connecting to the database: " + e.getMessage());
		}
	}

//2. Load dữ liệu vào bảng product_dim
	public static void insertDataFromTempToProductDim() {
		try (Connection stagingConnection = cof.getConnectionFromProperties("staging");
				Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {

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

//5. Kiểm tra sản phẩm tồn tại 	
	public static boolean hasDataInProductDim() {
		try (Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {
			String countQuery = "SELECT COUNT(*) FROM Product_dim";
			try (PreparedStatement countStatement = dataWarehouseConnection.prepareStatement(countQuery)) {
				ResultSet resultSet = countStatement.executeQuery();
				if (resultSet.next()) {
					int count = resultSet.getInt(1);
					return count > 0;
				}
			}
		} catch (SQLException e) {
			log.logError("Error checking data in Product_dim: " + e.getMessage(), e);
			log.insertLog("Tranfer", e.getMessage(), 1, 10);
		}
		return false;
	}

//6. Update dữ liệu từ staging sang product_dim
	public static void updateDataFromTempToProductDim() {
		try (Connection stagingConnection = cof.getConnectionFromProperties("staging");
				Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {
			String insertQuery = "INSERT INTO Product_dim (id, p_id, Name, time) VALUES (?, ?, ?, ?) "
					+ "ON DUPLICATE KEY UPDATE Name = VALUES(Name), time = VALUES(time)";
			String selectQuery = "SELECT Id, Product FROM TEMP";
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
					PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {
				ResultSet resultSet = selectStatement.executeQuery();
				int updatedRows = 0;
				while (resultSet.next()) {
					int id = resultSet.getInt("Id");
					String productFromStaging = resultSet.getString("Product");
					// Get the existing Name from Product_dim before the update
					String existingName = getProductNameFromProductDim(dataWarehouseConnection, id);
					// Set values for the insert statement
					insertStatement.setInt(1, id);
					insertStatement.setInt(2, id);
					insertStatement.setString(3, productFromStaging);
					// Set the current timestamp for the time column
					insertStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
					// Execute the insert statement and get the number of updated rows
					int rowsAffected = insertStatement.executeUpdate();
					// Get the updated Name from Product_dim after the update
					String updatedName = getProductNameFromProductDim(dataWarehouseConnection, id);
					if (rowsAffected > 0 && !Objects.equals(existingName, updatedName)) {
						// Row was updated and Name changed
						log.logInfo("Row with id " + id + " was updated. Name changed from '" + existingName + "' to '"
								+ updatedName + "'.");
						log.insertLog("Tranfer", existingName + "' to '" + updatedName, 1, 8);
						updatedRows++;
					}
				}
				log.logInfo(updatedRows + " rows updated from TEMP to Product_dim successfully.");
			} catch (SQLException e) {
				log.logError("Error while updating data from TEMP to Product_dim: " + e.getMessage(), e);
			}
		} catch (SQLException e) {
			log.logError("Error connecting to databases: " + e.getMessage(), e);
		}
	}

//7. Load Data từ Temp sang Table Product_dim
	public static void loadDataFromTempToProductDim() {
		try {
			// Check if Product_dim has data
			if (hasDataInProductDim()) {
				// Update data from TEMP to Product_dim
				updateDataFromTempToProductDim();
			} else {
				// Load data from TEMP to Product_dim
				insertDataFromTempToProductDim();
			}
			log.logInfo("Load and update process completed successfully.");
		} catch (Exception e) {
			log.logError("Error during load and update process: " + e.getMessage(), e);
		}
	}

//8. Kiểm tra tương thích dữ liệu
	private static String getProductNameFromProductDim(Connection connection, int id) throws SQLException {
		String selectQuery = "SELECT Name FROM Product_dim WHERE id = ?";
		try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery)) {
			selectStatement.setInt(1, id);
			ResultSet resultSet = selectStatement.executeQuery();
			if (resultSet.next()) {
				return resultSet.getString("Name");
			} else {
				return null;
			}
		}
	}

//5. Load dữ liệu từ staging sang Datawarehouse
	public void loadDataFromTempToDataWarehouse() {

		truncateProductFactTable();

		loadDataFromTempToProductDim();

		log.insertLog("Loading", "Data TEMP to Product_fact Loading....", 1, 7);
		try (Connection stagingConnection = cof.getConnectionFromProperties("staging");
				Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {

			String selectQuery = "SELECT date_dim.d_id, temp.Id, temp.Time, temp.BuyingPrice, temp.SellingPrice "
					+ "FROM giavang_staging.temp "
					+ "JOIN giavang_datawarehouse.date_dim ON temp.Date = date_dim.full_date";

			String insertQuery = "INSERT INTO giavang_datawarehouse.product_fact (Date_Id, Time, Date_ex, Product_Id, BuyingPrice, SellingPrice, Status) "
					+ "VALUES (?, ?, STR_TO_DATE('31/12/9999', '%d/%m/%Y'), ?, ?, ?, 8)";

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
				log.logInfo("Data loaded from TEMP to Product_fact successfully.");
				log.insertLog("Load Data", "Data loaded from TEMP to Product_fact successfully.", 1, 8);

			} catch (SQLException e) {
				System.err.println("Error while loading data from TEMP to Product_fact: " + e.getMessage());
				log.logError("Error while loading data from TEMP to Product_fact: " + e.getMessage(), e);
				log.insertLog("Load Data", "Error while loading data from TEMP to Product_fact: " + e.getMessage(), 1,
						9);
			}

		} catch (SQLException e) {
			System.err.println("Error connecting to databases: " + e.getMessage());
			log.logError("Unexpected error: " + e.getMessage(), e);
			log.insertLog("Load Data", "Unexpected error: " + e.getMessage(), 1, 9);
		}

	}

// Load datawarehouse vào datatamart
	public static void LoadDatawarehouseToDataMart() {
		try {
			// Connect to Data Warehouse
			Connection dwConnection = cof.getConnectionFromProperties("datawarehouse");
			// Extract data from Data Warehouse
			updateDateEx();

			String dwQuery = "SELECT dd.full_date AS Date_ef, CURTIME() AS Time, '2999-12-31' AS Date_ex, pf.Product_Id, pd.Name AS Product_Name, pf.BuyingPrice, pf.SellingPrice "
					+ "FROM GiaVang_DataWarehouse.Product_fact pf "
					+ "JOIN GiaVang_DataWarehouse.Product_dim pd ON pf.Product_Id = pd.p_id "
					+ "JOIN GiaVang_DataWarehouse.Date_dim dd ON pf.Date_Id = dd.d_id";

			try (PreparedStatement dwStatement = dwConnection.prepareStatement(dwQuery);
					ResultSet resultSet = dwStatement.executeQuery()) {

				// Connect to Data Mart
				Connection dmConnection = cof.getConnectionFromProperties("datamart");

				try {
					// Disable autocommit for Data Mart connection
					dmConnection.setAutoCommit(false);

					// Insert data into Data Mart
					String dmQuery = "INSERT INTO AGGREGATE (Date_ef, Time, Date_ex, Product_Id, Product_Name, BuyingPrice, SellingPrice) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?)";

					try (PreparedStatement dmStatement = dmConnection.prepareStatement(dmQuery)) {

						while (resultSet.next()) {
							dmStatement.setString(1, resultSet.getString("Date_ef"));
							dmStatement.setString(2, resultSet.getString("Time"));
							dmStatement.setString(3, resultSet.getString("Date_ex"));
							dmStatement.setInt(4, resultSet.getInt("Product_Id"));
							dmStatement.setString(5, resultSet.getString("Product_Name"));
							dmStatement.setDouble(6, resultSet.getDouble("BuyingPrice"));
							dmStatement.setDouble(7, resultSet.getDouble("SellingPrice"));

							dmStatement.addBatch();
						}
						System.out.println("Loading from datawarehouse to datamart successly !");
						dmStatement.executeBatch();
						dmConnection.commit();
					}
				} catch (SQLException e) {
					// Handle exceptions and possibly rollback the transaction
					dmConnection.rollback();
					e.printStackTrace();
				} finally {
					// Enable autocommit for Data Mart connection (optional, depending on your
					// application logic)
					dmConnection.setAutoCommit(true);

					// Close the Data Mart connection
					dmConnection.close();
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

// Update ngày hết hạn
	public static void updateDateEx() {
		try {
			// Connect to Data Mart
			Connection dmConnection = cof.getConnectionFromProperties("datamart");

			try {
				// Disable autocommit for Data Mart connection
				dmConnection.setAutoCommit(false);

				// Find the maximum time value
				String maxTimeQuery = "SELECT MAX(Time) AS MaxTime FROM AGGREGATE";

				try (PreparedStatement maxTimeStatement = dmConnection.prepareStatement(maxTimeQuery);
						ResultSet maxTimeResult = maxTimeStatement.executeQuery()) {

					if (maxTimeResult.next()) {
						// Get the maximum time value
						String maxTime = maxTimeResult.getString("MaxTime");

						// Update Date_ex where Time is the maximum
						String updateQuery = "UPDATE AGGREGATE SET Date_ex = CURDATE() WHERE Time = ?";

						try (PreparedStatement updateStatement = dmConnection.prepareStatement(updateQuery)) {
							updateStatement.setString(1, maxTime);
							updateStatement.executeUpdate();
							dmConnection.commit();
							System.out.println("Update Date_ex successfully!");
						}
					}
				}
			} catch (SQLException e) {
				// Handle exceptions and possibly rollback the transaction
				dmConnection.rollback();
				e.printStackTrace();
			} finally {
				// Enable autocommit for Data Mart connection (optional, depending on your
				// application logic)
				dmConnection.setAutoCommit(true);

				// Close the Data Mart connection
				dmConnection.close();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
