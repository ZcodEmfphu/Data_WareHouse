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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.Config;
import model.Log;

public class LoadFromTempToDataWareHouse {

	private static Log log = new Log();
	private static Config cof = new Config();
	private static long startTime = System.currentTimeMillis();
	private static long duration = 1 * 60 * 1000;

//	1. Xóa dữ liệu trong bảng fact
	public static void truncateProductFactTable() {
		// 1.1. Kết nối với datawarehouse database
		try (Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {
			String truncateQuery = "TRUNCATE TABLE product_fact";
			// 1.2.1. Thực thi câu lệnh
			try (PreparedStatement truncateStatement = dataWarehouseConnection.prepareStatement(truncateQuery)) {
				truncateStatement.executeUpdate();
				// 1.3.1. Ghi log hệ thống về việc tranform(update) dữ liệu mới
				log.insertLog("Update", "Update Product_Fact thành công", 1, 5);
			} catch (SQLException e) {
				// 1.3.2. Ghi log về sự cố lỗi khi update dữ liệu
				log.insertLog("Update", "Error while update Product_Fact", 1, 10);
			}
		} catch (SQLException e) {
			// 1.2.2. Kết nối với datawarehouse database lỗi
			log.insertLog("Connect", "Error while connect to Datawarehouse", 1, 10);
			log.logError("Error while connect to Datawarehouse" + e.getMessage(), e);
		}
	}

//2. Load dữ liệu vào bảng product_dim
	public static void insertDataFromTempToProductDim() {
		// 2.1. Kết nối với staging databases và datawarehouse database
		try (Connection stagingConnection = cof.getConnectionFromProperties("staging");
				Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {
			// Câu lệnh query insert dữ liệu vào product_dim của datawarehouse database
			String insertQuery = "INSERT INTO Product_dim (id, p_id, Name, time) VALUES (?, ?, ?, NOW())";
			// Câu lệnh query chọn id và Product từ table temp của staging database
			String selectQuery = "SELECT Id, Product FROM TEMP";
			// 2.2.1. Thực thi câu lệnh
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
					PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {
				ResultSet resultSet = selectStatement.executeQuery();
				// Chạy vòng lặp để duyệt từng phần tử trong database
				while (resultSet.next()) {
					int id = resultSet.getInt("Id");
					String product = resultSet.getString("Product");
					insertStatement.setInt(1, id);
					insertStatement.setInt(2, id);
					insertStatement.setString(3, product);

					insertStatement.executeUpdate();
				}
				// 2.3.1. Ghi log hệ thống về việc tranform dữ liệu mới
				log.insertLog("Transform", "Transform dữ liệu từ Temp đến Product_dim thành công", 1, 6);
			} catch (SQLException e) {
				// 2.3.2. Ghi log hệ thống về việc tranform dữ liệu lỗi
				log.insertLog("Transform", "Transform dữ liệu từ Temp đến Product_dim thất bại", 1, 10);
			}
		} catch (SQLException e) {
			// 2.2.2. Ghi log hệ thống về việc kết nối với database thất bại
			log.logError("Error while connect to Datawarehouse or Staging" + e.getMessage(), e);
		}
	}

//3. Kiểm tra sản phẩm tồn tại 	
	public static boolean hasDataInProductDim() {
		// Kiểm tra kết nối với datawarehouse database
		try (Connection dataWarehouseConnection = cof.getConnectionFromProperties("datawarehouse")) {
			// Câu lệnh đếm số sản phẩm trong table product_dim của datawareehouse database
			String countQuery = "SELECT COUNT(*) FROM Product_dim";
			// Thực thi câu lệnh
			try (PreparedStatement countStatement = dataWarehouseConnection.prepareStatement(countQuery)) {
				ResultSet resultSet = countStatement.executeQuery();
				// Nếu kết quả phẩn tử đầu tiên
				if (resultSet.next()) {
					int count = resultSet.getInt(1);// Có tồn tại
					return count > 0;// Trả về lớn hơn 0
				}
			}
		} catch (SQLException e) {
			//Thông báo lỗi log hệ thống 
			log.logError("Error checking data in Product_dim: " + e.getMessage(), e);			
		}
		return false;
	}

//4. Update dữ liệu từ staging sang product_dim
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

//5. Load Data từ Temp sang Table Product_dim
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

//6. Kiểm tra tương thích dữ liệu
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

//7.Kiểm tra trạng thái của log
	public boolean latestLogHasStatus() {
		try (Connection dataWarehouseConnection = cof.getConnectionFromProperties("control")) {
			String query = "SELECT l.Status FROM Log l ORDER BY l.Time DESC LIMIT 1";
			try (PreparedStatement preparedStatement = dataWarehouseConnection.prepareStatement(query);
					ResultSet resultSet = preparedStatement.executeQuery()) {
				if (resultSet.next()) {
					int latestStatus = resultSet.getInt("Status");
					log.logInfo("Latest Log Status: " + latestStatus);

					return latestStatus == 7;
				} else {
					log.logInfo("No log entries found.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.logError("Error while checking latest log status: " + e.getMessage(), e);
			return false;
		}
	}

//9. Load dữ liệu từ staging sang Datawarehouse
	public void loadDataFromTempToDataWarehouse() {

		if (latestLogHasStatus()) {
			log.logInfo("Stopping data loading because the latest log has Status ID 7.");
			return;
		}

		log.insertLog("Loading", "Data Loading TEMP to Product_fact ", 1, 7);
		log.logInfo("Loading....");

		while (System.currentTimeMillis() - startTime < duration) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		truncateProductFactTable();
		loadDataFromTempToProductDim();

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

				while (resultSet.next() && (System.currentTimeMillis() - startTime) < (3 * 60 * 1000)) {
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
				log.insertLog("Loaded", "Data loaded from TEMP to Product_fact successfully.", 1, 8);
			} catch (SQLException e) {
				log.logError("Error while loading data from TEMP to Product_fact: " + e.getMessage(), e);
				log.insertLog("Load Data", "Error while loading data from TEMP to Product_fact: " + e.getMessage(), 1,
						9);
			}
		} catch (SQLException e) {
			log.logError("Unexpected error: " + e.getMessage(), e);
			log.insertLog("Load Data", "Unexpected error: " + e.getMessage(), 1, 9);
		}
	}

}
