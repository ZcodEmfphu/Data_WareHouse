package Controller;

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
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWareHouse {

	private static final Logger logger = LoggerFactory.getLogger(DataWareHouse.class);
	private static final String LOG_INSERT_QUERY = "INSERT INTO Log (Time, Title, Description, Config_Id, Status) VALUES (?, ?, ?, ?, ?)";

//	1. Kết nối file cấu hình
	private static Connection getConnectionFromProperties(String connectionName) throws SQLException {
		Properties properties = loadProperties();
		String url = properties.getProperty(connectionName + ".url");
		String username = properties.getProperty(connectionName + ".username");
		String password = properties.getProperty(connectionName + ".password");
		return DriverManager.getConnection(url, username, password);
	}
//	2. Load file cấu hình
	private static Properties loadProperties() {
		Properties properties = new Properties();
		try (InputStream input = DataWareHouse.class.getClassLoader().getResourceAsStream("config.properties")) {
			if (input == null) {
				System.err.println("Sorry, unable to find config.properties");
				return properties;
			}
			properties.load(input);
		} catch (IOException e) {
			System.err.println("Error loading properties file: " + e.getMessage());
		}
		return properties;
	}
//	3. Xóa dữ liệu trong bảng fact
	public static void truncateProductFactTable() {
		try (Connection dataWarehouseConnection = getConnectionFromProperties("datawarehouse")) {
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
//4. Load dữ liệu vào bảng product_dim
	public static void loadDataFromTempToProductDim() {
		try (Connection stagingConnection = getConnectionFromProperties("satging");
				Connection dataWarehouseConnection = getConnectionFromProperties("datawarehouse")) {

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
	
//5. Load dữ liệu từ staging sang Datawarehouse
	public static void loadDataFromTempToDataWarehouse() {
		
		truncateProductFactTable();
		
		try (Connection stagingConnection = getConnectionFromProperties("staging");
				Connection dataWarehouseConnection = getConnectionFromProperties("datawarehouse")) {

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
				System.out.println("Data loaded from TEMP to Product_fact successfully.");
				logInfo("Data loaded from TEMP to Product_fact successfully.");
				insertLog("Load Data", "Data loaded from TEMP to Product_fact successfully.", 1, 8);
			} catch (SQLException e) {
				System.err.println("Error while loading data from TEMP to Product_fact: " + e.getMessage());
				logError("Error while loading data from TEMP to Product_fact: " + e.getMessage(), e);
				insertLog("Load Data", "Error while loading data from TEMP to Product_fact: " + e.getMessage(), 1, 9);
			}

		} catch (SQLException e) {
			System.err.println("Error connecting to databases: " + e.getMessage());
			logError("Unexpected error: " + e.getMessage(), e);
			insertLog("Load Data", "Unexpected error: " + e.getMessage(), 1, 9);
		}

	}

	private static void insertLog(String title, String description, int configId, int status) {
		try (Connection controlConnection = getControlConnection();
				PreparedStatement insertLogStatement = controlConnection.prepareStatement(LOG_INSERT_QUERY)) {

			insertLogStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			insertLogStatement.setString(2, title);
			insertLogStatement.setString(3, description);
			insertLogStatement.setInt(4, configId);
			insertLogStatement.setInt(5, status);

			insertLogStatement.executeUpdate();
			System.out.println("Log inserted successfully.");
		} catch (SQLException e) {
			System.err.println("Error inserting log: " + e.getMessage());
		}
	}

	private static Connection getControlConnection() throws SQLException {
		return getConnectionFromProperties("control");
	}

	private static void logInfo(String message) {
		logger.info(message);
	}

	private static void logError(String message, Throwable throwable) {
		logger.error(message, throwable);
	}

	public static void main(String[] args) {
		loadDataFromTempToDataWarehouse();
	}
}
