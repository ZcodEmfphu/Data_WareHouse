package controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
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
		try (Connection dataWarehouseConnection = cof.connectToDatabase("datawarehouse")) {
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
		try (Connection stagingConnection = cof.connectToDatabase("staging");
				Connection dataWarehouseConnection = cof.connectToDatabase("datawarehouse")) {
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

//3. Kiểm tra sản phẩm tồn tại trong table	
	public static boolean hasDataInProductDim() {
		// Kiểm tra kết nối với datawarehouse database
		try (Connection dataWarehouseConnection = cof.connectToDatabase("datawarehouse")) {
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
			// Thông báo lỗi log hệ thống
			log.logError("Error checking data in Product_dim: " + e.getMessage(), e);
		}
		return false;
	}

//4. Update dữ liệu từ staging sang product_dim
	public static void updateDataFromTempToProductDim() {
		// Kết nối với staging databases và datawarehouse database
		try (Connection stagingConnection = cof.connectToDatabase("staging");
				Connection dataWarehouseConnection = cof.connectToDatabase("datawarehouse")) {

			// Query SQL để insert hoặc update dữ liệu vào bảng Product_dim
			String insertQuery = "INSERT INTO Product_dim (id, p_id, Name, time) VALUES (?, ?, ?, ?) "
					+ "ON DUPLICATE KEY UPDATE Name = VALUES(Name), time = VALUES(time)";
			// Query SQL để lấy dữ liệu từ bảng TEMP
			String selectQuery = "SELECT Id, Product FROM TEMP";

			// Thực thi câu lệnh SQL
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
					PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {
				ResultSet resultSet = selectStatement.executeQuery();
				int updatedRows = 0;
				while (resultSet.next()) {
					int id = resultSet.getInt("Id");
					String productFromStaging = resultSet.getString("Product");

					// Lấy tên sản phẩm từ bảng Product_dim trước khi cập nhật
					String existingName = getProductNameFromProductDim(dataWarehouseConnection, id);

					// Thiết lập các tham số cho câu lệnh insert
					insertStatement.setInt(1, id);
					insertStatement.setInt(2, id);
					insertStatement.setString(3, productFromStaging);
					// Thiết lập thời gian
					insertStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));

					// Thực hiện câu lệnh insert và lấy số hàng bị ảnh hưởng
					int rowsAffected = insertStatement.executeUpdate();

					// Lấy tên đã cập nhật từ Product_dim sau khi cập nhật
					String updatedName = getProductNameFromProductDim(dataWarehouseConnection, id);

					// Nếu có hàng bị cập nhật và tên thay đổi
					if (rowsAffected > 0 && !Objects.equals(existingName, updatedName)) {
						// Log thông tin về việc cập nhật hàng
						log.logInfo("Row with id " + id + " was updated. Name changed from '" + existingName + "' to '"
								+ updatedName + "'.");
						// Ghi log
						log.insertLog("Tranfer", existingName + " to " + updatedName, 1, 8);
						updatedRows++;
					}
				}
				log.logInfo(updatedRows + " rows updated from TEMP to Product_dim successfully.");
			} catch (SQLException e) {
				// Log lỗi khi cập nhật dữ liệu từ TEMP vào Product_dim
				log.logError("Error while updating data from TEMP to Product_dim: " + e.getMessage(), e);
			}
		} catch (SQLException e) {
			// Log lỗi khi kết nối đến cơ sở dữ liệu
			log.logError("Error connecting to databases: " + e.getMessage(), e);
		}
	}

//5. Load Data từ Temp sang Table Product_dim
	public static void loadDataFromTempToProductDim() {
		try {
			// Kiểm tra xem Product_dim có dữ liệu không
			if (hasDataInProductDim()) {
				// Nếu có dữ liệu, cập nhật dữ liệu từ TEMP vào Product_dim
				updateDataFromTempToProductDim();
			} else {
				// Nếu không có dữ liệu, tải dữ liệu từ TEMP vào Product_dim
				insertDataFromTempToProductDim();
			}
			log.logInfo("Load and update process completed successfully.");
		} catch (Exception e) {
			// Log lỗi nếu có lỗi trong quá trình tải và cập nhật dữ liệu
			log.logError("Error during load and update process: " + e.getMessage(), e);
		}
	}

//6. Kiểm tra tương thích dữ liệu
	private static String getProductNameFromProductDim(Connection connection, int id) throws SQLException {
		// Câu truy vấn SQL để lấy tên từ bảng Product_dim dựa trên id
		String selectQuery = "SELECT Name FROM Product_dim WHERE id = ?";

		// Sử dụng try-with-resources để quản lý PreparedStatement và tự động đóng tài
		// nguyên
		try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery)) {
			// Thiết lập tham số trong PreparedStatement
			selectStatement.setInt(1, id);

			// Thực hiện truy vấn và nhận ResultSet chứa kết quả
			ResultSet resultSet = selectStatement.executeQuery();

			// Kiểm tra xem có kết quả nào trong ResultSet không
			if (resultSet.next()) {
				// Trả về tên sản phẩm nếu có kết quả
				return resultSet.getString("Name");
			} else {
				// Trả về null nếu không có kết quả
				return null;
			}
		}
	}

//7. Load dữ liệu từ staging sang Datawarehouse
	public void loadDataFromTempToDataWarehouse() {
		// Kiểm tra xem log mới nhất có Status ID là 7 không
		if (log.latestLogHasStatus()) {
			log.logInfo("Dừng quá trình tải dữ liệu");
			log.insertLog("Error", "Tiến trình đang được khởi tạo vui lòng chờ", 1, 10);
			return;
		}

		// Ghi log thông báo bắt đầu quá trình tải dữ liệu
		log.insertLog("Loading", "Data Loading TEMP to Product_fact ", 1, 7);
		log.logInfo("Đang tải dữ liệu....");

		// Chờ trong khoảng thời gian đã định
		while (System.currentTimeMillis() - startTime < duration) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Cắt bỏ dữ liệu trong bảng Product_fact
		truncateProductFactTable();

		// Tải dữ liệu từ TEMP vào Product_dim
		loadDataFromTempToProductDim();

		// Kết nối đến cơ sở dữ liệu staging và data warehouse
		try (Connection stagingConnection = cof.connectToDatabase("staging");
				Connection dataWarehouseConnection = cof.connectToDatabase("datawarehouse")) {

			// Câu truy vấn SQL để chọn dữ liệu từ bảng staging kết hợp với date_dim
			String selectQuery = "SELECT date_dim.d_id, temp.Id, temp.Time, temp.BuyingPrice, temp.SellingPrice "
					+ "FROM giavang_staging.temp "
					+ "JOIN giavang_datawarehouse.date_dim ON temp.Date = date_dim.full_date";

			// Câu truy vấn SQL để chèn dữ liệu vào bảng data warehouse
			String insertQuery = "INSERT INTO giavang_datawarehouse.product_fact (Date_Id, Time, Date_ex, Product_Id, BuyingPrice, SellingPrice, Status) "
					+ "VALUES (?, ?, STR_TO_DATE('31/12/9999', '%d/%m/%Y'), ?, ?, ?, 8)";

			// Thực hiện các câu truy vấn SQL
			try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
					ResultSet resultSet = selectStatement.executeQuery();
					PreparedStatement insertStatement = dataWarehouseConnection.prepareStatement(insertQuery)) {

				// Xử lý kết quả và chèn dữ liệu vào bảng data warehouse
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

				// Ghi log thông báo thành công
				log.logInfo("Dữ liệu được tải từ TEMP vào Product_fact thành công.");
				log.insertLog("Loaded", "Dữ liệu được tải từ TEMP vào Product_fact thành công.", 1, 8);
			} catch (SQLException e) {
				// Ghi log lỗi nếu có ngoại lệ trong quá trình tải dữ liệu
				log.logError("Lỗi khi tải dữ liệu từ TEMP vào Product_fact: " + e.getMessage(), e);
				log.insertLog("Load Data", "Lỗi khi tải dữ liệu từ TEMP vào Product_fact: " + e.getMessage(), 1, 9);
			}
		} catch (SQLException e) {
			// Ghi log lỗi không mong muốn nếu có ngoại lệ trong quá trình kết nối cơ sở dữ
			// liệu
			log.logError("Lỗi không mong muốn: " + e.getMessage(), e);
			log.insertLog("Load Data", "Lỗi không mong muốn: " + e.getMessage(), 1, 9);
		}
	}

}
