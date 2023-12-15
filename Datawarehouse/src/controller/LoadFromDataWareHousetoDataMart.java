package controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import model.Config;
import model.Log;

public class LoadFromDataWareHousetoDataMart {

	private static Log log = new Log();
	private static Config cof = new Config();
	private static long startTime = System.currentTimeMillis();
	private static long duration = 1 * 60 * 1000;

	// Load datawarehouse vào datatamart
	public void LoadDatawarehouseToDataMart() {
	    try {
	        // Kết nối đến Data Warehouse
	        Connection dwConnection = cof.connectToDatabase("datawarehouse");

	        // Cập nhật cột Date_ex trong Data Warehouse
	        updateDateEx();

	        // Truy vấn Data Warehouse để lấy dữ liệu mới nhất
	        String dwQuery = "SELECT dd.full_date AS Date_ef, MAX(CURTIME()) AS Time, '2999-12-31' AS Date_ex, pf.Product_Id, pd.Name AS Product_Name, pf.BuyingPrice, pf.SellingPrice "
	                + "FROM GiaVang_DataWarehouse.Product_fact pf "
	                + "JOIN GiaVang_DataWarehouse.Product_dim pd ON pf.Product_Id = pd.p_id "
	                + "JOIN GiaVang_DataWarehouse.Date_dim dd ON pf.Date_ef = dd.d_id "
	                + "GROUP BY dd.full_date, pf.Product_Id, pd.Name, pf.BuyingPrice, pf.SellingPrice";

	        try (PreparedStatement dwStatement = dwConnection.prepareStatement(dwQuery);
	             ResultSet resultSet = dwStatement.executeQuery()) {

	            // Kết nối đến Data Mart
	            Connection dmConnection = cof.connectToDatabase("datamart");

	            try {
	                // Tắt autocommit cho kết nối Data Mart
	                dmConnection.setAutoCommit(false);

	                // Chèn dữ liệu vào Data Mart
	                String dmQuery = "INSERT INTO AGGREGATE (Date_ef, Time, Date_ex, Product_Id, Product_Name, BuyingPrice, SellingPrice) "
	                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";

	                try (PreparedStatement dmStatement = dmConnection.prepareStatement(dmQuery)) {

	                    // Duyệt qua result set và thêm vào batch để chèn nhiều dữ liệu cùng một lúc
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

	                    // Ghi log thông báo thành công và thực hiện batch insert
	                    log.insertLog("Loaded", "Load data từ datawarehouse sang datamart thành công", 1, 8);
	                    dmStatement.executeBatch();
	                    dmConnection.commit();
	                }
	            } catch (SQLException e) {
	                // Xử lý ngoại lệ và có thể rollback transaction
	                dmConnection.rollback();
	                e.printStackTrace();
	            } finally {
	                // Bật autocommit cho kết nối Data Mart (tùy chọn, tùy thuộc vào logic ứng dụng của bạn)
	                dmConnection.setAutoCommit(true);

	                // Đóng kết nối Data Mart
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
			Connection dmConnection = cof.connectToDatabase("datamart");

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
