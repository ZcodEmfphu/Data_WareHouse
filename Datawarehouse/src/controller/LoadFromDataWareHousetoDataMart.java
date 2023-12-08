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
