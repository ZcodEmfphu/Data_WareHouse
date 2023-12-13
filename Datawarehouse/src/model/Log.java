package model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import controller.LoadFromTempToDataWareHouse;

public class Log {

	private static Config cof = new Config();

	private static final Logger logger = LoggerFactory.getLogger(LoadFromTempToDataWareHouse.class);
	private static final String LOG_INSERT_QUERY = "INSERT INTO Log (Time, Title, Description, Config_Id, Status) VALUES (?, ?, ?, ?, ?)";

	public void insertLog(String title, String description, int configId, int status) {
		try (Connection controlConnection = getControlConnection();
				PreparedStatement insertLogStatement = controlConnection.prepareStatement(LOG_INSERT_QUERY)) {

			insertLogStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			insertLogStatement.setString(2, title);
			insertLogStatement.setString(3, description);
			insertLogStatement.setInt(4, configId);
			insertLogStatement.setInt(5, status);

			insertLogStatement.executeUpdate();
		} catch (SQLException e) {
			System.err.println("Error inserting log: " + e.getMessage());
		}
	}

	public Connection getControlConnection() throws SQLException {
		return cof.connectToDatabase("control");
	}

	public void logInfo(String message) {
		logger.info(message);
	}

	public void logError(String message, Throwable throwable) {
		logger.error(message, throwable);
	}

	public boolean latestLogHasStatus() {
		try (Connection dataWarehouseConnection = cof.connectToDatabase("control")) {
			String query = "SELECT l.Status FROM Log l ORDER BY l.Time DESC LIMIT 1";
			try (PreparedStatement preparedStatement = dataWarehouseConnection.prepareStatement(query);
					ResultSet resultSet = preparedStatement.executeQuery()) {
				if (resultSet.next()) {
					int latestStatus = resultSet.getInt("Status");
					logInfo("Latest Log Status: " + latestStatus);

					return latestStatus == 7|| latestStatus == 10;
				} else {
					logInfo("No log entries found.");
					return false;
				}
			}
		} catch (SQLException e) {
			logError("Error while checking latest log status: " + e.getMessage(), e);
			return false;
		}
	}

}
