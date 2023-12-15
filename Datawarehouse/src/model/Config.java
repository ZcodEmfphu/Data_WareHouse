package model;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Config {

	// Method for establishing a database connection based on provided properties
	public Connection getConnectionFromProperties(Properties properties) throws SQLException {
		String url = "jdbc:mysql://" + properties.getProperty("host") + ":" + properties.getProperty("port") + "/"
				+ properties.getProperty("database");
		String username = properties.getProperty("username");
		String password = properties.getProperty("password");

		return DriverManager.getConnection(url, username, password);
	}

	// Method for loading properties from a configuration file
	public Properties loadProperties(String connectionName) {
		Properties properties = new Properties();
		try (InputStream input = Config.class.getClassLoader().getResourceAsStream("lib/config.properties")) {
			if (input == null) {
				System.err.println("Error: Unable to find config.properties");
				return properties;
			}
			properties.load(input);
			properties.putAll(loadSpecificProperties(connectionName, properties));
		} catch (IOException e) {
			System.err.println("Error loading properties file: " + e.getMessage());
		}
		return properties;
	}

	// Method for extracting specific properties based on connection name
	private Properties loadSpecificProperties(String connectionName, Properties properties) {
		Properties specificProperties = new Properties();
		specificProperties.setProperty("database", properties.getProperty(connectionName + ".database"));
		specificProperties.setProperty("host", properties.getProperty(connectionName + ".host"));
		specificProperties.setProperty("port", properties.getProperty(connectionName + ".port"));
		specificProperties.setProperty("username", properties.getProperty(connectionName + ".username"));
		specificProperties.setProperty("password", properties.getProperty(connectionName + ".password"));
		return specificProperties;
	}

	// Method for connecting to a database using a specified connection name
	public Connection connectToDatabase(String connectionName) {
		try {
			Properties properties = loadProperties(connectionName);
			return getConnectionFromProperties(properties);
		} catch (SQLException e) {
			System.err.println("Error connecting to the database: " + e.getMessage());
			return null;
		}
	}

	// Method for loading all configuration data from various sources
	public void loadDataFromPropertiesToConfig() {
		try (Connection conn = connectToDatabase("control")) {
			loadAllConfigData(conn);
		} catch (SQLException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public static void loadAllConfigData(Connection connection) throws SQLException {
		loadConfigData(connection, "staging");
		loadConfigData(connection, "datawarehouse");
		loadConfigData(connection, "datamart");
		loadConfigData(connection, "control");

    	System.out.println("Data loaded successfully.");
	}

	// Method for loading configuration data for a specific connection
	public static void loadConfigData(Connection connection, String connectionName) throws SQLException {
	    Properties properties = new Config().loadProperties(connectionName);

	    // Check if the process already exists in the Config table
	    if (processExists(connection, properties.getProperty("database"))) {
	        return;
	    }

	    String sql = "INSERT INTO Config (Process, Source, Username, Password, Port, Status) VALUES (?, ?, ?, ?, ?, ?)";

	    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
	        preparedStatement.setString(1, properties.getProperty("database"));
	        preparedStatement.setString(2, properties.getProperty("host"));
	        preparedStatement.setString(3, properties.getProperty("username"));
	        preparedStatement.setString(4, properties.getProperty("password"));
	        preparedStatement.setInt(5, Integer.parseInt(properties.getProperty("port")));

	        // Set a value for the 6th parameter (Status)
	        preparedStatement.setInt(6, 1);

	        preparedStatement.executeUpdate();
	    }
	}


	// Method for updating configuration data
	public void updateConfigData(Connection connection, String connectionName, String newTitle, String newDest,
			int newStatus) throws SQLException {
		loadDataFromPropertiesToConfig();
		Properties properties = new Config().loadProperties(connectionName);

		// Check if the process already exists in the Config table
		if (!processExists(connection, properties.getProperty("database"))) {
			return;
		}

		String sql = "UPDATE Config SET Title = ?, Dest = ?, Status = ? WHERE Process = ?";

		try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
			preparedStatement.setString(1, newTitle);
			preparedStatement.setString(2, newDest);
			preparedStatement.setInt(3, newStatus);
			preparedStatement.setString(4, properties.getProperty("database"));

			int rowsAffected = preparedStatement.executeUpdate();

			if (rowsAffected > 0) {
				System.out.println("Data updated for Process " + properties.getProperty("database"));
			} else {
				System.out.println("No matching data found for Process " + properties.getProperty("database"));
			}
		}
	}

	// Method for checking if a process already exists in the Config table
	private static boolean processExists(Connection connection, String processName) throws SQLException {
		String sql = "SELECT COUNT(*) FROM Config WHERE Process = ?";
		try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
			preparedStatement.setString(1, processName);
			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				return resultSet.next() && resultSet.getInt(1) > 0;
			}
		}
	}
}
