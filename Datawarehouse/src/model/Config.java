package model;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import controller.LoadFromTempToDataWareHouse;

public class Config {

	public Connection getConnectionFromProperties(String connectionName) throws SQLException {
		Properties properties = loadProperties();
		String url = properties.getProperty(connectionName + ".url");
		String username = properties.getProperty(connectionName + ".username");
		String password = properties.getProperty(connectionName + ".password");
		return DriverManager.getConnection(url, username, password);
	}

	public Properties loadProperties() {
		Properties properties = new Properties();
		try (InputStream input = LoadFromTempToDataWareHouse.class.getClassLoader().getResourceAsStream("lib/config.properties")) {
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
}
