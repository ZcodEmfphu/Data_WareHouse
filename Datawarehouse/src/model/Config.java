package model;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import controller.LoadFromTempToDataWareHouse;

public class Config {

	  public Connection getConnectionFromProperties(Properties properties) throws SQLException {
	        String url = "jdbc:mysql://" +
	                properties.getProperty("host") + ":" +
	                properties.getProperty("port") + "/" +
	                properties.getProperty("database");

	        String username = properties.getProperty("username");
	        String password = properties.getProperty("password");

	        return DriverManager.getConnection(url, username, password);
	    }

	    public Properties loadProperties(String connectionName) {
	        Properties properties = new Properties();
	        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("lib/config.properties")) {
	            if (input == null) {
	                System.err.println("Sorry, unable to find config.properties");
	                return properties;
	            }
	            properties.load(input);
	            properties.putAll(loadSpecificProperties(connectionName, properties));
	        } catch (IOException e) {
	            System.err.println("Error loading properties file: " + e.getMessage());
	        }
	        return properties;
	    }

	    private Properties loadSpecificProperties(String connectionName, Properties properties) {
	        Properties specificProperties = new Properties();
	        specificProperties.setProperty("database", properties.getProperty(connectionName + ".database"));
	        specificProperties.setProperty("host", properties.getProperty(connectionName + ".host"));
	        specificProperties.setProperty("port", properties.getProperty(connectionName + ".port"));
	        specificProperties.setProperty("username", properties.getProperty(connectionName + ".username"));
	        specificProperties.setProperty("password", properties.getProperty(connectionName + ".password"));
	        return specificProperties;
	    }

	    public Connection connectToDatabase(String connectionName) {
	        try {
	            Properties properties = loadProperties(connectionName);
	            return getConnectionFromProperties(properties);
	        } catch (SQLException e) {
	            System.err.println("Error connecting to the database: " + e.getMessage());
	            return null;
	        }
	    }
 
}
