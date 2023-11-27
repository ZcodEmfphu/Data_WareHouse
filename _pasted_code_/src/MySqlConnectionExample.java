import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlConnectionExample {

    private static final Logger logger = LoggerFactory.getLogger(MySqlConnectionExample.class);

    private static final String STAGING_URL = "jdbc:mysql://localhost:3306/GiaVang_Staging";
    private static final String STAGING_USERNAME = "root";
    private static final String STAGING_PASSWORD = "";

    private static final String DATAMART_URL = "jdbc:mysql://localhost:3306/GiaVang_Datamart";
    private static final String DATAMART_USERNAME = "root";
    private static final String DATAMART_PASSWORD = "";

    public static void loadDataFromStagingToTemp() {
        try (
            Connection stagingConnection = DriverManager.getConnection(STAGING_URL, STAGING_USERNAME, STAGING_PASSWORD);
            Connection datamartConnection = DriverManager.getConnection(DATAMART_URL, DATAMART_USERNAME, DATAMART_PASSWORD)
        ) {
            // Query to select data from the TEMP table in the staging database
            String selectQuery = "SELECT Id, Date, Time, Product, BuyingPrice, SellingPrice FROM TEMP";

            // Query to insert data into the Temp table in the datamart database
            String insertQuery = "INSERT INTO Temp (Id, Date, Time, Product, BuyingPrice, SellingPrice) VALUES (?, ?, ?, ?, ?, ?)";

            try (PreparedStatement selectStatement = stagingConnection.prepareStatement(selectQuery);
                 ResultSet resultSet = selectStatement.executeQuery();
                 PreparedStatement insertStatement = datamartConnection.prepareStatement(insertQuery)) {

                // Process result set and insert into the Temp table
                while (resultSet.next()) {
                    // Retrieve data from the result set
                    int id = resultSet.getInt("Id");
                    String date = resultSet.getString("Date");
                    String time = resultSet.getString("Time");
                    String product = resultSet.getString("Product");
                    String buyingPrice = resultSet.getString("BuyingPrice");
                    String sellingPrice = resultSet.getString("SellingPrice");

                    // Set parameters for the insert statement
                    insertStatement.setInt(1, id);
                    insertStatement.setString(2, date);
                    insertStatement.setString(3, time);
                    insertStatement.setString(4, product);
                    insertStatement.setString(5, buyingPrice);
                    insertStatement.setString(6, sellingPrice);

                    // Execute the insert statement
                    insertStatement.executeUpdate();
                }
            }

        } catch (SQLException e) {
            logger.error("Error while loading data from staging to temp", e);
        }
    }

    public static void loadDataFromProductDimToProductFact() {
        try (
            Connection datamartConnection = DriverManager.getConnection(DATAMART_URL, DATAMART_USERNAME, DATAMART_PASSWORD)
        ) {
            // Query to select data from the Product_dim table in the datamart database
            String selectQuery = "SELECT Id, Name FROM Product_dim";

            // Query to insert data into the Product_fact table in the datamart database
            String insertQuery = "INSERT INTO Product_fact (Date_Id, Time, Date_ex, Product_Id, BuyingPrice, SellingPrice, Status, Config_Id) " +
                                 "VALUES (?, ?, NOW(), ?, ?, ?, 1, 1)";

            try (PreparedStatement selectStatement = datamartConnection.prepareStatement(selectQuery);
                 ResultSet resultSet = selectStatement.executeQuery();
                 PreparedStatement insertStatement = datamartConnection.prepareStatement(insertQuery)) {

                // Process result set and insert into the Product_fact table
                while (resultSet.next()) {
                    // Retrieve data from the result set
                    int id = resultSet.getInt("Id");
                    String name = resultSet.getString("Name");

                    // Set parameters for the insert statement
                    insertStatement.setInt(1, id);
                    // Set other parameters as needed

                    // Execute the insert statement
                    insertStatement.executeUpdate();
                }
            }

        } catch (SQLException e) {
            logger.error("Error while loading data from product_dim to product_fact", e);
        }
    }

    public static void main(String[] args) {
        // Call the methods to load data
        loadDataFromStagingToTemp();
        loadDataFromProductDimToProductFact();
    }
}
