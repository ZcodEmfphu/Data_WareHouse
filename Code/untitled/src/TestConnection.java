import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestConnection {
    public static void main(String[] args) {
        String url ="jdbc://localhost:3306/Test";
        String username="root";
        String password="";
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection conn = DriverManager.getConnection(url,username,password);

            Statement statement = conn.createStatement();

            ResultSet res = statement.executeQuery("select * from data");

            while(res.next()){
                System.out.println(res.getInt(1)+" " +res.getString(2));
            }
            conn.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
