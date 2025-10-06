import java.sql.*;

// https://github.com/dolthub/dolt/issues/9890
public class MySQLConnectorTest_Collation {

    public static void main(String[] args) {
        String user = args[0];
        String port = args[1];
        String db   = args[2];

        try {
            String url = "jdbc:mysql://127.0.0.1:" + port + "/" + db;
            Connection conn = DriverManager.getConnection(url, user, "");
			var result = conn.getMetaData().getColumns(null, null, null, null);
        } catch (SQLException ex) {
            System.out.println("An error occurred.");
            ex.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
