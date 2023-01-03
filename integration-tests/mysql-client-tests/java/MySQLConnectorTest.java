import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
 
public class MySQLConnectorTest {
    public static void main(String[] args) {
        Connection conn = null;

	String user = args[0];
	String port = args[1];
	String db   = args[2];

        try {
            String url = "jdbc:mysql://127.0.0.1:" + port + "/" + db;
            
	    String password = "";
 
            conn = DriverManager.getConnection(url, user, password);
	    
	    Statement st = conn.createStatement();

	    String[] queries = {
		 "create table test (pk int, `value` int, primary key(pk))",
		 "describe test",
		 "select * from test",
		 "insert into test (pk, `value`) values (0,0)",
		 "select * from test",
		 "call dolt_add('-A')",
		 "call dolt_commit('-m', 'my commit')",
		 "select COUNT(*) FROM dolt_log",
		 "call dolt_checkout('-b', 'mybranch')",
		 "insert into test (pk, `value`) values (1,1)",
		 "call dolt_commit('-a', '-m', 'my commit2')",
		 "call dolt_checkout('main')",
		 "call dolt_merge('mybranch')",
		 "select COUNT(*) FROM dolt_log",
	    };

	    // Only test the first row, column pair for now
	    String[] results = {
		"0",
		"pk",
		null,
		"1",
		"0",
		"0",
		"0",
		"2",
		"0",
		"1",
		"1",
		"0",
		"1",
		"3"
	    };

	    for (int i = 0; i < queries.length; i++) { 
		String query    = queries[i];
		String expected = results[i];
		if ( st.execute(query) ) {
		    ResultSet rs = st.getResultSet();
		    if (rs.next()) {
			String result = rs.getString(1);
			if (!expected.equals(result) && !(query.contains("dolt_commit"))) {
			    System.out.println("Query: \n" + query);
			    System.out.println("Expected:\n" + expected);
			    System.out.println("Result:\n" + result);
			    System.exit(1);
			}
		    }
		} else {
		    String result = Integer.toString(st.getUpdateCount());
		    if ( !expected.equals(result) ) {
			System.out.println("Query: \n" + query);
			System.out.println("Expected:\n" + expected);
			System.out.println("Rows Updated:\n" + result);
			System.exit(1);
		    }
		}
	    }
	    
	    System.exit(0);

        } catch (SQLException ex) {
            System.out.println("An error occurred.");
            ex.printStackTrace();
	    System.exit(1);
        }
    }
}
