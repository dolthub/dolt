import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Objects;

public class MariaDBConnectorTest {

    // TestCase represents a single query test case
    static class TestCase {
        public String query;
        public String expectedResult;
        public Object fieldAccessor;  // String (column name) or Integer (field position)
        
        public TestCase(String query, String expectedResult, Object fieldAccessor) {
            this.query = query;
            this.expectedResult = expectedResult;
            this.fieldAccessor = fieldAccessor;
        }
    }

    // test queries to be run against Dolt
    private static final TestCase[] testCases = {
            new TestCase("create table test (pk int, `value` int, primary key(pk))", "0", 1),
            new TestCase("describe test", "pk", 1),
            new TestCase("select * from test", null, "pk"),
            new TestCase("insert into test (pk, `value`) values (0,0)", "1", 1),
            new TestCase("select * from test", "0", "test.pk"),
            new TestCase("call dolt_add('-A')", "0", 1),
            new TestCase("call dolt_commit('-m', 'my commit')", "0", 1),
            new TestCase("select COUNT(*) FROM dolt_log", "2", 1),
            new TestCase("call dolt_checkout('-b', 'mybranch')", "0", 1),
            new TestCase("insert into test (pk, `value`) values (1,1)", "1", 1),
            new TestCase("call dolt_commit('-a', '-m', 'my commit2')", "1", 1),
            new TestCase("call dolt_checkout('main')", "0", 1),
            new TestCase("call dolt_merge('mybranch')", "", 1),
            new TestCase("select COUNT(*) FROM dolt_log", "3", "COUNT(*)"),
    };

    public static void main(String[] args) {
        testStatements(args);
        testServerSideCursors(args);
        testCollation(args);
        System.exit(0);
    }

    // testServerSideCursors does a simple smoke test with server-side cursors to make sure
    // results can be read. Note that we don't test results here; this is just a high level
    // smoke test that we can execute server-side cursors logic without the server erroring out.
    // This test was added for a regression where server-side cursor logic was getting
    // corrupted result set memory and sending invalid data to the client, which caused the
    // server to error out and crash the connection. If any errors are encountered, a stack trace
    // is printed and this function exits with a non-zero code.
    // For more details, see: https://github.com/dolthub/dolt/issues/9125
    private static void testServerSideCursors(String[] args) {
        String user = args[0];
        String port = args[1];
        String db   = args[2];

        try {
            // MariaDB JDBC URL format
            String url = "jdbc:mariadb://127.0.0.1:" + port + "/" + db +
                         "?useCursorFetch=true";
            Connection conn = DriverManager.getConnection(url, user, "");

            executePreparedQuery(conn, "SELECT 1;");
            executePreparedQuery(conn, "SELECT database();");
            executePreparedQuery(conn, "SHOW COLLATION;");
            executePreparedQuery(conn, "SHOW COLLATION;");
            executePreparedQuery(conn, "SHOW COLLATION;");
        } catch (SQLException ex) {
            System.out.println("An error occurred.");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    // executePreparedQuery executes the specified |query| using |conn| as a prepared statement,
    // and uses server-side cursor to fetch results. This method does not do any validation of
    // results from the query. It is simply a smoke test to ensure the connection doesn't crash.
    private static void executePreparedQuery(Connection conn, String query) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY);
        stmt.setFetchSize(25); // needed to ensure a server-side cursor is used

        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {}

        rs.close();
        stmt.close();
    }

    // testStatements executes the queries from |queries| and asserts their results from
    // |expectedResults|. If any errors are encountered, a stack trace is printed and this
    // function exits with a non-zero code.
    private static void testStatements(String[] args) {
        Connection conn = null;

        String user = args[0];
        String port = args[1];
        String db   = args[2];

        try {
            // MariaDB JDBC URL format
            String url = "jdbc:mariadb://127.0.0.1:" + port + "/" + db;
            String password = "";

            conn = DriverManager.getConnection(url, user, password);
            Statement st = conn.createStatement();

            for (TestCase test : testCases) {
                if ( st.execute(test.query) ) {
                    ResultSet rs = st.getResultSet();
                    if (rs.next()) {
                        String result = "";
                        if (test.fieldAccessor instanceof String) {
                            result = rs.getString((String)test.fieldAccessor);
                        } else if (test.fieldAccessor instanceof Integer) {
                            result = rs.getString((Integer)test.fieldAccessor);
                        } else {
                            System.out.println("Unsupported field accessor value: " + test.fieldAccessor);
                            System.exit(1);
                        }

                        if (!Objects.equals(test.expectedResult, result) && 
                            !(test.query.contains("dolt_commit")) && 
                            !(test.query.contains("dolt_merge"))) {
                            System.out.println("Query: \n" + test.query);
                            System.out.println("Expected:\n" + test.expectedResult);
                            System.out.println("Result:\n" + result);
                            System.exit(1);
                        }
                    }
                } else {
                    String result = Integer.toString(st.getUpdateCount());
                    if ( !Objects.equals(test.expectedResult, result) ) {
                        System.out.println("Query: \n" + test.query);
                        System.out.println("Expected:\n" + test.expectedResult);
                        System.out.println("Rows Updated:\n" + result);
                        System.exit(1);
                    }
                }
            }
        } catch (SQLException ex) {
            System.out.println("An error occurred.");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    // testCollation tests that metadata queries work properly with collations.
    // This is a regression test for https://github.com/dolthub/dolt/issues/9890
    private static void testCollation(String[] args) {
        String user = args[0];
        String port = args[1];
        String db   = args[2];

        try {
            // MariaDB JDBC URL format
            String url = "jdbc:mariadb://127.0.0.1:" + port + "/" + db;
            Connection conn = DriverManager.getConnection(url, user, "");
            
            // This should not throw an exception
            ResultSet result = conn.getMetaData().getColumns(null, null, null, null);
            
            // Close the result set
            if (result != null) {
                result.close();
            }
            conn.close();
        } catch (SQLException ex) {
            System.out.println("Collation test failed.");
            ex.printStackTrace();
            System.exit(1);
        }
    }
}

