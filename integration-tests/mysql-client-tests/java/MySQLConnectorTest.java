import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class MySQLConnectorTest {

    // test queries to be run against Dolt
    private static final String[] queries = {
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

    // We currently only test a single field value in the first row
    private static final String[] expectedResults = {
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
            "",
            "3"
    };

    // fieldAccessors are the value used to access a field in a row in a result set. Currently, only
    // String (i.e column name) and Integer (i.e. field position) values are supported.
    private static final Object[] fieldAccessors = {
            1,
            1,
            "pk",
            1,
            "test.pk",
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            "COUNT(*)",
    };

    public static void main(String[] args) {
        testStatements(args);
        testServerSideCursors(args);
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
            String url = "jdbc:mysql://127.0.0.1:" + port + "/" + db +
                         "?useServerPrepStmts=true&useCursorFetch=true";
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
            String url = "jdbc:mysql://127.0.0.1:" + port + "/" + db;
            String password = "";

            conn = DriverManager.getConnection(url, user, password);
            Statement st = conn.createStatement();

            for (int i = 0; i < queries.length; i++) {
                String query    = queries[i];
                String expected = expectedResults[i];
                if ( st.execute(query) ) {
                    ResultSet rs = st.getResultSet();
                    if (rs.next()) {
                        String result = "";
                        Object fieldAccessor = fieldAccessors[i];
                        if (fieldAccessor instanceof String) {
                            result = rs.getString((String)fieldAccessor);
                        } else if (fieldAccessor instanceof Integer) {
                            result = rs.getString((Integer)fieldAccessor);
                        } else {
                            System.out.println("Unsupported field accessor value: " + fieldAccessor);
                            System.exit(1);
                        }

                        if (!expected.equals(result) && !(query.contains("dolt_commit")) && !(query.contains("dolt_merge"))) {
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
        } catch (SQLException ex) {
            System.out.println("An error occurred.");
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
