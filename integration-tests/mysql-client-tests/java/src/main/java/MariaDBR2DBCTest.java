import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class MariaDBR2DBCTest {

    // TestCase represents a single query test case
    static class TestCase {
        public String query;
        public boolean isUpdate;
        
        public TestCase(String query, boolean isUpdate) {
            this.query = query;
            this.isUpdate = isUpdate;
        }
    }

    // test queries to be run against Dolt
    private static final TestCase[] testCases = {
            new TestCase("create table test (pk int, `value` int, primary key(pk))", true),
            new TestCase("insert into test (pk, `value`) values (0,0)", true),
            new TestCase("select * from test", false),
            new TestCase("call dolt_add('-A')", false),
            new TestCase("call dolt_commit('-m', 'my commit')", false),
            new TestCase("select COUNT(*) FROM dolt_log", false),
            new TestCase("call dolt_checkout('-b', 'mybranch')", false),
            new TestCase("insert into test (pk, `value`) values (1,1)", true),
            new TestCase("call dolt_commit('-a', '-m', 'my commit2')", false),
            new TestCase("call dolt_checkout('main')", false),
            new TestCase("call dolt_merge('mybranch')", false),
            new TestCase("select COUNT(*) FROM dolt_log", false),
    };

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: MariaDBR2DBCTest <user> <port> <database>");
            System.exit(1);
        }

        String user = args[0];
        int port = Integer.parseInt(args[1]);
        String database = args[2];

        try {
            runTests(user, port, database);
            System.out.println("All R2DBC tests passed!");
        } catch (Exception e) {
            System.err.println("R2DBC test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runTests(String user, int port, String database) {
        // Create connection configuration
        MariadbConnectionConfiguration config = MariadbConnectionConfiguration.builder()
                .host("127.0.0.1")
                .port(port)
                .username(user)
                .password("")
                .database(database)
                .build();

        ConnectionFactory connectionFactory = new MariadbConnectionFactory(config);

        // Run tests reactively - block at the end for the test
        Mono.from(connectionFactory.create())
            .flatMapMany(connection -> 
                Flux.fromArray(testCases)
                    .concatMap(testCase -> executeTest(connection, testCase))
                    .doFinally(signalType -> 
                        Mono.from(connection.close()).subscribe()
                    )
            )
            .blockLast(Duration.ofSeconds(30));
    }

    private static Mono<Void> executeTest(Connection connection, TestCase testCase) {
        System.out.println("Executing: " + testCase.query);
        
        return Mono.from(connection.createStatement(testCase.query).execute())
            .flatMap(result -> {
                if (testCase.isUpdate) {
                    // For updates, just get the rows affected
                    return Mono.from(result.getRowsUpdated()).then();
                } else {
                    // For selects, consume all rows
                    return Flux.from(result.map((row, metadata) -> row))
                        .then();
                }
            })
            .onErrorResume(e -> {
                System.err.println("Error executing query: " + testCase.query);
                System.err.println("Error: " + e.getMessage());
                return Mono.error(e);
            });
    }
}

