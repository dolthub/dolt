import mariadb from "mariadb";
import { getArgs } from "./helpers.js";

const tests = [
  { q: "create table test (pk int, `value` int, primary key(pk))", isQuery: false },
  { q: "describe test", isQuery: true },
  { q: "select * from test", isQuery: true },
  { q: "insert into test (pk, `value`) values (0,0)", isQuery: false },
  { q: "select * from test", isQuery: true },
  { q: "call dolt_add('-A');", isQuery: true },
  { q: "call dolt_commit('-m', 'my commit')", isQuery: true },
  { q: "select COUNT(*) FROM dolt_log", isQuery: true },
  { q: "call dolt_checkout('-b', 'mybranch')", isQuery: true },
  { q: "insert into test (pk, `value`) values (1,1)", isQuery: false },
  { q: "call dolt_commit('-a', '-m', 'my commit2')", isQuery: true },
  { q: "call dolt_checkout('main')", isQuery: true },
  { q: "call dolt_merge('mybranch')", isQuery: true },
  { q: "select COUNT(*) FROM dolt_log", isQuery: true },
];

async function main() {
  const { user, port, dbName } = getArgs();

  let conn;
  try {
    // Create connection pool
    const pool = mariadb.createPool({
      host: "127.0.0.1",
      port: port,
      user: user,
      database: dbName,
      connectionLimit: 5,
    });

    // Get a connection from the pool
    conn = await pool.getConnection();
    console.log("Connected to MariaDB!");

    // Run all tests
    for (const test of tests) {
      console.log(`Executing: ${test.q}`);
      
      if (test.isQuery) {
        // Execute query and fetch results
        const rows = await conn.query(test.q);
        console.log(`  → ${rows.length} row(s) returned`);
      } else {
        // Execute update/insert
        const result = await conn.query(test.q);
        console.log(`  → ${result.affectedRows} row(s) affected`);
      }
    }

    console.log("\nAll MariaDB connector tests passed!");
    
    // Close connection and pool
    if (conn) conn.release();
    await pool.end();
    
    process.exit(0);
  } catch (err) {
    console.error("MariaDB connector test failed:");
    console.error(err);
    if (conn) conn.release();
    process.exit(1);
  }
}

main();

