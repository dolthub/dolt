import { Database } from "./database.js";
import { getConfig } from "./helpers.js";

const tests = [
  {
    q: "create table test (pk int, `value` int, primary key(pk))",
    res: {
      fieldCount: 0,
      affectedRows: 0,
      insertId: 0,
      serverStatus: 2,
      warningCount: 0,
      message: "",
      protocol41: true,
      changedRows: 0,
    },
  },
  {
    q: "describe test",
    res: [
      {
        Field: "pk",
        Type: "int",
        Null: "NO",
        Key: "PRI",
        Default: "NULL",
        Extra: "",
      },
      {
        Field: "value",
        Type: "int",
        Null: "YES",
        Key: "",
        Default: "NULL",
        Extra: "",
      },
    ],
  },
  { q: "select * from test", res: [] },
  {
    q: "insert into test (pk, `value`) values (0,0)",
    res: {
      fieldCount: 0,
      affectedRows: 1,
      insertId: 0,
      serverStatus: 2,
      warningCount: 0,
      message: "",
      protocol41: true,
      changedRows: 0,
    },
  },
  { q: "select * from test", res: [{ pk: 0, value: 0 }] },
  { q: "call dolt_add('-A');", res: [{ status: 0 }] },
  { q: "call dolt_commit('-m', 'my commit')", res: [] },
  { q: "select COUNT(*) FROM dolt_log", res: [{ "COUNT(*)": 2 }] },
  { q: "call dolt_checkout('-b', 'mybranch')", res: [{ status: 0 }] },
  {
    q: "insert into test (pk, `value`) values (1,1)",
    res: {
      fieldCount: 0,
      affectedRows: 1,
      insertId: 0,
      serverStatus: 2,
      warningCount: 0,
      message: "",
      protocol41: true,
      changedRows: 0,
    },
  },
  { q: "call dolt_commit('-a', '-m', 'my commit2')", res: [] },
  { q: "call dolt_checkout('main')", res: [{ status: 0 }] },
  {
    q: "call dolt_merge('mybranch')",
    res: [{ fast_forward: 1, conflicts: 0 }],
  },
  { q: "select COUNT(*) FROM dolt_log", res: [{ "COUNT(*)": 3 }] },
];

async function main() {
  const database = new Database(getConfig());

  await Promise.all(
    tests.map((test) => {
      const expected = test.res;
      return database
        .query(test.q)
        .then((rows) => {
          const resultStr = JSON.stringify(rows);
          const result = JSON.parse(resultStr);
          if (
            resultStr !== JSON.stringify(expected) &&
            test.q.includes("dolt_commit") &&
            !(rows.length === 1 && rows[0].hash.length > 0)
          ) {
            console.log("Query:", test.q);
            console.log("Results:", result);
            console.log("Expected:", expected);
            throw new Error("Query failed");
          } else {
            console.log("Query succeeded:", test.q);
          }
        })
        .catch((err) => {
          console.error(err);
          process.exit(1);
        });
    })
  );

  database.close();
  process.exit(0);
}

main();
