import { branchTests } from "./branches.js";
import { databaseTests } from "./databases.js";
import { logTests } from "./logs.js";
import { mergeTests } from "./merge.js";
import { tableTests } from "./table.js";
import {
  assertQueryResult,
  getQueryWithEscapedParameters,
} from "../helpers.js";

export default async function runWorkbenchTests(database) {
  await runTests(database, databaseTests);
  await runTests(database, branchTests);
  await runTests(database, logTests);
  await runTests(database, mergeTests);
  await runTests(database, tableTests);
  // TODO:
  // Workspaces
  // Diffs
  // Docs
  // Views
  // Tags
}

async function runTests(database, tests) {
  await Promise.all(
    tests.map((test) => {
      const expected = test.res;
      const { sql, values } = getQueryWithEscapedParameters(test.q, test.p);
      return database
        .query(sql, values)
        .then((rows) => {
          const resultStr = JSON.stringify(rows);
          const result = JSON.parse(resultStr);
          if (
            !assertQueryResult(test.q, resultStr, expected, rows, test.matcher)
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
}
