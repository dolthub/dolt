import fs from "fs";
import path from "path";
import { branchTests } from "./branches.js";
import { databaseTests } from "./databases.js";
import { logTests } from "./logs.js";
import { mergeTests } from "./merge.js";
import { tableTests } from "./table.js";
import {
  assertQueryResult,
  getQueryWithEscapedParameters,
} from "../helpers.js";
import { docsTests } from "./docs.js";
import { tagsTests } from "./tags.js";
import { viewsTests } from "./views.js";
import { diffTests } from "./diffs.js";

const args = process.argv.slice(2);
const testDataPath = args[3];

export default async function runWorkbenchTests(database) {
  await runTests(database, databaseTests);
  await runTests(database, branchTests);
  await runTests(database, logTests);
  await runTests(database, mergeTests);
  await runTests(database, tableTests);
  await runTests(database, docsTests);
  await runTests(database, tagsTests);
  await runTests(database, viewsTests);
  await runTests(database, diffTests);
}

async function runTests(database, tests) {
  await Promise.all(
    tests.map((test) => {
      const escaped = getQueryWithEscapedParameters(test.q, test.p);
      // Some stored-procedure option-flag queries can leave a trailing placeholder unbound after named expansion, so those tests provide explicit positional values.
      const sql = test.values ? test.q : escaped.sql;
      const values = test.values ?? escaped.values;

      if (test.file) {
        return database
          .query({
            sql,
            values,
            infileStreamFactory: () => fs.createReadStream(path.resolve(testDataPath, test.file)),
          })
          .then((rows) => {
            assertEqualRows(test, rows);
          })
          .catch((err) => {
            if (test.expectedErr) {
              if (err.message.includes(test.expectedErr)) {
                return;
              } else {
                console.log("Query error did not match expected:", test.q);
              }
            }
            console.error(err);
            process.exit(1);
          });
      }

      return database
        .query(sql, values)
        .then((rows) => {
          assertEqualRows(test, rows);
        })
        .catch((err) => {
          if (test.expectedErr) {
            if (err.message.includes(test.expectedErr)) {
              return;
            } else {
              console.log("Query error did not match expected:", test.q);
            }
          }
          console.error(err);
          process.exit(1);
        });
    })
  );
}

function assertEqualRows(test, rows) {
  const expected = test.res;
  const resultStr = JSON.stringify(rows);
  const result = JSON.parse(resultStr);
  if (!assertQueryResult(test.q, resultStr, expected, rows, test.matcher)) {
    console.log("Results:", result);
    console.log("Expected:", expected);
    throw new Error(`Query failed: ${test.q}`);
  }
}
