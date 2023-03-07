import { mysql as escapeQueryWithParameters } from "yesql";

const args = process.argv.slice(2);
const user = args[0];
const port = args[1];
const dbName = args[2];

export function getArgs() {
  return { user, port, dbName };
}

export function getConfig() {
  const { user, port, dbName } = getArgs();
  return {
    host: "127.0.0.1",
    port: port,
    user: user,
    database: dbName,
  };
}

export function assertQueryResult(q, resultStr, expected, rows, matcher) {
  if (matcher) {
    return matcher(rows, expected);
  }
  if (q.toLowerCase().includes("dolt_commit")) {
    return rows.length === 1 && rows[0].hash.length === 32;
  }
  return resultStr === JSON.stringify(expected);
}

export function getQueryWithEscapedParameters(q, parameters) {
  return escapeQueryWithParameters(q)(parameters || {});
}
