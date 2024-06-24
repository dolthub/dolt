import { getArgs } from "../helpers.js";

const { dbName } = getArgs();

export const databaseTests = [
  {
    q: `USE ::dbName`,
    p: { dbName: `${dbName}/main` },
    res: {
      fieldCount: 0,
      affectedRows: 0,
      insertId: 0,
      info: "",
      serverStatus: 2,
      warningStatus: 0,
      changedRows: 0,
    },
  },
  {
    q: `SHOW DATABASES`,
    res: [
      { Database: `${dbName}` },
      { Database: `${dbName}/main` },
      { Database: "information_schema" },
      { Database: "mysql" },
      { Database: "performance_schema" },
    ],
  },
  {
    q: `CREATE DATABASE ::dbName`,
    p: { dbName: "new_db" },
    res: {
      fieldCount: 0,
      affectedRows: 1,
      insertId: 0,
      info: "",
      serverStatus: 2,
      warningStatus: 0,
      changedRows: 0,
    },
  },
  {
    q: `SHOW DATABASES`,
    res: [
      { Database: `${dbName}` },
      { Database: `${dbName}/main` },
      { Database: "information_schema" },
      { Database: "mysql" },
      { Database: "new_db" },
      { Database: "performance_schema" },
    ],
  },
  {
    q: `SELECT dolt_version()`,
    res: [{ "dolt_version()": "0.0.0" }],
    matcher: (_, exp) => {
      return exp[0]["dolt_version()"].length > 0;
    },
  },
];
