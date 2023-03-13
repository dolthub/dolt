import { getArgs } from "../helpers.js";

const args = getArgs();

export const viewsTests = [
  // Getting "fatal: 'head' is not a commit and a branch 'more-updates' cannot be created from it" error
  // TODO: Replace once this issue is resolved https://github.com/dolthub/dolt/issues/5526
  // {
  //   q: "CALL DOLT_CHECKOUT(:branchName)",
  //   p: { branchName: "main" },
  //   res: [{ status: 0 }],
  // },
  // {
  {
    q: "CALL DOLT_BRANCH(:branchName)",
    p: { branchName: "more-updates" },
    res: [{ status: 0 }],
  },
  {
    q: "CALL DOLT_CHECKOUT(:branchName)",
    p: { branchName: "more-updates" },
    res: [{ status: 0 }],
  },
  {
    q: "SELECT * FROM ::tableName ::col0 LIMIT :limit OFFSET :offset",
    p: { tableName: "dolt_schemas", col0: "id", limit: 10, offset: 0 },
    expectedErr: "table not found: dolt_schemas",
  },
  {
    q: "CREATE VIEW ::name AS SELECT * FROM test",
    p: { name: "myview" },
    res: {
      fieldCount: 0,
      affectedRows: 0,
      insertId: 0,
      info: "",
      serverStatus: 2,
      warningStatus: 0,
    },
  },
  {
    q: "SELECT * FROM ::tableName ::col0 LIMIT :limit OFFSET :offset",
    p: { tableName: "dolt_schemas", col0: "id", limit: 10, offset: 0 },
    res: [
      {
        type: "view",
        name: "myview",
        fragment: "CREATE VIEW `myview` AS SELECT * FROM test",
        extra: { CreatedAt: 0 },
      },
    ],
  },
  {
    // Excludes views
    q: "SHOW FULL TABLES WHERE table_type = 'BASE TABLE'",
    res: [
      {
        [`Tables_in_${args.dbName}`]: "test",
        Table_type: "BASE TABLE",
      },
      {
        [`Tables_in_${args.dbName}`]: "test_info",
        Table_type: "BASE TABLE",
      },
    ],
  },
  {
    // Includes views
    q: "SHOW FULL TABLES",
    res: [
      {
        [`Tables_in_${args.dbName}`]: "myview",
        Table_type: "VIEW",
      },
      {
        [`Tables_in_${args.dbName}`]: "test",
        Table_type: "BASE TABLE",
      },
      {
        [`Tables_in_${args.dbName}`]: "test_info",
        Table_type: "BASE TABLE",
      },
    ],
  },
  {
    q: "SHOW CREATE VIEW ::viewName",
    p: { viewName: "myview" },
    res: [
      {
        View: "myview",
        "Create View": "CREATE VIEW `myview` AS SELECT * FROM test",
        character_set_client: "utf8mb4",
        collation_connection: "utf8mb4_0900_bin",
      },
    ],
  },
];
