import { getArgs } from "../helpers.js";

const args = getArgs();

export const viewsTests = [
  {
    q: "CALL DOLT_CHECKOUT('-b', :branchName)",
    p: { branchName: "more-updates" },
    res: [{ status: 0, message: "Switched to branch 'more-updates'" }],
  },
  {
    q: "SELECT * FROM ::tableName ::col0 LIMIT :limit OFFSET :offset",
    p: { tableName: "dolt_schemas", col0: "id", limit: 10, offset: 0 },
    res: [],
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
      changedRows: 0,
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
        sql_mode:
          "NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
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
