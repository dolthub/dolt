import { getArgs } from "../helpers.js";

const { dbName } = getArgs();

export const tableTests = [
  {
    q: "INSERT INTO test VALUES (0, 0), (1, 1), (2,2)",
    res: {
      fieldCount: 0,
      affectedRows: 3,
      insertId: 0,
      info: "",
      serverStatus: 2,
      warningStatus: 0,
      changedRows: 0,
    },
  },
  {
    q: `CREATE UNIQUE INDEX test_idx ON test (pk, value)`,
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
    q: `SHOW CREATE TABLE ::tableName AS OF :refName`,
    p: { tableName: "test", refName: "main" },
    res: [
      // Should not show new index in working set
      {
        Table: "test",
        "Create Table":
          "CREATE TABLE `test` (\n" +
          "  `pk` int NOT NULL,\n" +
          "  `value` int,\n" +
          "  PRIMARY KEY (`pk`)\n" +
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
      },
    ],
  },
  {
    q: `CALL DOLT_COMMIT("-A", "-m", :commitMsg)`,
    p: { commitMsg: "Add some rows and a column index" },
    res: [{ hash: "" }],
  },
  {
    q: `DESCRIBE ::tableName AS OF :refName`,
    p: { tableName: "test", refName: "main" },
    res: [
      {
        Field: "pk",
        Type: "int",
        Null: "NO",
        Key: "PRI",
        Default: null,
        Extra: "",
      },
      {
        Field: "value",
        Type: "int",
        Null: "YES",
        Key: "",
        Default: null,
        Extra: "",
      },
    ],
  },
  {
    q: `SELECT 
    table_name, index_name, comment, non_unique, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS COLUMNS 
  FROM information_schema.statistics 
  WHERE table_schema=:tableSchema AND table_name=:tableName AND index_name!="PRIMARY" 
  GROUP BY index_name;`,
    p: { tableSchema: `${dbName}/main`, tableName: "test" },
    res: [
      {
        TABLE_NAME: "test",
        INDEX_NAME: "test_idx",
        COMMENT: "",
        NON_UNIQUE: 0,
        COLUMNS: "pk,value",
      },
    ],
  },
  {
    q: "CREATE TABLE test_info (id int, info varchar(255), test_pk int, primary key(id), foreign key (test_pk) references test(pk))",
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
    q: "INSERT INTO test_info VALUES (1, 'info about test pk 0', 0)",
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
    q: `CALL DOLT_COMMIT("-A", "-m", :commitMsg)`,
    p: { commitMsg: "Add test_info with foreign key" },
    res: [{ hash: "" }],
  },
  {
    q: `SHOW FULL TABLES AS OF :refName WHERE table_type = 'BASE TABLE'`,
    p: { refName: "main" },
    res: [
      { [`Tables_in_${dbName}/main`]: "test", Table_type: "BASE TABLE" },
      { [`Tables_in_${dbName}/main`]: "test_info", Table_type: "BASE TABLE" },
    ],
  },
  {
    q: `SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE table_name=:tableName AND table_schema=:tableSchema AND referenced_table_schema IS NOT NULL`,
    p: { tableName: "test_info", tableSchema: `${dbName}/main` },
    res: [
      {
        CONSTRAINT_CATALOG: "def",
        CONSTRAINT_SCHEMA: `${dbName}/main`,
        CONSTRAINT_NAME: "test_info_ibfk_1",
        TABLE_CATALOG: "def",
        TABLE_SCHEMA: `${dbName}/main`,
        TABLE_NAME: "test_info",
        COLUMN_NAME: "test_pk",
        ORDINAL_POSITION: 1,
        POSITION_IN_UNIQUE_CONSTRAINT: 1,
        REFERENCED_TABLE_SCHEMA: dbName,
        REFERENCED_TABLE_NAME: "test",
        REFERENCED_COLUMN_NAME: "pk",
      },
    ],
  },
  {
    q: `SELECT * FROM ::tableName ORDER BY ::col0 LIMIT :limit OFFSET :offset`,
    p: { tableName: "test_info", col0: "id", limit: 10, offset: 0 },
    res: [{ id: 1, info: "info about test pk 0", test_pk: 0 }],
  },
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

  // Load file
  {
    q: `SELECT * FROM dolt_status`,
    res: [],
  },
  {
    q: "SET GLOBAL local_infile=ON;",
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
    q: `LOAD DATA LOCAL INFILE '../testdata/update_test_info.csv'
    INTO TABLE \`test_info\` 
    FIELDS TERMINATED BY ',' ENCLOSED BY '' 
    LINES TERMINATED BY '\n' 
    IGNORE 1 ROWS;`,
    file: "update_test_info.csv",
    res: {
      fieldCount: 0,
      affectedRows: 3,
      insertId: 0,
      info: "",
      serverStatus: 2,
      warningStatus: 0,
      changedRows: 0,
    },
  },
  {
    q: "SELECT * FROM dolt_diff_stat(:fromRefName, :toRefName)",
    p: { fromRefName: "HEAD", toRefName: "WORKING" },
    res: [
      {
        table_name: "test_info",
        rows_unmodified: 1,
        rows_added: 3,
        rows_deleted: 0,
        rows_modified: 0,
        cells_added: 9,
        cells_deleted: 0,
        cells_modified: 0,
        old_row_count: 1,
        new_row_count: 4,
        old_cell_count: 3,
        new_cell_count: 12,
      },
    ],
  },
  {
    q: `LOAD DATA LOCAL INFILE '../testdata/replace_test_info.psv'
    REPLACE INTO TABLE \`test_info\` 
    FIELDS TERMINATED BY '|' ENCLOSED BY '' 
    LINES TERMINATED BY '\n' 
    IGNORE 1 ROWS;`,
    file: "replace_test_info.psv",
    res: {
      fieldCount: 0,
      affectedRows: 6,
      insertId: 0,
      info: "",
      serverStatus: 2,
      warningStatus: 0,
      changedRows: 0,
    },
  },
  {
    q: "SELECT * FROM dolt_diff_stat(:fromRefName, :toRefName)",
    p: { fromRefName: "HEAD", toRefName: "WORKING" },
    res: [
      {
        table_name: "test_info",
        rows_unmodified: 0,
        rows_added: 3,
        rows_deleted: 0,
        rows_modified: 1,
        cells_added: 9,
        cells_deleted: 0,
        cells_modified: 1,
        old_row_count: 1,
        new_row_count: 4,
        old_cell_count: 3,
        new_cell_count: 12,
      },
    ],
  },

  // Add and revert load data changes
  {
    q: `SELECT * FROM dolt_status`,
    res: [{ table_name: "test_info", staged: 0, status: "modified" }],
  },
  {
    q: "CALL DOLT_ADD('.')",
    res: [{ status: 0 }],
  },
  {
    q: `SELECT * FROM dolt_status`,
    res: [{ table_name: "test_info", staged: 1, status: "modified" }],
  },
  {
    q: "CALL DOLT_RESET('test_info')",
    res: [{ status: 0 }],
  },
  {
    q: `SELECT * FROM dolt_status`,
    res: [{ table_name: "test_info", staged: 0, status: "modified" }],
  },
  {
    q: "CALL DOLT_CHECKOUT('test_info')",
    res: [{ status: 0, message: "" }],
  },
  {
    q: `SELECT * FROM dolt_status`,
    res: [],
  },
];
