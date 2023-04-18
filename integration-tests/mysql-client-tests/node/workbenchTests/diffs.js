import { diffRowsMatcher, patchRowsMatcher } from "./matchers.js";

export const diffTests = [
  {
    q: "UPDATE test SET value=1 WHERE pk=0",
    res: {
      fieldCount: 0,
      affectedRows: 1,
      insertId: 0,
      info: "#Rows matched: 1  Changed: 1  Warnings: 0",
      serverStatus: 2,
      warningStatus: 0,
      changedRows: 1,
    },
  },
  {
    q: "DROP TABLE test_info",
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
    q: "SELECT * FROM dolt_diff_summary(:fromRefName, :toRefName)",
    p: { fromRefName: "HEAD", toRefName: "WORKING" },
    res: [
      {
        from_table_name: "test_info",
        to_table_name: "",
        diff_type: "dropped",
        data_change: 1,
        schema_change: 1,
      },
      {
        from_table_name: "",
        to_table_name: "dolt_schemas",
        diff_type: "added",
        data_change: 1,
        schema_change: 1,
      },
      {
        from_table_name: "test",
        to_table_name: "test",
        diff_type: "modified",
        data_change: 1,
        schema_change: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM dolt_diff_summary(:fromRefName, :toRefName, :tableName)",
    p: { fromRefName: "HEAD", toRefName: "WORKING", tableName: "test" },
    res: [
      {
        from_table_name: "test",
        to_table_name: "test",
        diff_type: "modified",
        data_change: 1,
        schema_change: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM dolt_diff_stat(:fromRefName, :toRefName)",
    p: { fromRefName: "HEAD", toRefName: "WORKING" },
    res: [
      {
        table_name: "dolt_schemas",
        rows_unmodified: 0,
        rows_added: 1,
        rows_deleted: 0,
        rows_modified: 0,
        cells_added: 4,
        cells_deleted: 0,
        cells_modified: 0,
        old_row_count: 0,
        new_row_count: 1,
        old_cell_count: 0,
        new_cell_count: 4,
      },
      {
        table_name: "test",
        rows_unmodified: 2,
        rows_added: 0,
        rows_deleted: 0,
        rows_modified: 1,
        cells_added: 0,
        cells_deleted: 0,
        cells_modified: 1,
        old_row_count: 3,
        new_row_count: 3,
        old_cell_count: 6,
        new_cell_count: 6,
      },
      {
        table_name: "test_info",
        rows_unmodified: 0,
        rows_added: 0,
        rows_deleted: 1,
        rows_modified: 0,
        cells_added: 0,
        cells_deleted: 3,
        cells_modified: 0,
        old_row_count: 1,
        new_row_count: 0,
        old_cell_count: 3,
        new_cell_count: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM dolt_diff_stat(:fromRefName, :toRefName, :tableName)",
    p: { fromRefName: "HEAD", toRefName: "WORKING", tableName: "test_info" },
    res: [
      {
        table_name: "test_info",
        rows_unmodified: 0,
        rows_added: 0,
        rows_deleted: 1,
        rows_modified: 0,
        cells_added: 0,
        cells_deleted: 3,
        cells_modified: 0,
        old_row_count: 1,
        new_row_count: 0,
        old_cell_count: 3,
        new_cell_count: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM DOLT_DIFF(:fromCommitId, :toCommitId, :tableName) ORDER BY to_pk ASC, from_pk ASC LIMIT :limit OFFSET :offset",
    p: {
      fromCommitId: "HEAD",
      toCommitId: "WORKING",
      tableName: "test",
      limit: 10,
      offset: 0,
    },
    res: [
      {
        to_pk: 0,
        to_value: 1,
        to_commit: "WORKING",
        to_commit_date: "2023-03-09T07:44:47.670Z",
        from_pk: 0,
        from_value: 0,
        from_commit: "HEAD",
        from_commit_date: "2023-03-09T07:44:47.488Z",
        diff_type: "modified",
      },
    ],
    matcher: diffRowsMatcher,
  },
  {
    q: "SELECT * FROM DOLT_DIFF(:fromCommitId, :toCommitId, :tableName) ORDER BY to_id ASC, from_id ASC LIMIT :limit OFFSET :offset",
    p: {
      fromCommitId: "HEAD",
      toCommitId: "WORKING",
      tableName: "test_info",
      limit: 10,
      offset: 0,
    },
    res: [
      {
        to_id: null,
        to_info: null,
        to_test_pk: null,
        to_commit: "WORKING",
        to_commit_date: "2023-03-09T07:53:48.614Z",
        from_id: 1,
        from_info: "info about test pk 0",
        from_test_pk: 0,
        from_commit: "HEAD",
        from_commit_date: "2023-03-09T07:53:48.284Z",
        diff_type: "removed",
      },
    ],
    matcher: diffRowsMatcher,
  },
  {
    q: "SELECT * FROM DOLT_DIFF(:fromCommitId, :toCommitId, :tableName) ORDER BY to_name ASC, from_name ASC LIMIT :limit OFFSET :offset",
    p: {
      fromCommitId: "HEAD",
      toCommitId: "WORKING",
      tableName: "dolt_schemas",
      limit: 10,
      offset: 0,
    },
    res: [
      {
        to_type: "view",
        to_name: "myview",
        to_fragment: "CREATE VIEW `myview` AS SELECT * FROM test",
        to_extra: { CreatedAt: 0 },
        to_commit: "WORKING",
        to_commit_date: "2023-03-09T07:56:29.035Z",
        from_type: null,
        from_name: null,
        from_fragment: null,
        from_extra: null,
        from_commit: "HEAD",
        from_commit_date: "2023-03-09T07:56:28.841Z",
        diff_type: "added",
      },
    ],
    matcher: diffRowsMatcher,
  },
  {
    q: "SELECT * FROM DOLT_PATCH(:fromRefName, :toRefName) WHERE diff_type = 'schema'",
    p: { fromRefName: "HEAD", toRefName: "WORKING" },
    res: [
      {
        statement_order: 1,
        from_commit_hash: "",
        to_commit_hash: "WORKING",
        table_name: "test_info",
        diff_type: "schema",
        statement: "DROP TABLE `test_info`;",
      },
      {
        statement_order: 2,
        from_commit_hash: "",
        to_commit_hash: "WORKING",
        table_name: "dolt_schemas",
        diff_type: "schema",
        statement:
          "CREATE TABLE `dolt_schemas` (\n" +
          "  `type` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL,\n" +
          "  `name` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL,\n" +
          "  `fragment` longtext,\n" +
          "  `extra` json,\n" +
          "  PRIMARY KEY (`type`,`name`)\n" +
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
      },
    ],
    matcher: patchRowsMatcher,
  },
  {
    q: "SELECT * FROM DOLT_PATCH(:fromRefName, :toRefName, :tableName) WHERE diff_type = 'schema'",
    p: { fromRefName: "HEAD", toRefName: "WORKING", tableName: "test_info" },
    res: [
      {
        statement_order: 1,
        from_commit_hash: "",
        to_commit_hash: "WORKING",
        table_name: "test_info",
        diff_type: "schema",
        statement: "DROP TABLE `test_info`;",
      },
    ],
    matcher: patchRowsMatcher,
  },
  {
    q: `CALL DOLT_COMMIT("-A", "-m", :commitMsg)`,
    p: { commitMsg: "Make some changes on branch" },
    res: [{ hash: "" }],
  },

  // Three dot
  {
    q: "SELECT * FROM dolt_diff_summary(:refRange)",
    p: { refRange: "main...HEAD" },
    res: [
      {
        from_table_name: "test_info",
        to_table_name: "",
        diff_type: "dropped",
        data_change: 1,
        schema_change: 1,
      },
      {
        from_table_name: "",
        to_table_name: "dolt_schemas",
        diff_type: "added",
        data_change: 1,
        schema_change: 1,
      },
      {
        from_table_name: "test",
        to_table_name: "test",
        diff_type: "modified",
        data_change: 1,
        schema_change: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM dolt_diff_summary(:refRange, :tableName)",
    p: { refRange: "main...HEAD", tableName: "test" },
    res: [
      {
        from_table_name: "test",
        to_table_name: "test",
        diff_type: "modified",
        data_change: 1,
        schema_change: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM dolt_diff_stat(:refRange)",
    p: { refRange: "main...HEAD" },
    res: [
      {
        table_name: "dolt_schemas",
        rows_unmodified: 0,
        rows_added: 1,
        rows_deleted: 0,
        rows_modified: 0,
        cells_added: 4,
        cells_deleted: 0,
        cells_modified: 0,
        old_row_count: 0,
        new_row_count: 1,
        old_cell_count: 0,
        new_cell_count: 4,
      },
      {
        table_name: "test",
        rows_unmodified: 2,
        rows_added: 0,
        rows_deleted: 0,
        rows_modified: 1,
        cells_added: 0,
        cells_deleted: 0,
        cells_modified: 1,
        old_row_count: 3,
        new_row_count: 3,
        old_cell_count: 6,
        new_cell_count: 6,
      },
      {
        table_name: "test_info",
        rows_unmodified: 0,
        rows_added: 0,
        rows_deleted: 1,
        rows_modified: 0,
        cells_added: 0,
        cells_deleted: 3,
        cells_modified: 0,
        old_row_count: 1,
        new_row_count: 0,
        old_cell_count: 3,
        new_cell_count: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM dolt_diff_stat(:refRange, :tableName)",
    p: { refRange: "main...HEAD", tableName: "test_info" },
    res: [
      {
        table_name: "test_info",
        rows_unmodified: 0,
        rows_added: 0,
        rows_deleted: 1,
        rows_modified: 0,
        cells_added: 0,
        cells_deleted: 3,
        cells_modified: 0,
        old_row_count: 1,
        new_row_count: 0,
        old_cell_count: 3,
        new_cell_count: 0,
      },
    ],
  },
  {
    q: "SELECT * FROM DOLT_PATCH(:refRange) WHERE diff_type = 'schema'",
    p: { refRange: "main...HEAD" },
    res: [
      {
        statement_order: 1,
        from_commit_hash: "",
        to_commit_hash: "",
        table_name: "test_info",
        diff_type: "schema",
        statement: "DROP TABLE `test_info`;",
      },
      {
        statement_order: 2,
        from_commit_hash: "",
        to_commit_hash: "",
        table_name: "dolt_schemas",
        diff_type: "schema",
        statement:
          "CREATE TABLE `dolt_schemas` (\n" +
          "  `type` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL,\n" +
          "  `name` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL,\n" +
          "  `fragment` longtext,\n" +
          "  `extra` json,\n" +
          "  PRIMARY KEY (`type`,`name`)\n" +
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
      },
    ],
    matcher: patchRowsMatcher,
  },
  {
    q: "SELECT * FROM DOLT_PATCH(:refRange, :tableName) WHERE diff_type = 'schema'",
    p: { refRange: "main...HEAD", tableName: "test_info" },
    res: [
      {
        statement_order: 1,
        from_commit_hash: "",
        to_commit_hash: "",
        table_name: "test_info",
        diff_type: "schema",
        statement: "DROP TABLE `test_info`;",
      },
    ],
    matcher: patchRowsMatcher,
  },
];
