import { branchesMatcher } from "./matchers.js";
import { getArgs } from "../helpers.js";

const args = getArgs();

export const branchTests = [
  {
    q: `CALL DOLT_BRANCH(:newBranchName, :fromRefName)`,
    p: { newBranchName: "mybranch", fromRefName: "main" },
    res: [{ status: 0 }],
  },
  {
    q: `CALL DOLT_CLEAN('mysqldump_table', 'warehouse')`,
    res: [{ status: 0 }],
  },
  {
    q: `USE ::dbName`,
    p: { dbName: `${args.dbName}/mybranch` },
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
    q: "create table test (pk int, `value` int, primary key(pk))",
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
    q: `CALL DOLT_COMMIT("-A", "-m", :commitMsg, "--author", :authorName)`,
    p: {
      commitMsg: "Create table test",
      authorName: "Dolt <dolt@dolthub.com>",
    },
    res: [{ hash: "" }],
  },
  {
    q: `SELECT * FROM dolt_branches LIMIT 200`,
    res: [
      {
        name: "main",
        hash: "",
        latest_committer: "mysql-test-runner",
        latest_committer_email: "mysql-test-runner@liquidata.co",
        latest_commit_date: "",
        latest_commit_message: "Initialize data repository",
        remote: "",
        branch: "",
        dirty: 0,
      },
      {
        name: "mybranch",
        hash: "",
        latest_committer: "Dolt",
        latest_committer_email: "dolt@dolthub.com",
        latest_commit_date: "",
        latest_commit_message: "Create table test",
        remote: "",
        branch: "",
        dirty: 0,
      },
    ],
    matcher: branchesMatcher,
  },
  {
    q: `CALL DOLT_CHECKOUT("-b", :branchName)`,
    p: { branchName: "branch-to-delete" },
    res: [{ status: 0, message: "Switched to branch 'branch-to-delete'" }],
  },
  {
    q: `SELECT COUNT(*) FROM dolt_branches LIMIT 200`,
    res: [{ ["COUNT(*)"]: 3 }],
  },
  {
    q: `USE ::dbName`,
    p: { dbName: `${args.dbName}/main` },
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
    q: `CALL DOLT_BRANCH("-D", :branchName)`,
    p: { branchName: "branch-to-delete" },
    res: [{ status: 0 }],
  },
  {
    q: `SELECT COUNT(*) FROM dolt_branches LIMIT 200`,
    res: [{ ["COUNT(*)"]: 2 }],
  },
];
