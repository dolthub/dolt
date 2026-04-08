import { logsMatcher, mergeBaseMatcher, mergeMatcher } from "./matchers.js";

export const mergeTests = [
  {
    q: `SELECT DOLT_MERGE_BASE(:fromBranchName, :toBranchName)`,
    p: { fromBranchName: "mybranch", toBranchName: "main" },
    res: [{ "DOLT_MERGE_BASE('mybranch', 'main')": "" }],
    matcher: mergeBaseMatcher,
  },
  {
    q: `SELECT * FROM dolt_status`,
    res: [],
  },
  {
    q: `CALL DOLT_MERGE(?, '--no-ff', '-m', ?)`,
    values: ["mybranch", "Merge mybranch into main"],
    res: [{ hash: "", fast_forward: 0, conflicts: 0, message: "merge successful" }],
    matcher: mergeMatcher,
  },
  {
    q: `SELECT * FROM DOLT_LOG(:refName, '--parents') LIMIT :limit OFFSET :offset`,
    p: { refName: "main", limit: 10, offset: 0 },
    res: [
      {
        commit_hash: "",
        message: "Merge mybranch into main",
        committer: "dolt",
        email: "dolt@%",
        date: "",
        commit_order: 3,
        author: "dolt",
        author_email: "dolt@%",
        author_date: "",
        parents: ["", ""],
      },
      {
        commit_hash: "",
        message: "Create table test",
        committer: "dolt",
        email: "dolt@%",
        date: "",
        commit_order: 2,
        author: "Dolt",
        author_email: "dolt@dolthub.com",
        author_date: "",
        parents: [""],
      },
      {
        commit_hash: "",
        message: "Initialize data repository",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
        date: "",
        commit_order: 1,
        author: "mysql-test-runner",
        author_email: "mysql-test-runner@liquidata.co",
        author_date: "",
        parents: [],
      },
    ],
    matcher: logsMatcher,
  },
];
