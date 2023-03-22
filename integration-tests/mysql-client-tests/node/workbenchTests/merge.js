import { logsMatcher, mergeBaseMatcher } from "./matchers.js";

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
    q: `CALL DOLT_MERGE(:branchName, "--no-ff", "-m", :commitMsg)`,
    p: { branchName: "mybranch", commitMsg: "Merge mybranch into main" },
    res: [{ fast_forward: 1, conflicts: 0 }],
  },
  {
    q: `SELECT * FROM DOLT_LOG(:refName, '--parents') LIMIT :limit OFFSET :offset`,
    p: { refName: "main", limit: 10, offset: 0 },
    res: [
      {
        commit_hash: "",
        message: "Merge mybranch into main",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
        date: "",
        parents: ["", ""],
      },
      {
        commit_hash: "",
        message: "Create table test",
        committer: "Dolt",
        email: "dolt@dolthub.com",
        date: "",
        parents: [""],
      },
      {
        commit_hash: "",
        message: "Initialize data repository",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
        date: "",
        parents: [],
      },
    ],
    matcher: logsMatcher,
  },
];
