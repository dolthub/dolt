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
    q: `CALL DOLT_MERGE(:branchName, "--no-ff", "-m", :commitMsg)`,
    p: { branchName: "mybranch", commitMsg: "Merge mybranch into main" },
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
        parents: ["", ""],
      },
      {
        commit_hash: "",
        message: "Create table test",
        committer: "Dolt",
        email: "dolt@dolthub.com",
        date: "",
        commit_order: 2,
        parents: [""],
      },
      {
        commit_hash: "",
        message: "Initialize data repository",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
        date: "",
        commit_order: 1,
        parents: [],
      },
    ],
    matcher: logsMatcher,
  },
];
