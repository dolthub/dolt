import { logsMatcher } from "./matchers.js";

export const logTests = [
  {
    q: `SELECT * FROM DOLT_LOG(:refName, '--parents') LIMIT :limit OFFSET :offset`,
    p: { refName: "main", limit: 10, offset: 0 },
    res: [
      {
        commit_hash: "",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
        date: "",
        message: "Initialize data repository",
        parents: [],
      },
    ],
    matcher: logsMatcher,
  },
  {
    q: `SELECT * FROM dolt_log AS OF :refName`,
    p: { refName: "mybranch" },
    res: [
      {
        commit_hash: "",
        committer: "Dolt",
        email: "dolt@dolthub.com",
        date: "",
        message: "Create table test",
      },
      {
        commit_hash: "",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
        date: "",
        message: "Initialize data repository",
      },
    ],
    matcher: logsMatcher,
  },
  {
    q: `SELECT * FROM DOLT_LOG(:refRange, '--parents')`,
    p: { refRange: "main..mybranch" },
    res: [
      {
        commit_hash: "",
        committer: "Dolt",
        email: "dolt@dolthub.com",
        date: "",
        message: "Create table test",
        parents: [""],
      },
    ],
    matcher: logsMatcher,
  },
];
