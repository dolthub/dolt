import { logsMatcher } from "./matchers.js";

export const logTests = [
  {
    q: `SELECT * FROM DOLT_LOG(:refName, '--parents') LIMIT :limit OFFSET :offset`,
    p: { refName: "main", limit: 10, offset: 0 },
    res: [
      {
        message: "Initialize data repository",
        parentsLength: 0,
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
      },
    ],
    matcher: logsMatcher,
  },
  {
    q: `SELECT * FROM dolt_log AS OF :refName`,
    p: { refName: "mybranch" },
    res: [
      {
        message: "Create table test",
        committer: "Dolt",
        email: "dolt@dolthub.com",
      },
      {
        message: "Initialize data repository",
        committer: "mysql-test-runner",
        email: "mysql-test-runner@liquidata.co",
      },
    ],
    matcher: logsMatcher,
  },
  {
    q: `SELECT * FROM DOLT_LOG(:refRange, '--parents')`,
    p: { refRange: "main..mybranch" },
    res: [
      {
        message: "Create table test",
        parentsLength: 1,
        committer: "Dolt",
        email: "dolt@dolthub.com",
      },
    ],
    matcher: logsMatcher,
  },
];
