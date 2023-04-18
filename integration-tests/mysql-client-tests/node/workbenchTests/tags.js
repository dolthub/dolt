import { tagsMatcher } from "./matchers.js";

export const tagsTests = [
  {
    q: "SELECT * FROM dolt_tags ORDER BY date DESC",
    res: [],
  },
  {
    q: `CALL DOLT_TAG(:tagName, :fromRefName)`,
    p: { tagName: "mytag", fromRefName: "main" },
    res: [{ status: 0 }],
  },
  {
    q: "SELECT * FROM dolt_tags ORDER BY date DESC",
    res: [
      {
        tag_name: "mytag",
        message: "",
        email: "mysql-test-runner@liquidata.co",
        tagger: "mysql-test-runner",
        tag_hash: "",
        date: "",
      },
    ],
    matcher: tagsMatcher,
  },
  {
    q: `CALL DOLT_TAG("-m", :message, :tagName, :fromRefName)`,
    p: { message: "latest release", tagName: "mytagnew", fromRefName: "main" },
    res: [{ status: 0 }],
  },
  {
    q: "SELECT * FROM dolt_tags ORDER BY date DESC",
    res: [
      {
        tag_name: "mytagnew",
        message: "latest release",
        email: "mysql-test-runner@liquidata.co",
        tagger: "mysql-test-runner",
        tag_hash: "",
        date: "",
      },
      {
        tag_name: "mytag",
        message: "",
        email: "mysql-test-runner@liquidata.co",
        tagger: "mysql-test-runner",
        tag_hash: "",
        date: "",
      },
    ],
    matcher: tagsMatcher,
  },
  {
    q: `CALL DOLT_TAG("-d", :tagName)`,
    p: { tagName: "mytagnew" },
    res: [{ status: 0 }],
  },
  {
    q: "SELECT * FROM dolt_tags ORDER BY date DESC",
    res: [
      {
        tag_name: "mytag",
        message: "",
        email: "mysql-test-runner@liquidata.co",
        tagger: "mysql-test-runner",
        tag_hash: "",
        date: "",
      },
    ],
    matcher: tagsMatcher,
  },
  {
    q: `CALL DOLT_COMMIT("-A", "-m", :commitMsg)`,
    p: { commitMsg: "Add a tag" },
    expectedErr: "nothing to commit",
  },
];
