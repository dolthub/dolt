const readmeText = `# README
## My List

- Item 1
- Item 2
`;

export const docsTests = [
  {
    q: "select * from dolt_docs",
    res: [],
  },
  {
    q: "REPLACE INTO dolt_docs VALUES (:docName, :docText);",
    p: {
      docName: "README.md",
      docText: readmeText,
    },
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
    q: `select * from dolt_docs where doc_name=:docName`,
    p: { docName: "README.md" },
    res: [{ doc_name: "README.md", doc_text: readmeText }],
  },
  {
    q: "DELETE FROM dolt_docs WHERE doc_name=:docName",
    p: { docName: "README.md" },
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
    q: `select * from dolt_docs where doc_name=:docName`,
    p: { docName: "README.md" },
    res: [],
  },
  {
    q: `CALL DOLT_COMMIT("-A", "-m", :commitMsg)`,
    p: { commitMsg: "Add dolt_docs table" },
    res: [{ hash: "" }],
  },
];
