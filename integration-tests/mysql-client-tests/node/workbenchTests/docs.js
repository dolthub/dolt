const readmeText = `# README
## My List

- Item 1
- Item 2
`;

const agentText = `# Agent Config Info`

export const docsTests = [
  {
    q: "INSERT INTO dolt_docs VALUES (:docName, :docText);",
    p: {
      docName: "AGENT.md",
      docText: agentText,
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
    q: "select * from dolt_docs",
    res: [{doc_name: "AGENT.md", doc_text: agentText}],
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
    res: [
      { doc_name: "README.md", doc_text: readmeText }
    ],
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
