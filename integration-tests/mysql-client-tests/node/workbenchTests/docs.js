const readmeText = `# README
## My List

- Item 1
- Item 2
`;

export const docsTests = [
  {
    q: "select * from dolt_docs",
    expectedErr: "table not found: dolt_docs",
  },
  {
    q: `CREATE TABLE IF NOT EXISTS \`dolt_docs\` (
      \`doc_name\` varchar(16383) NOT NULL,
      \`doc_text\` varchar(16383),
      PRIMARY KEY (\`doc_name\`)
    )`,
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
    q: "select * from dolt_docs",
    res: [],
  },
  {
    q: `CREATE TABLE IF NOT EXISTS \`dolt_docs\` (
      \`doc_name\` varchar(16383) NOT NULL,
      \`doc_text\` varchar(16383),
      PRIMARY KEY (\`doc_name\`)
    )`,
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
    q: "INSERT INTO dolt_docs VALUES (:docName, :docText) ON DUPLICATE KEY UPDATE doc_text=:docText",
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
