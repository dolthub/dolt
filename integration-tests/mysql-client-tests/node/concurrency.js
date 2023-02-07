import mysql from "mysql2/promise";
import { getConfig } from "./helpers";

async function createTable() {
  const conn = await getConnection();
  try {
    await conn.execute("create table users (name varchar(20))");
  } catch (err) {
    console.error(`Error creating table:`, err);
    process.exit(1);
  } finally {
    conn.end();
  }
}

async function commitTable() {
  const conn = await getConnection();
  try {
    await conn.execute(`call dolt_add('.')`);
    await conn.execute(`call dolt_commit('-am', 'new table')`);
  } catch (err) {
    console.error(`Error committing table:`, err);
  } finally {
    conn.end();
  }
}

const authors = [
  "bob",
  "john",
  "mary",
  "alice",
  "bob2",
  "john2",
  "mary2",
  "alice2",
  "bob3",
  "john3",
  "mary3",
  "alice3",
  "bob4",
  "john4",
  "mary4",
  "alice4",
  "bob5",
  "john5",
  "mary5",
  "alice5",
  "bob6",
  "john6",
  "mary6",
  "alice6",
  "bob7",
  "john7",
  "mary7",
  "alice7",
  "bob8",
  "john8",
  "mary8",
  "alice8",
  "bob9",
  "john9",
  "mary9",
  "alice9",
];

async function insertAuthor(name) {
  const conn = await getConnection();
  try {
    await conn.execute("start transaction");
    await conn.execute("INSERT INTO users (name) VALUES(?);", [name]);
    await conn.execute(`call dolt_commit('-am', concat('created author', ?))`, [
      name,
    ]);
  } catch (err) {
    console.error(`Error committing ${name}:`, err);
    process.exit(1);
  } finally {
    conn.end();
  }
}

async function validateCommits(name) {
  const conn = await getConnection();
  let results;
  try {
    results = await conn.query(
      `select count(*) as c from dolt_log where message like 'created author%'`
    );
  } catch (err) {
    console.error(`Error:`, err);
    process.exit(1);
  } finally {
    conn.end();
  }

  const count = results[0][0].c;
  const expectedCount = authors.length;
  if (count != expectedCount) {
    console.error(
      `Unexpected number of commits: expected ${expectedCount}, was ${count}`
    );
    process.exit(1);
  }
}

async function getConnection() {
  const connection = await mysql.createConnection(getConfig());
  return connection;
}

// Regression test concurrent dolt_commit with node clients
// https://github.com/dolthub/dolt/issues/4361
async function main() {
  await createTable();
  await commitTable();
  await Promise.all(authors.map(insertAuthor));
  await validateCommits();
}

main();
