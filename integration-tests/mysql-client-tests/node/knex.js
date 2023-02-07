import knex from "knex";
import wtfnode from "wtfnode";
import { Socket } from "net";
import { getConfig } from "./helpers.js";

const db = knex({
  client: "mysql2",
  connection: getConfig(),
});

async function createTable() {
  const val = await db.schema.createTable("test2", (table) => {
    table.integer("id").primary();
    table.integer("foo");
  });
  return val;
}

async function upsert(table, data) {
  const val = await db(table).insert(data).onConflict().merge();
  return val;
}

async function select() {
  const val = await db.select("id", "foo").from("test2");
  return val;
}

async function main() {
  await createTable();
  await Promise.all([
    upsert("test2", { id: 1, foo: 1 }),
    upsert("test2", { id: 2, foo: 2 }),
  ]);

  const expectedResult = JSON.stringify([
    { id: 1, foo: 1 },
    { id: 2, foo: 2 },
  ]);
  const result = await select();
  if (JSON.stringify(result) !== expectedResult) {
    console.log("Results:", result);
    console.log("Expected:", expectedResult);
    process.exit(1);
  }

  await db.destroy();

  // cc: https://github.com/dolthub/dolt/issues/3752
  setTimeout(async () => {
    const sockets = await getOpenSockets();

    if (sockets.length > 0) {
      wtfnode.dump();
      process.exit(1);
    }
  }, 3000);
}

// cc: https://github.com/myndzi/wtfnode/blob/master/index.js#L457
async function getOpenSockets() {
  const sockets = [];
  process._getActiveHandles().forEach(function (h) {
    // handles can be null now? early exit to guard against this
    if (!h) {
      return;
    }

    if (h instanceof Socket) {
      if (h.fd == null && h.localAddress && !h.destroyed) {
        sockets.push(h);
      }
    }
  });

  return sockets;
}

main();
