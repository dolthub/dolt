import { Database } from "./database.js";
import { getConfig } from "./helpers.js";
import runWorkbenchTests from "./workbenchTests/index.js";

async function workbench() {
  const database = new Database(getConfig());

  await runWorkbenchTests(database);

  database.close();
  process.exit(0);
}

workbench();
