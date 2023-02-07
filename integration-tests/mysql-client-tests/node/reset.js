import { Database } from "./database.js";
import { getConfig } from "./helpers.js";

async function main() {
  const database = new Database(getConfig());
  try {
    await database.query("call dolt_checkout('main')");
    await database.query(
      "call dolt_reset('--hard', 'p6dj4b4fng13r3eostv63di4iun58lr2')"
    );
    const status = await database.query("select * from dolt_status");
    if (status.length > 0) {
      await database.query("drop table test");
    } else {
      await database.query("call dolt_branch('-D', 'mybranch')");
    }
  } catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    database.close();
    process.exit(0);
  }
}

main();
