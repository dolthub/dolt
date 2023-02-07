const args = process.argv.slice(2);
const user = args[0];
const port = args[1];
const dbName = args[2];

export function getArgs() {
  return { user, port, dbName };
}

export function getConfig() {
  const { user, port, dbName } = getArgs();
  return {
    host: "127.0.0.1",
    port: port,
    user: user,
    database: dbName,
  };
}
