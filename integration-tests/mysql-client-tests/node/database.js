import mysql from "mysql2";

export class Database {
  constructor(config) {
    this.connection = mysql.createConnection(config);
    this.connection.connect();
    this.connection.query("SET @@SESSION.dolt_log_committer_only = 1;", (err) => {
      if (err) console.warn("Failed to set dolt_log_committer_only:", err.message);
    });
  }

  query(sql, args) {
    return new Promise((resolve, reject) => {
      this.connection.query(sql, args, (err, rows) => {
        if (err) return reject(err);
        return resolve(rows);
      });
    });
  }

  close() {
    this.connection.end((err) => {
      if (err) {
        console.error(err);
      } else {
        console.log("db connection closed");
      }
    });
  }
}
