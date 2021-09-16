library(RMariaDB)
library(DBI)

args = commandArgs(trailingOnly=TRUE)

user = args[1]
port = args[2]
db = args[3]

conn = dbConnect(RMariaDB::MariaDB(), host="127.0.0.1", port = port,
                 username = user, dbname = db)

# check standard queries
queries = list("create table test (pk int, value int, primary key(pk))",
               "describe test",
               "insert into test (pk, `value`) values (0,0)",
               "select * from test")

responses = list(NULL,
                 data.frame(Field = c("pk", "value"), Type = c("int", "int"), Null = c("NO", "YES"), Key = c("PRI", ""), Default = c("", ""), Extra = c("", ""), stringsAsFactors = FALSE),
                 NULL,
                 data.frame(pk = c(0), value = c(0), stringsAsFactors = FALSE))

for(i in 1:length(queries)) {
    q = queries[[i]]
    want = responses[[i]]
    if (!is.null(want)) {
        got <- dbGetQuery(conn, q)
        if (!isTRUE(all.equal(want, got))) {
            print(q)
            print(want)
            print(got)
            quit(1)
        }
    } else {
        dbExecute(conn, q)
    }
}

# check prepared statements
stmt <- dbSendStatement(conn, "INSERT INTO test values (?, ?)")
rs <- dbBind(stmt, list(1,1))
rowsAff <- dbGetRowsAffected(rs)
dbClearResult(rs)

if (rowsAff != 1) {
    print("failed to execute prepared statement")
    quit(1)
}

got <- dbGetQuery(conn, "select * from test where pk = 1")
want = data.frame(pk = c(1), value = c(1))
if (!isTRUE(all.equal(want, got))) {
    print("unexpected prepared statement result")
    print(rows)
    quit(1)
}
