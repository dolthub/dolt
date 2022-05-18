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
    print(got)
    quit(1)
}

dolt_queries = list("SELECT DOLT_ADD('-A')",
                    "select dolt_commit('-m', 'my commit')",
                    "select dolt_checkout('-b', 'mybranch')",
                    "insert into test (pk, `value`) values (2,2)",
                     "select dolt_commit('-a', '-m', 'my commit2')",
                     "select dolt_checkout('main')",
                     "select dolt_merge('mybranch')")

for(i in 1:length(dolt_queries)) {
    q = dolt_queries[[i]]
    dbExecute(conn, q)
}

count <- dbGetQuery(conn, "select COUNT(*) as c from dolt_log")
want <- data.frame(c = c(3))
ret <- all.equal(count, want)
if (!ret) {
    print("Number of commits is incorrect")
    quit(1)
}

# Add a failing query and ensure that the connection does not quit.
# cc. https://github.com/dolthub/dolt/issues/3418
try(dbExecute(conn, "insert into test values (0, 1)"), silent = TRUE)
one <- dbGetQuery(conn, "select 1 as pk")
ret <- one == data.frame(pk=1)
if (!ret) {
    print("Number of commits is incorrect")
    quit(1)
}

