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
               "insert into test (pk, value) values (0,0)",
               "select * from test")

responses = list(NULL,
                 data.frame(Field = c("pk", "value"), Type = c("int", "int"), Null = c("NO", "YES"), Key = c("PRI", ""), Default = c("", ""), Extra = c("", "")),
                 NULL,
                 data.frame(pk = c(0), value = c(0)))

for(i in 1:length(queries)) {
    q = queries[[i]]
    want = responses[[i]]
    if (!is.null(want)) {
        got <- dbGetQuery(conn, q)
        if (!all(want == got)) {
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
statement <- enc2utf8(new("SQL", .Data = "INSERT INTO test (pk, value) values (?, ?)"))
params <- unname(list(1,1))
rs <- new("MariaDBResult",
          sql = statement,
          ptr = RMariaDB:::result_create(conn@ptr, statement, is_statement = TRUE),
          bigint = conn@bigint,
          conn = conn)

dbBind(rs, params)
rowsAff = dbGetRowsAffected(rs)
dbClearResult(rs)

if (rowsAff != 1) {
    print("failed to execute prepared statement")
    quit(1)
}

got <- dbGetQuery(conn, "select * from test where pk = 1")
want = data.frame(pk = c(1), value = c(1))
if (!all(want == got)) {
    print("unexpected prepared statement result")
    print(rows)
    quit(1)
}
