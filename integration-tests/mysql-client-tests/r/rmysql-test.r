library(RMySQL)
library(DBI)

args = commandArgs(trailingOnly=TRUE)

user = args[1]
port = strtoi(args[2])
db = args[3]

conn = dbConnect(RMySQL::MySQL(), host="127.0.0.1", port = port,
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
