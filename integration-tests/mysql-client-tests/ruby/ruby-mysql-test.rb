#!/usr/bin/ruby

require 'mysql'

user = ARGV[0]
port = ARGV[1]
db   = ARGV[2]

queries = [
  "create table test (pk int, `value` int, primary key(pk))",
  "describe test",
  "select * from test",
  "insert into test (pk, `value`) values (0,0)",
  "select * from test",
  "select dolt_add('-A');",
  "select dolt_commit('-m', 'my commit')",
  "select COUNT(*) FROM dolt_log",
  "select dolt_checkout('-b', 'mybranch')",
  "insert into test (pk, `value`) values (1,1)",
  "select dolt_commit('-a', '-m', 'my commit2')",
  "select dolt_checkout('main')",
  "select dolt_merge('mybranch')",
  "select COUNT(*) FROM dolt_log",
]

conn = Mysql::new("127.0.0.1", user, "", db, port)
queries.each do |query|
  res = conn.query(query)
end
conn.close()

exit(0)
