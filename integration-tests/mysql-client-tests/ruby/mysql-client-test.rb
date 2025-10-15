#!/usr/bin/ruby

require 'mysql'
require 'test/unit'
require 'pp'

extend Test::Unit::Assertions

user = ARGV[0]
port = ARGV[1]
db   = ARGV[2]

queries = [
  "create table test (pk int, `value` int, d1 decimal(9, 3), f1 float, primary key(pk))",
  "describe test",
  "select * from test",
  "insert into test (pk, `value`, d1, f1) values (0,0,0.0,0.0)",
  "select * from test",
  "call dolt_add('-A');",
  "call dolt_commit('-m', 'my commit')",
  "select COUNT(*) FROM dolt_log",
  "call dolt_checkout('-b', 'mybranch')",
  "insert into test (pk, `value`, d1, f1) values (1,1, 123456.789, 420.42)",
  "call dolt_commit('-a', '-m', 'my commit2')",
  "call dolt_checkout('main')",
  "call dolt_merge('mybranch')",
  "select COUNT(*) FROM dolt_log",
]

# Smoke test the queries to make sure nothing blows up
conn = Mysql::new("127.0.0.1", user, "", db, port)
queries.each do |query|
  res = conn.query(query)
end

# Then make sure we can read some data back
res = conn.query("SELECT * from test where pk = 1;")
rowCount = 0
res.each do |row|
  rowCount += 1
  assert_equal 1, row[0].to_i
  assert_equal 1, row[1].to_i
  assert_equal 123456.789, row[2].to_f
  assert_equal 420.42, row[3].to_f
end
assert_equal 1, rowCount

conn.close()
exit(0)
