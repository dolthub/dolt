defmodule SmokeTest do
  def myTestFunc(arg1, arg2) do
    if arg1 != arg2 do
      raise "Test error"
    end
  end

  def run do
    {:ok, pid} = MyXQL.start_link(username: "root", port: 3307, password: "root", database: "dsimple")
    {:ok, _} = MyXQL.query(pid, "drop table if exists test")
    {:ok, _} = MyXQL.query(pid, "create table test (pk int, `value` int, primary key(pk))")
    {:ok, _} = MyXQL.query(pid, "describe test")

    {:ok, result} = MyXQL.query(pid, "select * from test")
    myTestFunc(result.num_rows, 0)

    {:ok, _} = MyXQL.query(pid, "insert into test (pk, `value`) values (0,0)")

    # MyXQL uses the CLIENT_FOUND_ROWS flag so we should return the number of rows matched
    {:ok, result} = MyXQL.query(pid, "UPDATE test SET pk = pk where pk = 0")
    myTestFunc(result.num_rows, 1)

    {:ok, result} = MyXQL.query(pid, "SELECT * FROM test")
    myTestFunc(result.num_rows, 1)
    myTestFunc(result.rows, [[0,0]])
  end
end
