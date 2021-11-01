defmodule SmokeTest do
  def myTestFunc(arg1, arg2) do
    if arg1 != arg2 do
      raise "Test error"
    end
  end

  @spec run :: nil
  def run do
    args = System.argv()
    user = Enum.at(args, 0)
    {port, _} = Integer.parse(Enum.at(args, 1))
    database = Enum.at(args, 2)

    {:ok, pid} = MyXQL.start_link(username: user, port: port, database: database)
    {:ok, _} = MyXQL.query(pid, "drop table if exists test")
    {:ok, _} = MyXQL.query(pid, "create table test (pk int, `value` int, primary key(pk))")
    {:ok, _} = MyXQL.query(pid, "describe test")

    {:ok, result} = MyXQL.query(pid, "select * from test")
    myTestFunc(result.num_rows, 0)

    {:ok, _} = MyXQL.query(pid, "insert into test (pk, `value`) values (0,0)")

    # MyXQL uses the CLIENT_FOUND_ROWS flag so we should return the number of rows matched
    {:ok, result} = MyXQL.query(pid, "UPDATE test SET pk = pk where pk = 0")
    myTestFunc(result.num_rows, 1)

    {:ok, result} = MyXQL.query(pid, "INSERT INTO test VALUES (0, 0) ON DUPLICATE KEY UPDATE `value` = `value`")
    myTestFunc(result.num_rows, 1)

    {:ok, result} = MyXQL.query(pid, "SELECT * FROM test")
    myTestFunc(result.num_rows, 1)
    myTestFunc(result.rows, [[0,0]])

    {:ok, _} = MyXQL.query(pid, "select dolt_add('-A');")
    {:ok, _} = MyXQL.query(pid, "select dolt_commit('-m', 'my commit')")
    {:ok, _} = MyXQL.query(pid, "select COUNT(*) FROM dolt_log")
    {:ok, _} = MyXQL.query(pid, "select dolt_checkout('-b', 'mybranch')")
    {:ok, _} = MyXQL.query(pid, "insert into test (pk, `value`) values (1,1)")
    {:ok, _} = MyXQL.query(pid, "select dolt_commit('-a', '-m', 'my commit2')")
    {:ok, _} = MyXQL.query(pid, "select dolt_checkout('main')")
    {:ok, _} = MyXQL.query(pid, "select dolt_merge('mybranch')")

    {:ok, result} = MyXQL.query(pid, "select COUNT(*) FROM dolt_log")
    myTestFunc(result.num_rows, 1)
    myTestFunc(result.rows, [[3]])
  end
end
